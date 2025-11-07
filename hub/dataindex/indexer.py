import asyncio
from copy import deepcopy
from datetime import datetime
from functools import partial
from types import SimpleNamespace
from typing import Callable

from biothings.hub.dataindex.indexer import Indexer, _BuildBackend, _BuildDoc, ProcessInfo
from biothings.hub.dataindex.indexer_payload import IndexSettings, IndexMappings, DEFAULT_INDEX_SETTINGS, \
    DEFAULT_INDEX_MAPPINGS
from biothings.hub.dataindex.indexer_task import (
    IndexingTask,
    Mode,
    _ensure_logger,
    _get_es_client,
    _get_mg_client,
    _validate_ids,
)
from biothings.hub.dataindex.indexer_schedule import Schedule, SchedulerMismatchError
from biothings.utils import mongo

from biothings.utils.mongo import DatabaseClient, id_feeder
from biothings.utils.common import iter_n

try:
    from biothings.utils.mongo import doc_feeder
except ImportError:
    import biothings

    biothings.config = SimpleNamespace()
    biothings.config.DATA_SRC_DATABASE = "biothings_src"
    biothings.config.DATA_TARGET_DATABASE = "biothings_build"
    from biothings.utils.mongo import doc_feeder


class MissingNodeCollectionError(Exception):
    def __init__(self, discovered_collections: list[str], database):
        message = (
            f"Missing node collections {discovered_collections} "
            f"in database {database}"
        )
        super().__init__(message)


class KGXIndexer(Indexer):
    """
    RTXKG2 Knowledge Graph custom indexer
    """

    def __init__(self, build_doc: dict, indexer_env: dict, index_name: str):

        super().__init__(build_doc, indexer_env, index_name)

        print("using KGXIndexer")

        _build_doc = _BuildDoc(build_doc)

        # mongodb edge client metadata
        edge_build = _build_doc.parse_backend()
        self.mongo_edge_client_args = edge_build.args
        self.mongo_edge_database_name = edge_build.dbs
        self.mongo_edge_collection_name = edge_build.col

        # mongodb node client metadata
        # Need to acquire the RTXKG2 nodes collection from mongodb
        node_build = self._build_node_backend_client(build_doc)
        self.mongo_node_client_args = node_build.args
        self.mongo_node_database_name = node_build.dbs
        self.mongo_node_collection_name = node_build.col

        # -----------dest-----------

        # [1] https://elasticsearch-py.readthedocs.io/en/v7.12.0/api.html#elasticsearch.Elasticsearch
        # [2] https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#elasticsearch.helpers.bulk
        self.es_client_args = indexer_env.get("args", {})  # See [1] for available args
        self.es_blkidx_args = indexer_env.get("bulk", {})  # See [2] for available args
        self.es_index_name = index_name or _build_doc.build_name
        self.es_index_settings = IndexSettings(deepcopy(DEFAULT_INDEX_SETTINGS))
        self.es_index_mappings = IndexMappings(deepcopy(DEFAULT_INDEX_MAPPINGS))



        # add tokenizer to settings
        self.es_index_settings.update({
            "analysis": {
              "analyzer": {
                "curie_analyzer": {
                  "type": "custom",
                  "tokenizer": "curie_tokenizer"
                }
              },
              "tokenizer": {
                "curie_tokenizer": {
                  "type": "pattern",
                  "pattern": ":"
                }
              }
            }
        })

        _build_doc.enrich_settings(self.es_index_settings)
        _build_doc.enrich_mappings(self.es_index_mappings)

        # -----------info-----------

        self.env_name = indexer_env.get("name")
        self.conf_name = _build_doc.build_config.get("name")
        self.build_name = _build_doc.build_name

        self.setup_log()
        self.pinfo = ProcessInfo(self, indexer_env.get("concurrency", 10))

    def _build_node_backend_client(self, build_doc: _BuildDoc) -> _BuildBackend:
        """
        Internal method for building a mongodb client specifically
        for the node collection from the BuildDoc, separate from the
        default client that targets the edges
        """
        backend = build_doc.get("target_backend")
        backend_url = build_doc["build_config"]["node_collection"]
        # backend_url = ['src', 'CEBS_nodes']
        # backend_url = 'CEBS_nodes'
        # backend_url = "DrugCentral_nodes"
        # backend_url = "rtx_nodes"

        # backend_url = "ctd_nodes"

        db = mongo.get_src_db()


        discovered_collections = db.list_collection_names()
        if backend_url in discovered_collections:
            node_build_backend = _BuildBackend(
                dict(zip(("host", "port"), db.client.address)),
                db.name,
                backend_url,
            )
            return node_build_backend
        raise MissingNodeCollectionError(discovered_collections, db)

    async def do_index(self, job_manager, batch_size, ids, mode, **kwargs):
        client = DatabaseClient(**self.mongo_edge_client_args)
        database = client[self.mongo_edge_database_name]
        collection = database[self.mongo_edge_collection_name]

        if ids:
            self.logger.info(
                ("Indexing from '%s' with specific list of _ids, " "create indexer job with batch_size=%d."),
                self.mongo_edge_collection_name,
                batch_size,
            )
            # use user provided ids in batch
            id_provider = iter_n(ids, batch_size)
        else:
            self.logger.info(
                ("Fetch _ids from '%s', and create " "indexer job with batch_size=%d."),
                self.mongo_edge_collection_name,
                batch_size,
            )
            # use ids from the target mongodb collection in batch
            id_provider = id_feeder(collection, batch_size, logger=self.logger)

        jobs = []  # asyncio.Future(s)
        error = None  # the first Exception

        total = len(ids) if ids else collection.count()
        schedule = Schedule(total, batch_size)

        def batch_finished(future):
            nonlocal error
            try:
                schedule.finished += future.result()
            except Exception as exc:
                self.logger.error(exc)
                error = exc

        for batch_num, ids in zip(schedule, id_provider):
            await asyncio.sleep(0.0)

            # when one batch failed, and job scheduling has not completed,
            # stop scheduling and cancel all on-going jobs, to fail quickly.

            if isinstance(error, Exception):
                for job in jobs:
                    if not job.done():
                        job.cancel()
                raise error

            self.logger.info(schedule)

            pinfo = self.pinfo.get_pinfo(schedule.suffix(self.mongo_edge_collection_name))

            job = await job_manager.defer_to_process(
                pinfo,
                dispatch,
                (self.mongo_edge_client_args, self.mongo_edge_database_name, self.mongo_edge_collection_name),
                (self.mongo_node_client_args, self.mongo_node_database_name, self.mongo_node_collection_name),
                (self.es_client_args, self.es_blkidx_args, self.es_index_name),
                ids,
                mode,
                batch_num,
            )
            job.add_done_callback(batch_finished)
            jobs.append(job)

        self.logger.info(schedule)
        await asyncio.gather(*jobs)

        try:
            schedule.completed()
        except SchedulerMismatchError as schedule_error:
            scheduler_error_message = (
                f"mongo client configuration: {self.mongo_edge_client_args} | "
                f"mongo database: {self.mongo_edge_database_name} | "
                f"mongo collection: {self.mongo_edge_collection_name} "
            )
            self.logger.error(scheduler_error_message)
            raise schedule_error

        self.logger.notify(schedule)
        return {"count": total, "created_at": datetime.now().astimezone()}


def dispatch(
    mongo_edge_metadata: tuple,
    mongo_node_metadata: tuple,
    es_metadata: tuple,
    ids,
    mode,
    name,
):
    es_index_name = es_metadata[2]
    return KGXIndexingTask(
        partial(_get_es_client, *es_metadata),
        partial(_get_mg_client, *mongo_edge_metadata),
        partial(_get_mg_client, *mongo_node_metadata),
        ids,
        mode,
        f"index_{es_index_name}",
        name,
    ).dispatch()


class KGXIndexingTask(IndexingTask):
    """
    Overridden Indexing Task specific to the RTXKG2 knowledge graph

    Index one batch of documents from MongoDB to Elasticsearch.
    The documents to index are specified by their ids.
    """

    def __init__(self, es: Callable, edge_mongo: Callable, node_mongo: Callable, ids, mode=None, logger=None,
                 name="task"):

        # super().__init__(es, mongo, ids, mode, logger, name)
        assert callable(es)
        assert callable(edge_mongo)
        assert callable(node_mongo)

        self.logger = _ensure_logger(logger)
        self.name = f"#{name}" if isinstance(name, int) else name

        self.ids, self.invalid_ids = _validate_ids(ids, self.logger)
        self.mode = Mode(mode or "index")

        # these are functions to create clients,
        # each also associated with an organizational
        # structure in the corresponding database,
        # functioning as the source or destination
        # of batch document manipulation.
        self.backend = SimpleNamespace()
        self.backend.es = es()  # wrt an index
        self.backend.edge_collection = edge_mongo()  # wrt a collection
        self.backend.node_collection = node_mongo()  # wrt a collection


    def build_node_cache(self):
        # prefetch necessary node ids
        edge_docs = self.backend.edge_collection.find(
            filter={"_id": {"$in": self.ids}},
            projection={"subject": 1, "object": 1},
        )

        node_cache = {}

        for edge in edge_docs:
            node_cache[edge["subject"]] = None
            node_cache[edge["object"]] = None


        node_docs = self.backend.node_collection.find(
            filter={"_id": {"$in": list(node_cache.keys())}},
        )

        for node_doc in node_docs:
            _id = node_doc["_id"]
            node_doc.pop("_id", None)
            node_cache[_id] = node_doc



        return node_cache


    def index(self):
        """
        Alternative indexing method
        """

        node_cache = self.build_node_cache()

        edge_docs = doc_feeder(
            self.backend.edge_collection,
            step=len(self.ids),
            inbatch=False,
            query={"_id": {"$in": self.ids}},
        )

        def update_edge(edge):
            edge["subject"] = node_cache[edge["subject"]]
            edge["object"] = node_cache[edge["object"]]




            return edge

        updated_edges = map(update_edge, edge_docs)

        self.logger.info("%s: %d documents.", self.name, len(self.ids))
        count_docs = self.backend.es.mindex(updated_edges)
        return count_docs + len(self.invalid_ids)


    def merge_nodes_traffic_heavy(self, node_cache, edge_doc):
        subject_id = edge_doc["subject"]
        object_id = edge_doc["object"]

        query_ids = []

        for node_id in [subject_id, object_id]:
            if node_id not in node_cache:
                query_ids.append(node_id)
            else:
                for field in ["subject", "object"]:
                    if edge_doc[field] == node_id:
                        edge_doc[field] = node_cache[node_id]


        if query_ids:
            node_docs = doc_feeder(
                self.backend.node_collection,
                step=len(query_ids),
                inbatch=False,
                query={"_id": {"$in": query_ids}},
            )

            for node_doc in node_docs:
                # remove _id from node
                _id = node_doc["_id"]
                node_doc.pop("_id", None)

                # update cache
                node_cache[_id] = node_doc
                for field in ["subject", "object"]:
                    if edge_doc[field] == _id:
                        edge_doc[field] = node_doc

        return edge_doc

    def _index(self):
        """
        Overridden from base index function to handle the merging of nodes and edges

        The main index we supply will contain all the edges we care about from RTXKG2,
        and we want to merge the nodes from an external target backend collection
        """
        edge_docs = doc_feeder(
            self.backend.edge_collection,
            step=len(self.ids),
            inbatch=False,
            query={"_id": {"$in": self.ids}},
        )
        # method 2
        node_cache = {}
        edge_docs = map(partial(self.merge_nodes_traffic_heavy, node_cache), edge_docs)


        self.logger.info("%s: %d documents.", self.name, len(self.ids))
        count_docs = self.backend.es.mindex(edge_docs)
        return count_docs + len(self.invalid_ids)

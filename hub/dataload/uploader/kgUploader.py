import logging as loggingmod
import json
from functools import partial

import requests
from biothings.hub.dataload.uploader import BaseSourceUploader
from typing_extensions import override

import config
from hub.dataload.kgDumper import KGX_METADATA_FIELD, KGX_METADATA_KEYS

logging = config.logger


def normalize_parser_metadata(metadata):
    metadata = dict(metadata or {})

    qualifier_fields = metadata.get("qualifier_fields")
    if isinstance(qualifier_fields, set):
        metadata["qualifier_fields"] = sorted(qualifier_fields)

    return metadata


def kg_upload_worker(name, storage_class, loaddata_func, col_name, batch_size, batch_num, *args, **kwargs):
    data = loaddata_func(*args)
    max_batch_num = kwargs.get("max_batch_num", None)
    db = kwargs.get("db", None)

    if isinstance(storage_class, tuple):
        klass_name = "_".join([k.__class__.__name__ for k in storage_class])
        storage = type(klass_name, storage_class, {})(None, col_name, loggingmod)
    else:
        storage = storage_class(db, col_name, loggingmod)

    count = storage.process((doc for doc in data), batch_size, max_batch_num)

    return {
        "count": count,
        "metadata": normalize_parser_metadata(getattr(data, "metadata", {})),
    }


class KGXUploader(BaseSourceUploader):
    # from template
    # __metadata__ = {"src_meta" : $__metadata__}

    def get_parser_metadata(self):
        return getattr(self, "parser_metadata", {})

    @override
    async def update_data(self, batch_size, job_manager):
        """
        Iterate over load_data() to pull data and store it.

        Extends the base uploader flow by returning parser metadata from the
        upload worker process and keeping it on this uploader instance for
        generate_doc_src_master().
        """
        pinfo = self.get_pinfo()
        pinfo["step"] = "update_data"

        self.unprepare()
        job = await job_manager.defer_to_process(
            pinfo,
            partial(
                kg_upload_worker,
                self.fullname,
                self.__class__.storage_class,
                self.load_data,
                self.temp_collection_name,
                batch_size,
                1,  # no batch, just #1
                self.data_folder,
            ),
        )

        result = await job
        if not isinstance(result, dict) or not isinstance(result.get("count"), int):
            raise TypeError(f"upload error (expected count/metadata result, got {repr(result)})")

        self.parser_metadata = result.get("metadata") or {}
        self.switch_collection()

        return result["count"]

    @classmethod
    def get_manifest_src_meta(cls):
        """Return the manifest's src_meta, before any expansion.

        generate_doc_src_master() writes its expanded result back to __metadata__,
        so the unexpanded manifest values are kept aside here. Without this, the
        first expansion would freeze src_meta for the life of the hub process and
        later releases would never be picked up.
        """
        if "_manifest_src_meta" not in cls.__dict__:
            cls._manifest_src_meta = dict(getattr(cls, "__metadata__", {}).get("src_meta", {}))
        return cls._manifest_src_meta

    def resolve_manifest_metadata(self, meta_src):
        """Replace the manifest's metadata URLs with the documents they point at.

        Prefers what KgDumper captured at dump time, so src_meta describes the
        release that is actually in the collection. Sources whose last dump
        predates that capture, or that are uploaded without a preceding dump,
        fall back to fetching the URL directly.
        """
        cached = (self.src_doc or {}).get(KGX_METADATA_FIELD) or {}

        for meta_key in KGX_METADATA_KEYS:
            meta_loc = meta_src.get(meta_key)
            if not isinstance(meta_loc, str):
                continue

            resolved = cached.get(meta_key)
            if isinstance(resolved, dict):
                meta_src[meta_key] = resolved
                continue

            logging.info(f"No dump-time {meta_key} metadata for {self.fullname}, fetching {meta_loc}")
            try:
                meta_src[meta_key] = requests.get(meta_loc).json()
            except requests.exceptions.RequestException as e:
                logging.info(f"Error getting remote metadata: {e}. Injection bypassed.")
            except (json.JSONDecodeError, ValueError) as e:
                logging.info(f"Error parsing remote metadata {e}. Injection bypassed.")

    @override
    def generate_doc_src_master(self):
        """Parse metadata from graph/release json files provided with KG datasets"""

        # modify and  inject __metadata__ here
        if hasattr(self.__class__, "__metadata__"):
            meta_src = dict(self.get_manifest_src_meta())
            self.resolve_manifest_metadata(meta_src)
            meta_src.update(self.get_parser_metadata())
            self.__class__.__metadata__ = {"src_meta": meta_src}

        return super().generate_doc_src_master()

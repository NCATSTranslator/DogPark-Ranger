from functools import partial

from hub.dataload.compressed_parser import load_from_tar
from hub.dataload.info_parser import get_adj_list, encapsule, split_n_chunks, to_key_value_pair
from hub.dataload.kgx_normalization import normalize_kgx_edge, normalize_kgx_node


class ParserResult:
    def __init__(self, docs, metadata=None):
        self.docs = iter(docs)
        self.metadata = metadata or {}

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.docs)

    def items(self):
        for doc in self:
            yield doc.get("_id", doc["id"]), doc


GANDALF_NORMALIZATION_STEPS = [
    "nodes normalized to id/name/categories/attributes",
    "edges normalized to subject/object/predicate/id/sources/qualifiers/attributes",
    "edge sources normalized and dogpark-tier0 aggregator prepended",
    "edge qualifier fields copied to TRAPI qualifiers list",
    "node/edge non-core fields copied to TRAPI attributes list",
]

def node_processor(node):
    return normalize_kgx_node(node)


def edge_processor(unique_qualifier_set: set, edge):
    return normalize_kgx_edge(edge, unique_qualifier_set)


def parser(*args, **kwargs):
    entity = kwargs.get('entity')
    unique_qualifier_set = kwargs.pop("qualifier_set", None)

    if entity is None:
        raise ValueError("No entity specified")

    if unique_qualifier_set is None:
        unique_qualifier_set = set()

    processor_pipeline = (
        node_processor
        if entity == "nodes"
        else partial(
            edge_processor,
            unique_qualifier_set,
        )
    )

    # disable sequence generation by default
    if entity == "nodes" and kwargs.get('gen_seq', None) is None:
        kwargs['gen_seq'] = False

    docs = map(processor_pipeline, load_from_tar(*args, **kwargs))
    if entity == "nodes":
        docs = (doc for doc in docs if doc.get("id"))

    metadata = {}
    metadata["gandalf_normalization"] = GANDALF_NORMALIZATION_STEPS

    if entity == "edges":
        metadata["qualifier_fields"] = unique_qualifier_set

    return ParserResult(docs, metadata)


def node_info_parser(*args, **kwargs):
    adj_list_key = kwargs.pop('adj_list_key')
    should_reverse = kwargs.pop('should_reverse', False)

    assert isinstance(adj_list_key, str)

    # disable extra payload
    kwargs['gen_seq'] = False
    kwargs['gen_id'] = False

    edge_kwargs = {
        **kwargs,
        "entity": "edges"
    }

    node_kwargs = {
        **kwargs,
        "entity": "nodes"
    }

    edge_iterator = parser(*args, **edge_kwargs)
    adj_list = get_adj_list(edge_iterator, should_reverse)
    # node_iterator = parser(*args, **node_kwargs)
    # nodes = dict(to_key_value_pair(node_iterator))

    encapsuled = encapsule({
        adj_list_key: adj_list,
        "size": len(adj_list),
    })


    chunks = (10 ** 3) * 5

    payload = []

    node_chunks = split_n_chunks(encapsuled, chunks)
    payload.extend(node_chunks)

    return payload

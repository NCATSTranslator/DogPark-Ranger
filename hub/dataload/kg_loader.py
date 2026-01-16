import base64
import math
import zlib
from collections import defaultdict
from functools import partial
from typing import Iterable

import msgpack

from hub.dataload.compressed_parser import load_from_tar
from hub.dataload.utils.pipeline import apply_processors
from hub.dataload.utils.process_category import process_category_list
from hub.dataload.utils.process_node_fields import process_chembl_black_box_warning
from hub.dataload.utils.process_predicate import process_predicate
from hub.dataload.utils.process_qualifiers import process_qualifiers
from hub.dataload.utils.process_sources import process_sources


def node_processor(node):
    processors = [
        process_chembl_black_box_warning,
        process_category_list
    ]
    return apply_processors(processors, node)


def edge_processor(predicate_cache: dict, edge):
    processors = [
        process_category_list,
        process_qualifiers,
        process_sources,
        # we need cache to do faster ancestor look up
        partial(process_predicate, predicate_cache=predicate_cache)
    ]
    return apply_processors(processors, edge)


def parser(*args, **kwargs):
    entity = kwargs.get('entity')

    if entity is None:
        raise ValueError("No entity specified")

    processor_pipeline = (
        node_processor
        if entity == "nodes"
        else partial(edge_processor, {}) # initialize predicate look-up cache
    )

    # disable sequence generation by default
    if entity == "nodes" and kwargs.get('gen_seq', None) is None:
        kwargs['gen_seq'] = False

    yield from map(processor_pipeline, load_from_tar(*args, **kwargs))




def get_adj_list(edge_iterator: Iterable) -> dict[str, list[str]]:
    adj_set = defaultdict(set[str])

    for edge in edge_iterator:
        sub = edge['subject']
        obj = edge['object']
        adj_set[sub].add(obj)

    adj_list: dict[str, list[str]] = {
        k: list(v) for k, v in adj_set.items()
    }

    return adj_list

def encapsule(payload: list | dict) -> str:

    # todo test pickle
    payload_b = msgpack.packb(payload, use_bin_type=True)

    # todo zst
    compressed_b = zlib.compress(payload_b, level=6)
    base64_payload = base64.b64encode(compressed_b).decode("ascii")

    return base64_payload


def split_n_chunks(b64: str, n: int, key: str, offset = 0) -> list:
    if n <= 0:
        raise ValueError("n must be > 0")

    length = len(b64)
    chunk_size = math.ceil(length / n)

    return [
        {
            "_id": str(offset + i),
            "key": key,
            "chunk_index": i,
            "value": b64[i : i + chunk_size]
        } for i in range(0, length, chunk_size)
    ]


def flat_parser(*args, **kwargs):
    result_key = kwargs.pop('result_key')
    adj_list_key = kwargs.pop('adj_list_key')

    assert isinstance(result_key, str)
    assert isinstance(adj_list_key, str)
    assert result_key != adj_list_key

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
    adj_list = get_adj_list(edge_iterator)
    nodes = list(parser(*args, **node_kwargs))

    # todo encapsule one unified object
    nodes_b = encapsule(nodes)
    adj_list_b = encapsule(adj_list)


    node_chunks = 10 ** 3
    adj_list_chunks = 10 ** 3

    payload = []

    node_chunks = split_n_chunks(nodes_b, node_chunks, result_key)
    payload.extend(node_chunks)
    payload.extend(split_n_chunks(adj_list_b, adj_list_chunks, adj_list_key, len(node_chunks)))

    return payload








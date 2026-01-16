import base64
import math
import zlib
from collections import defaultdict
from typing import Iterable, Any

import msgpack


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

def to_key_value_pair(nodes: Iterable[dict]):
    for node in nodes:
        _id = node.pop("_id")
        yield _id, node

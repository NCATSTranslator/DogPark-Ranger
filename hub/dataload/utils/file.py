import gzip
import pathlib
from contextlib import contextmanager
from typing import Union

import jsonlines


@contextmanager
def gz_open(path: Union[str, pathlib.Path]):
    """Open a gzipped JSONL file with jsonlines.Reader."""
    with gzip.open(path, "rt") as f:
        reader = jsonlines.Reader(f)
        try:
            yield reader
        finally:
            reader.close()


def buffered_yield(size: int):
    """Wraps any generator to yield items in batches of `size`."""
    def wrapper(generator_function):
        def wrapped(*args, **kwargs):
            buffer = []
            for item in generator_function(*args, **kwargs):
                buffer.append(item)
                if len(buffer) == size:
                    yield from buffer
                    buffer = []

            if len(buffer) > 0:
                yield from buffer
        return wrapped
    return wrapper
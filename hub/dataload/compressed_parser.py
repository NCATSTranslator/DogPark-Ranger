import pathlib
from typing import Union, Literal

import jsonlines
from biothings.utils.common import open_anyfile

from hub.dataload.data_parsers import EDGE_BUFFER_SIZE
from hub.dataload.utils.file import buffered_yield


@buffered_yield(EDGE_BUFFER_SIZE)
def read_compressed(
        compressed_file_path: Union[str, pathlib.Path],
        target_file: str,
        gen_id=False,
        expect_id=False
):
    """ Read a target jsonl from a bundled/compressed file without decompressing"""
    with open_anyfile((compressed_file_path, target_file)) as f:
        reader = jsonlines.Reader(f)
        index = 0

        for doc in reader:
            if doc:
                if expect_id and "id" not in doc:
                    raise Exception(f"id is expected for {target_file}")
                if gen_id:
                    doc["_id"] = str(doc["id"]) if "id" in doc else str(index)
                index += 1
                yield doc


def load_from_tar(data_folder: Union[str, pathlib.Path], file_name: str, entity: Literal['edges', 'nodes'], gen_id=True):
    """ Stream edge data from given JSONL file """
    data_folder = pathlib.Path(data_folder).resolve().absolute()
    tar_file = data_folder.joinpath(file_name)
    target_file = f"{entity}.jsonl"
    yield from read_compressed(tar_file, target_file, gen_id, expect_id=True)

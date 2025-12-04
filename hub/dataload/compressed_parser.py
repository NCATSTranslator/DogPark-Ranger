import glob
import os
import pathlib
import tarfile
from typing import Union, Literal, Optional

import jsonlines
import zstandard as zstd
from biothings.utils.common import open_anyfile
from biothings.utils.info import logger

from hub.dataload.data_parsers import EDGE_BUFFER_SIZE
from hub.dataload.utils.file import buffered_yield


@buffered_yield(EDGE_BUFFER_SIZE)
def read_compressed(
        compressed_file_path: Union[str, pathlib.Path],
        target_file: str,
        gen_id=False,
        gen_seq=False,
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
                if gen_seq and "seq_" not in doc:
                    doc["seq_"] = index
                index += 1
                yield doc



def open_zst_tar_member(path: Union[str, pathlib.Path], member_name: str):
    """Stream-decompress a .tar.zst and return a file-like object for a specific member"""
    f = open(path, "rb")
    dctx = zstd.ZstdDecompressor()
    reader = dctx.stream_reader(f)
    tar = tarfile.open(fileobj=reader, mode="r|")  # streaming mode

    for member in tar:
        if member.name == member_name:
            return tar.extractfile(member)
    raise FileNotFoundError(f"{member_name} not found in {path}")


@buffered_yield(EDGE_BUFFER_SIZE)
def read_zst(
        compressed_file_path: Union[str, pathlib.Path],
        target_file: str,
        gen_id=False,
        gen_seq=False,
        expect_id=False
):
    """ Read a target jsonl from a bundled/compressed file without decompressing"""
    with open_zst_tar_member(compressed_file_path, target_file) as f:
        reader = jsonlines.Reader(f)
        index = 0

        for doc in reader:
            if doc:
                if expect_id and "id" not in doc:
                    raise Exception(f"id is expected for {target_file}")
                if gen_id:
                    doc["_id"] = str(doc["id"]) if "id" in doc else str(index)
                if gen_seq and "seq_" not in doc:
                    doc["seq_"] = index
                index += 1
                yield doc

def load_from_zst(data_folder: Union[str, pathlib.Path], entity: Literal['edges', 'nodes'], file_name:Optional[str]=None, gen_id=True, gen_seq=True):
    """ Stream data from given JSONL file """
    data_folder = pathlib.Path(data_folder).resolve().absolute()

    if file_name is None:
        files = [
            f for f in data_folder.glob('*')
            if not os.path.basename(f).startswith(".")
        ]

        if not files:
            raise FileNotFoundError("No valid tar files found.")

        # picking the newest files
        tar_file = max(files, key=os.path.getmtime)

        if len(files) > 1:
            logger.warning(f"Warning: multiple files found, picking newest: {tar_file}")
    else:
        # use file_name if provided
        tar_file = data_folder.joinpath(file_name)

    if not tar_file.exists():
        raise FileNotFoundError(f"File {tar_file} not found.")

    target_file = f"{entity}.jsonl"
    yield from read_zst(tar_file, target_file, gen_id, gen_seq=gen_seq, expect_id=True)


def load_from_tar(data_folder: Union[str, pathlib.Path], entity: Literal['edges', 'nodes'], file_name:Optional[str]=None, gen_id=True, gen_seq=True):
    """ Stream data from given JSONL file """
    data_folder = pathlib.Path(data_folder).resolve().absolute()

    if file_name is None:
        files = [
            f for f in data_folder.glob('*')
            if not os.path.basename(f).startswith(".")
        ]

        if not files:
            raise FileNotFoundError("No valid tar files found.")

        # picking the newest files
        tar_file = max(files, key=os.path.getmtime)

        if len(files) > 1:
            logger.warning(f"Warning: multiple files found, picking newest: {tar_file}")
    else:
        # use file_name if provided
        tar_file = data_folder.joinpath(file_name)

    if not tar_file.exists():
        raise FileNotFoundError(f"File {tar_file} not found.")

    target_file = f"{entity}.jsonl"
    yield from read_compressed(tar_file, target_file, gen_id, gen_seq=gen_seq, expect_id=True)

import pathlib
from collections import defaultdict

import jsonlines
from typing import Union, Literal

from hub.dataload.utils.file import buffered_yield, gz_open
from hub.dataload.utils.flatten_publication import process_publications
from hub.dataload.utils.process_category import process_category
from hub.dataload.utils.process_predicate import process_predicate

NODE_BUFFER_SIZE = 4096
EDGE_BUFFER_SIZE = 2048


@buffered_yield(NODE_BUFFER_SIZE)
def read_jsonl(input_file: Union[str, pathlib.Path], gen_id=False):
    """ Common reader to load data from jsonl files """

    gzip_file = input_file.with_name(input_file.name + ".gz")

    file_loader = jsonlines.open

    if pathlib.Path(gzip_file).exists():
        input_file = gzip_file
        file_loader = gz_open

    with file_loader(input_file) as source:
        index = 0
        for doc in source:
            if doc:
                if gen_id:
                    doc["_id"] = str(doc["id"]) if "id" in doc else str(index)
                index += 1
                yield doc



def loader(data_folder: Union[str, pathlib.Path], entity: Literal['edges', 'nodes'], gen_id=False):
    """ Meta loader to stream edge data from given JSONL file """
    data_folder = pathlib.Path(data_folder).resolve().absolute()
    edge_file = data_folder.joinpath(f"{entity}.jsonl")
    yield from read_jsonl(edge_file, gen_id)


def load_edges(data_folder: Union[str, pathlib.Path]):
    """ Stream edge data from given JSONL file """
    yield from loader(data_folder, "edges", gen_id=True)


def load_nodes(data_folder: Union[str, pathlib.Path]):
    """ Stream node data from given JSONL file """
    yield from loader(data_folder, "nodes", gen_id=True)


def load_edges_with_processing(data_folder: Union[str, pathlib.Path]):
    """ Stream edge data from given JSONL file """
    predicate_cache = {}
    for edge in loader(data_folder, "edges", gen_id=True):
        process_publications(edge)
        process_predicate(edge, predicate_cache, "all_predicates")
        yield edge



def load_nodes_with_processing(data_folder: Union[str, pathlib.Path]):
    """ Stream node data from given JSONL file """
    category_cache = {}

    for node in loader(data_folder, "nodes", gen_id=True):
        process_category(node, category_cache)
        yield node


@buffered_yield(EDGE_BUFFER_SIZE)
def load_merged_edges(data_folder: Union[str, pathlib.Path]):
    """ Generate merged edge data"""

    # use loaded node info as reference dict
    nodes = {node['id']: node for node in load_nodes(data_folder)}

    predicate_cache = {}
    category_cache = {}

    index = 0
    for edge in load_edges(data_folder):
        process_publications(edge)

        subject_id = edge["subject"]
        object_id = edge["object"]

        subject_node = nodes[subject_id]
        object_node = nodes[object_id]

        process_predicate(edge, predicate_cache)
        process_category(subject_node, category_cache)
        process_category(object_node, category_cache)

        edge["subject"] = subject_node
        edge["object"] = object_node

        edge["_id"] = str(edge["id"]) if "id" in edge else str(index)
        index += 1

        yield edge

def build_node_edge_mapping(edges):

    # try to build
    # 0) a node_id -> in/out edge mapping
    # 1) an edge_id -> edge mapping
    nodes_mapping = defaultdict(lambda: {"in" : set(), "out": set()})
    edges_mapping = {}

    for edge in edges:
        edge_id = edge['_id']
        subject_node = edge["subject"]
        object_node = edge["object"]

        nodes_mapping[subject_node]["out"].add(edge_id)
        nodes_mapping[object_node]["in"].add(edge_id)

        edges_mapping[edge_id] = edge

    return nodes_mapping, edges_mapping


@buffered_yield(NODE_BUFFER_SIZE)
def load_adjacency_nodes(data_folder: Union[str, pathlib.Path]):
    # get reference mappings
    nodes_mapping, edges_mapping = build_node_edge_mapping(load_edges(data_folder))
    for node in load_nodes(data_folder):
        node_id = node["_id"]

        node['in_edges'] = [edges_mapping[edge_id] for edge_id in nodes_mapping[node_id]["in"]]
        node['out_edges'] = [edges_mapping[edge_id] for edge_id in nodes_mapping[node_id]["out"]]

        yield node

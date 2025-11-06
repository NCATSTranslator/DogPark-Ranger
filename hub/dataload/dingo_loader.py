from hub.dataload.compressed_parser import load_from_tar
from hub.dataload.utils.pipeline import apply_processors, id_processor


def dingo_node_processor(node):
    processors = [
        id_processor
    ]
    return apply_processors(processors, node)


def dingo_edge_processor(edge):
    processors = [
        id_processor
    ]
    return apply_processors(processors, edge)


def parser(*args, **kwargs):
    entity = kwargs.get('entity')

    if entity is None:
        raise ValueError("No entity specified")

    processor_pipeline = dingo_node_processor if entity == "nodes" else dingo_edge_processor

    yield from map(processor_pipeline, load_from_tar(*args, **kwargs))

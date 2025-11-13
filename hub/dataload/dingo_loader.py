from functools import partial

from hub.dataload.compressed_parser import load_from_tar
from hub.dataload.utils.pipeline import apply_processors
from hub.dataload.utils.process_category import process_category_list
from hub.dataload.utils.process_predicate import process_predicate
from hub.dataload.utils.process_qualifiers import process_qualifiers
from hub.dataload.utils.process_sources import process_sources


def node_processor(node):
    processors = [
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

    yield from map(processor_pipeline, load_from_tar(*args, **kwargs))

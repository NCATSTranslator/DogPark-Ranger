from functools import partial

from hub.dataload.compressed_parser import load_from_tar
from hub.dataload.info_parser import get_adj_list, encapsule, split_n_chunks, to_key_value_pair
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


def node_info_parser(*args, **kwargs):
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
    node_iterator = parser(*args, **node_kwargs)
    # nodes = dict(to_key_value_pair(node_iterator))

    encapsuled = encapsule({
        adj_list_key: adj_list,
    })


    chunks = (10 ** 3) * 5

    payload = []

    node_chunks = split_n_chunks(encapsuled, chunks)
    payload.extend(node_chunks)

    return payload








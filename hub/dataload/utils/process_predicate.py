from hub.dataload.utils.postprocessing import get_ancestors, remove_biolink_prefix

ORIGIN_FIELD_NAME = "predicate"
TARGET_FIELD_NAME = "predicate_ancestors"

def process_predicate(edge, predicate_cache: dict, target = None):
    predicate = edge.get(ORIGIN_FIELD_NAME)

    if target is None:
        target = TARGET_FIELD_NAME

    if predicate:
        ancestors = get_ancestors(predicate, predicate_cache)
        if ancestors:
            edge[target] = ancestors

        edge[ORIGIN_FIELD_NAME] = remove_biolink_prefix(predicate)

    return edge
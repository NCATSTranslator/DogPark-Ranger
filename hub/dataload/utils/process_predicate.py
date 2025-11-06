from hub.dataload.utils.postprocessing import get_ancestors, remove_biolink_prefix

ORIGIN_FIELD_NAME = "predicate"
TARGET_FIELD_NAME = "predicate_ancestors"

def process_predicate(edge, predicate_cache: dict):
    predicate = edge.get(ORIGIN_FIELD_NAME)

    if predicate:
        ancestors = get_ancestors(predicate, predicate_cache)
        if ancestors:
            edge[TARGET_FIELD_NAME] = ancestors

        edge[ORIGIN_FIELD_NAME] = remove_biolink_prefix(predicate)

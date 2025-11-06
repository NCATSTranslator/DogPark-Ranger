from hub.dataload.utils.postprocessing import get_ancestors, remove_biolink_prefix

CAT_FIELD = "category"

def process_categories(categories:list, category_cache: dict):
    ancestor_set = set()
    for category in categories:
        ancestors = get_ancestors(category, category_cache)
        ancestor_set.update(ancestors)

    return list(ancestor_set)


def process_category(node, category_cache: dict):
    category = node.get("category")
    all_categories = node.get("all_categories")

    # if we have all_categories, use that as reference
    # otherwise we use category
    reference = category
    if all_categories:
        reference = all_categories

    ancestors = []
    if type(reference) == list:
        ancestors = process_categories(reference, category_cache)
    elif type(reference) == str:
        ancestors = get_ancestors(reference, category_cache)

    node["all_categories"] = ancestors

    if category:
        node["category"] = remove_biolink_prefix(category)


def process_category_list(node):
    """processor for DINGO datasets, where `category` is already a list"""
    category = node.get(CAT_FIELD)

    if not isinstance(category, list):
        raise TypeError("category must be a list")

    # only processing work is to remove biolink prefixes
    node[CAT_FIELD] = list(map(remove_biolink_prefix, category))

    return node
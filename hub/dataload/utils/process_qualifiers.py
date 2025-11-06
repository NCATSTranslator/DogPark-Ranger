from hub.dataload.utils.postprocessing import biolink

TARGET_FIELD = "qualifiers"

def process_qualifiers(edge):
    """
    merge qualifier fields of a given edge
    """
    qualifier_fields = [
        field for field in edge.keys()
        if biolink.is_qualifier(field)
    ]

    if qualifier_fields:
        edge[TARGET_FIELD] = qualifier_fields

    return edge


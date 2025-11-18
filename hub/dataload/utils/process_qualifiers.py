from hub.dataload.utils.postprocessing import biolink

TARGET_FIELD = "qualifiers"

QUAL_TYPE_NAME = "type_id"
QUAL_VALUE="value"

def process_qualifiers(edge):
    """
    merge qualifier fields of a given edge
    """
    qualifier_fields = []

    for field in list(edge.keys()):
        if biolink.is_qualifier(field):
            qualifier_fields.append({
                QUAL_TYPE_NAME: field,
                QUAL_VALUE: edge[field]
            })

            edge.pop(field)

    if qualifier_fields:
        edge[TARGET_FIELD] = qualifier_fields

    return edge


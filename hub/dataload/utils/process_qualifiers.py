from hub.dataload.utils.postprocessing import biolink, remove_biolink_prefix

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
            if type(edge[field]) is not str:
                raise TypeError("entry of an qualifier value must be a string")

            qualifier_fields.append({
                QUAL_TYPE_NAME: remove_biolink_prefix(field),
                QUAL_VALUE: remove_biolink_prefix(edge[field])
            })

            edge.pop(field)

    if qualifier_fields:
        edge[TARGET_FIELD] = qualifier_fields

    return edge


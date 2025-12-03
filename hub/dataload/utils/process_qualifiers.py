from hub.dataload.utils.postprocessing import biolink, remove_biolink_prefix

# not used, just for reference
QUALIFIERS = [
    "subject_form_or_variant_qualifier",
    "qualified_predicate",
    "disease_context_qualifier",
    "frequency_qualifier",
    "onset_qualifier",
    "sex_qualifier",
]

def process_qualifiers(edge):
    """
    merge qualifier fields of a given edge
    """
    qualifier_fields = []

    for field in list(edge.keys()):
        if biolink.is_qualifier(field):
            if type(edge[field]) is not str:
                raise TypeError("entry of an qualifier value must be a string. Edge:", edge)

            field_stripped: str = remove_biolink_prefix(field)
            if field_stripped != field:
                # remove old field, shouldn't need to though
                edge.pop(field)

            edge[field_stripped] = remove_biolink_prefix(edge[field])


    return edge


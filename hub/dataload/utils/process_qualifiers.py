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
            field_value = edge[field]

            if type(field_value) is str or type(field_value) is list:
                field_value = remove_biolink_prefix(field_value)
            else:
                raise TypeError("entry of an qualifier value must be a string or a list of string. Edge:", edge, "field:", field)

            field_stripped: str = remove_biolink_prefix(field)
            if field_stripped != field:
                # remove old field, shouldn't need to though
                edge.pop(field)

            edge[field_stripped] = field_value


    return edge


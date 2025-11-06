
INFORES_FIELD="source_inforeses"
SOURCE_FIELD="sources"
RESOURCE_ROLE_FIELD= "resource_role"
RESOURCE_ID_FIELD= "resource_id"


def process_sources(edge):
    """
    processor to format `source_inforeses` and `sources` fields.

    1. Every field ending with "_source" will be added to `sources` with its value.
    2. All such field names and any existing `sources` entries will appear in `source_inforeses`.
    """


    fields_not_in_sources = set()
    inforeses = set()

    source_fields = [
        field for field in edge.keys()
        if field.endswith("_source")
    ]

    fields_not_in_sources.update(source_fields)


    # get sources fields and create it if not present
    sources = edge.get(SOURCE_FIELD, [])
    edge[SOURCE_FIELD] = sources

    # loop over `sources` fields
    for source_entry in sources:
        resource_role = source_entry.get(RESOURCE_ROLE_FIELD)

        # skip invalid entries
        if not resource_role:
            continue

        # individual source field already in `sources`
        if resource_role in fields_not_in_sources:
            fields_not_in_sources.remove(resource_role)
            del edge[resource_role]

        # update inforeses
        inforeses.add(resource_role)


    # check if any individual fields not in sources
    while fields_not_in_sources:
        field = fields_not_in_sources.pop()

        # update inforeses
        inforeses.add(field)

        # update sources field
        field_value = edge[field]
        sources.append({
            RESOURCE_ROLE_FIELD: field,
            RESOURCE_ID_FIELD: field_value,
        })

        # delete that field
        del edge[field]

    # update inforeses field
    edge[INFORES_FIELD] = sorted(inforeses)

    return edge
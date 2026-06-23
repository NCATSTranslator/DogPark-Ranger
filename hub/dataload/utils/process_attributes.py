from typing import Any

from typing_extensions import Literal

from hub.dataload.mapping import kg_mapping
from hub.dataload.utils.postprocessing import biolink


Entity = Literal["nodes", "edges"]
ATTRIBUTE_FIELD = "attributes"


DINGO_KG_EDGE_TOPLEVEL_VALUES = {
    "binding",
    "category",
    "direction",
    "predicate",
    "predicate_ancestors",
    "node",
    "sources",
    "source_inforeses",
    "id",
    "_id",
    "subject",
    "object",
    "_index",
    "seq_",
    "negated",  # Should only ever show up as false, field to be removed in future
    "eid",
}


DINGO_KG_NODE_TOPLEVEL_VALUES = {
    "binding",
    "id",
    "_id",
    "name",
    "edges",
    "category",
}


EDGE_TOPLEVEL_FIELDS = DINGO_KG_EDGE_TOPLEVEL_VALUES | set(
    kg_mapping.merged_edges_mapping(None)
)
NODE_TOPLEVEL_FIELDS = DINGO_KG_NODE_TOPLEVEL_VALUES | set(
    kg_mapping.nodes_mapping(None)
)


def process_attributes(current, entity: Entity):
    """Move unmapped, non-qualifier fields into a source-only attributes object."""
    existing_attributes = current.get(ATTRIBUTE_FIELD)
    attributes: dict[str, Any] = (
        dict(existing_attributes) if isinstance(existing_attributes, dict) else {}
    )
    if entity == "edges":
        top_level_fields = EDGE_TOPLEVEL_FIELDS
    elif entity == "nodes":
        top_level_fields = NODE_TOPLEVEL_FIELDS
    else:
        raise ValueError(f"Unknown entity: {entity!r}")

    for key, value in list(current.items()):
        if (
            key == ATTRIBUTE_FIELD
            or key in top_level_fields
            or biolink.is_qualifier(key)
        ):
            continue
        attributes[key] = value
        del current[key]

    current[ATTRIBUTE_FIELD] = attributes

    # todo possible way to reduce redundancy:
    #  top-level attributes indexed on ES, but excluded in store source
    #  (script/runtime field, autogen at indexing time)

    return current

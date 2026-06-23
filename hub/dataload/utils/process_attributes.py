from typing import Any

from typing_extensions import Literal

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


def process_attributes(current, entity: Entity):
    """Collect non-core, non-qualifier fields into a source-only attributes object."""
    existing_attributes = current.get(ATTRIBUTE_FIELD)
    attributes: dict[str, Any] = (
        dict(existing_attributes) if isinstance(existing_attributes, dict) else {}
    )
    if entity == "edges":
        top_level_fields = DINGO_KG_EDGE_TOPLEVEL_VALUES
    elif entity == "nodes":
        top_level_fields = DINGO_KG_NODE_TOPLEVEL_VALUES
    else:
        raise ValueError(f"Unknown entity: {entity!r}")

    for key, value in current.items():
        if (
            key == ATTRIBUTE_FIELD
            or key in top_level_fields
            or biolink.is_qualifier(key)
        ):
            continue
        attributes[key] = value

    current[ATTRIBUTE_FIELD] = attributes

    # todo possible way to reduce redundancy:
    #  top-level attributes indexed on ES, but excluded in store source
    #  (script/runtime field, autogen at indexing time)

    return current

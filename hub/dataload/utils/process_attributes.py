from typing import Any

from typing_extensions import Literal

from hub.dataload.mapping import kg_mapping
from hub.dataload.utils.postprocessing import biolink


Entity = Literal["nodes", "edges"]
ATTRIBUTE_FIELD = "attributes"
ATTRIBUTE_TYPE_ID_FIELD = "attribute_type_id"
VALUE_FIELD = "value"


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


def build_attribute(attribute_type_id: str, value: Any) -> dict[str, Any]:
    return {
        ATTRIBUTE_TYPE_ID_FIELD: attribute_type_id,
        VALUE_FIELD: value,
    }


def normalize_attributes(existing_attributes: Any) -> list[dict[str, Any]]:
    if isinstance(existing_attributes, dict):
        if ATTRIBUTE_TYPE_ID_FIELD in existing_attributes and VALUE_FIELD in existing_attributes:
            return [existing_attributes]

        return [
            build_attribute(attribute_type_id, value)
            for attribute_type_id, value in existing_attributes.items()
        ]

    if isinstance(existing_attributes, list):
        return [
            item
            if isinstance(item, dict)
            and ATTRIBUTE_TYPE_ID_FIELD in item
            and VALUE_FIELD in item
            else build_attribute(ATTRIBUTE_FIELD, item)
            for item in existing_attributes
        ]

    return []


def process_attributes(current, entity: Entity):
    """Move unmapped, non-qualifier fields into source-only attribute objects."""
    existing_attributes = current.get(ATTRIBUTE_FIELD)
    attributes = normalize_attributes(existing_attributes)
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
        attributes.append(build_attribute(key, value))
        del current[key]

    current[ATTRIBUTE_FIELD] = attributes

    # todo possible way to reduce redundancy:
    #  top-level attributes indexed on ES, but excluded in store source
    #  (script/runtime field, autogen at indexing time)

    return current

from typing_extensions import Literal

from hub.dataload.utils.process_qualifiers import get_qualifier_fields


Entity = Literal["nodes", "edges"]

_CORE_EDGE_FIELDS = {
    "id",
    "_id",
    "category",
    "subject",
    "object",
    "predicate",
    "sources",
    "primary_knowledge_source",
    "aggregator_knowledge_source",
}
_CORE_NODE_FIELDS = {"id", "_id", "name", "category"}


def _extract_edge_attributes(edge):
    qualifier_fields = get_qualifier_fields()
    attributes = []
    for field, value in edge.items():
        if (
            field in _CORE_EDGE_FIELDS
            or field in qualifier_fields
            or field == "qualifiers"
        ):
            continue
        attributes.append(
            {
                "attribute_type_id": f"biolink:{field}",
                "value": value,
                "original_attribute_name": field,
            }
        )
    return attributes


def _extract_node_attributes(node):
    attributes = []
    for field, value in node.items():
        if field in _CORE_NODE_FIELDS:
            continue
        attributes.append(
            {
                "attribute_type_id": "biolink:Attribute",
                "value": value,
                "original_attribute_name": field,
            }
        )
    return attributes


def process_attributes(current, entity: Entity):
    """Return Gandalf's normalized KGX node/edge document shape."""
    if entity == "edges":
        return {
            "subject": current["subject"],
            "object": current["object"],
            "predicate": current["predicate"],
            "id": current.get("id"),
            "sources": current["sources"],
            "qualifiers": current["qualifiers"],
            "attributes": _extract_edge_attributes(current),
        }

    if entity == "nodes":
        return {
            "id": current.get("id"),
            "name": current.get("name"),
            "categories": current.get("category", []),
            "attributes": _extract_node_attributes(current),
        }

    raise ValueError(f"Unknown entity: {entity!r}")

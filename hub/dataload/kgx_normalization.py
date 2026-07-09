"""KGX-only normalization for Gandalf-compatible tier0 Mongo loads.

The generic processors in ``hub.dataload.utils`` are shared by many DogPark
data sources and keep their historical indexing-oriented behavior.  This module
is intentionally scoped to the tier0 KGX parser: it reshapes raw KGX
``nodes.jsonl`` / ``edges.jsonl`` rows into the normalized document contract
that Gandalf's Mongo loader expects.
"""

import json
import logging

from bmt.toolkit import Toolkit


BIOLINK_VERSION = "4.3.2"
BIOLINK_PREFIX = "biolink:"
GANDALF_INFORES = "infores:dogpark-tier0"

_SCHEMA_URL = (
    "https://raw.githubusercontent.com/biolink/biolink-model/"
    "refs/tags/v{version}/biolink-model.yaml"
)
_PREDICATE_MAP_URL = (
    "https://raw.githubusercontent.com/biolink/biolink-model/"
    "refs/tags/v{version}/predicate_mapping.yaml"
)

_CORE_EDGE_FIELDS = {
    "id",
    "_id",
    "seq_",
    "category",
    "subject",
    "object",
    "predicate",
    "sources",
    "primary_knowledge_source",
    "aggregator_knowledge_source",
}
_CORE_NODE_FIELDS = {"id", "_id", "seq_", "name", "category"}

_FALLBACK_QUALIFIER_FIELDS = {
    "anatomical_context_qualifier",
    "aspect_qualifier",
    "causal_mechanism_qualifier",
    "context_qualifier",
    "derivative_qualifier",
    "direction_qualifier",
    "disease_context_qualifier",
    "form_or_variant_qualifier",
    "frequency_qualifier",
    "object_aspect_qualifier",
    "object_context_qualifier",
    "object_derivative_qualifier",
    "object_direction_qualifier",
    "object_form_or_variant_qualifier",
    "object_part_qualifier",
    "object_specialization_qualifier",
    "onset_qualifier",
    "part_qualifier",
    "population_context_qualifier",
    "qualified_predicate",
    "response_context_qualifier",
    "response_target_context_qualifier",
    "severity_qualifier",
    "sex_qualifier",
    "specialization_qualifier",
    "species_context_qualifier",
    "stage_qualifier",
    "statement_qualifier",
    "subject_aspect_qualifier",
    "subject_context_qualifier",
    "subject_derivative_qualifier",
    "subject_direction_qualifier",
    "subject_form_or_variant_qualifier",
    "subject_part_qualifier",
    "subject_specialization_qualifier",
    "temporal_context_qualifier",
    "temporal_interval_qualifier",
}


GANDALF_NORMALIZATION_STEPS = [
    "nodes normalized to id/name/categories/attributes",
    "edges normalized to subject/object/predicate/id/sources/qualifiers/attributes",
    "edge sources normalized and dogpark-tier0 aggregator prepended",
    "edge qualifier fields copied to TRAPI qualifiers list",
    "node/edge non-core fields copied to TRAPI attributes list",
]

logger = logging.getLogger(__name__)
_bmt = None
_qualifier_fields = None


def _get_bmt():
    global _bmt
    if _bmt is None:
        _bmt = Toolkit(
            schema=_SCHEMA_URL.format(version=BIOLINK_VERSION),
            predicate_map=_PREDICATE_MAP_URL.format(version=BIOLINK_VERSION),
        )
    return _bmt


def get_qualifier_fields():
    """Return raw KGX edge fields that should become TRAPI qualifiers."""
    global _qualifier_fields
    if _qualifier_fields is None:
        try:
            descendants = _get_bmt().get_descendants("qualifier", formatted=True)
            fields = {descendant.split(":", 1)[-1] for descendant in descendants}
            fields.discard("qualifier")
            _qualifier_fields = fields or set(_FALLBACK_QUALIFIER_FIELDS)
        except Exception:
            logger.warning(
                "Could not load Biolink qualifier slots from BMT; "
                "using fallback qualifier set",
                exc_info=True,
            )
            _qualifier_fields = set(_FALLBACK_QUALIFIER_FIELDS)
    return _qualifier_fields


def _ensure_biolink_prefix(value):
    local = value.split(":", 1)[1] if ":" in value else value
    return f"{BIOLINK_PREFIX}{local}"


def _extract_sources(raw):
    sources = []
    raw_sources = raw.get("sources", [])

    if len(raw_sources) == 0:
        sources.append(
            {
                "resource_id": raw["primary_knowledge_source"],
                "resource_role": "primary_knowledge_source",
                "upstream_resource_ids": [],
            }
        )

        if (
            "aggregator_knowledge_source" in raw
            and len(raw["aggregator_knowledge_source"]) > 0
        ):
            previous_source = raw["primary_knowledge_source"]
            for aggregator_source in reversed(raw["aggregator_knowledge_source"]):
                sources.append(
                    {
                        "resource_id": aggregator_source,
                        "resource_role": "aggregator_knowledge_source",
                        "upstream_resource_ids": [previous_source],
                    }
                )
                previous_source = aggregator_source
    else:
        sources = []
        for source in raw_sources:
            src = {
                "resource_id": source["resource_id"],
                "resource_role": source["resource_role"],
                "upstream_resource_ids": source.get("upstream_resource_ids", []),
            }
            if source.get("source_record_urls"):
                src["source_record_urls"] = source["source_record_urls"]
            sources.append(src)

    all_upstream = {uid for source in sources for uid in source["upstream_resource_ids"]}
    top_ids = [
        source["resource_id"]
        for source in sources
        if source["resource_id"] not in all_upstream
    ]

    return [
        {
            "resource_id": GANDALF_INFORES,
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": top_ids,
        }
    ] + sources


def _qualifier_value(field, value):
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if field == "qualified_predicate":
        value = _ensure_biolink_prefix(value)
    return value


def _extract_qualifiers(raw, unique_qualifier_set=None):
    qualifiers = []
    for field in get_qualifier_fields():
        if field not in raw:
            continue
        qualifiers.append(
            {
                "qualifier_type_id": f"{BIOLINK_PREFIX}{field}",
                "qualifier_value": _qualifier_value(field, raw[field]),
            }
        )
        if unique_qualifier_set is not None:
            unique_qualifier_set.add(field)
    return qualifiers


def _extract_edge_attributes(raw):
    qualifier_fields = get_qualifier_fields()
    attributes = []
    for field, value in raw.items():
        if field in _CORE_EDGE_FIELDS or field in qualifier_fields or field == "qualifiers":
            continue
        attributes.append(
            {
                "attribute_type_id": f"{BIOLINK_PREFIX}{field}",
                "value": value,
                "original_attribute_name": field,
            }
        )
    return attributes


def _extract_node_attributes(raw):
    attributes = []
    for field, value in raw.items():
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


def normalize_kgx_edge(raw, unique_qualifier_set=None):
    """Normalize one raw KGX edge row for Gandalf's Mongo source."""
    return {
        "subject": raw["subject"],
        "object": raw["object"],
        "predicate": raw["predicate"],
        "id": raw.get("id"),
        "sources": _extract_sources(raw),
        "qualifiers": _extract_qualifiers(raw, unique_qualifier_set),
        "attributes": _extract_edge_attributes(raw),
    }


def normalize_kgx_node(raw):
    """Normalize one raw KGX node row for Gandalf's Mongo source."""
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "categories": raw.get("category", []),
        "attributes": _extract_node_attributes(raw),
    }

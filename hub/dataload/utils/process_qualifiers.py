import json
import logging

from bmt.toolkit import Toolkit


QUALIFIED_PREDICATE_FIELD = "qualified_predicate"
BIOLINK_PREFIX = "biolink:"
BIOLINK_VERSION = "4.3.2"

_SCHEMA_URL = (
    "https://raw.githubusercontent.com/biolink/biolink-model/"
    "refs/tags/v{version}/biolink-model.yaml"
)
_PREDICATE_MAP_URL = (
    "https://raw.githubusercontent.com/biolink/biolink-model/"
    "refs/tags/v{version}/predicate_mapping.yaml"
)
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


def _qualifier_value(field, value):
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if field == QUALIFIED_PREDICATE_FIELD:
        value = _ensure_biolink_prefix(value)
    return value


def process_qualifiers(edge, unique_qualifier_set: set):
    """Extract TRAPI qualifiers the same way Gandalf's KGX loader does."""
    qualifier_fields = get_qualifier_fields()
    qualifiers = []

    for field in qualifier_fields:
        if field not in edge:
            continue
        value = edge[field]
        qualifiers.append(
            {
                "qualifier_type_id": f"{BIOLINK_PREFIX}{field}",
                "qualifier_value": _qualifier_value(field, value),
            }
        )
        unique_qualifier_set.add(field)

    edge["qualifiers"] = qualifiers
    return edge

"""Tests for hub.dataload.kgx_normalization: the tier0 KGX -> Gandalf contract.

Stubs bmt the same way tests/test_process_attributes.py does, but one level
deeper: kgx_normalization imports ``from bmt.toolkit import Toolkit``
specifically, so the stub has to register a ``bmt.toolkit`` submodule, not
just a ``bmt`` package with a bare ``Toolkit`` attribute (a plain
``sys.modules["bmt"] = SimpleNamespace(Toolkit=...)`` raises
``ModuleNotFoundError: No module named 'bmt.toolkit'``).
"""

import sys
import types
import unittest
from unittest import mock


class FakeToolkit:
    """Always falls back to kgx_normalization's own _FALLBACK_QUALIFIER_FIELDS,
    which is itself real production code -- this exercises that fallback
    directly instead of hand-maintaining a second copy of the qualifier list
    in test data that could silently drift from it.
    """

    def __init__(self, **kwargs):
        pass

    def get_descendants(self, *args, **kwargs):
        raise RuntimeError("network access to the Biolink schema is disabled in tests")


_fake_toolkit_module = types.ModuleType("bmt.toolkit")
_fake_toolkit_module.Toolkit = FakeToolkit
_fake_bmt_package = types.ModuleType("bmt")
_fake_bmt_package.toolkit = _fake_toolkit_module

# mock.patch.dict restores sys.modules on exit (same convention as
# tests/test_kg_uploader.py), so kgx_normalization is imported here, under
# the stub, and the module object held onto -- re-importing it by name
# later would just return this same cached object anyway, since Python
# only executes a module's top-level code once per process. Restoring
# sys.modules afterward matters because a plain, un-restored assignment
# would leak into tests/test_process_attributes.py, which needs a
# differently-shaped bmt stub (a bare top-level ``bmt.Toolkit``, not
# ``bmt.toolkit.Toolkit``) for a module this one doesn't otherwise touch.
with mock.patch.dict(
    sys.modules,
    {"bmt": _fake_bmt_package, "bmt.toolkit": _fake_toolkit_module},
):
    from hub.dataload import kgx_normalization as kgxn

try:
    import gandalf.sources.base as gandalf_base

    HAVE_GANDALF = True
except ImportError:
    HAVE_GANDALF = False


# A raw tier0 KGX edge row as it actually looks today (see
# tmp_gandalf_mongo_fixture/edges.jsonl): knowledge_level/agent_type present
# at the top level, no pre-built "sources" list.
RAW_EDGE_WITH_KL_AT = {
    "id": "uuid:1",
    "subject": "CHEBI:45783",
    "object": "MONDO:0005148",
    "predicate": "biolink:treats",
    "category": ["biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation"],
    "knowledge_level": "knowledge_assertion",
    "agent_type": "manual_agent",
    "primary_knowledge_source": "infores:ctd",
    "aggregator_knowledge_source": ["infores:dogpark-tier0-upstream"],
    "object_aspect_qualifier": "activity",
    "object_direction_qualifier": "increased",
    "publications": ["PMID:12345"],
}

# Some KGX rows predate the knowledge_level/agent_type requirement -- Gandalf's
# contract treats both as optional, and the fix must not invent values for them.
RAW_EDGE_WITHOUT_KL_AT = {
    "id": "uuid:2",
    "subject": "NCBIGene:1017",
    "object": "GO:0000082",
    "predicate": "biolink:actively_involved_in",
    "sources": [
        {"resource_id": "infores:goa", "resource_role": "primary_knowledge_source", "upstream_resource_ids": []}
    ],
}

# The other branch of _extract_sources(): upstream already provides a
# "sources" list instead of bare primary/aggregator_knowledge_source fields.
RAW_EDGE_WITH_EXISTING_SOURCES = {
    "id": "uuid:3",
    "subject": "HGNC:1100",
    "object": "MONDO:0007254",
    "predicate": "biolink:gene_associated_with_condition",
    "knowledge_level": "prediction",
    "agent_type": "automated_agent",
    "sources": [
        {"resource_id": "infores:ctd", "resource_role": "primary_knowledge_source", "upstream_resource_ids": []},
        {
            "resource_id": "infores:dgidb",
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": ["infores:ctd"],
        },
    ],
    "qualified_predicate": "biolink:causes",
}

ALL_RAW_EDGES = (RAW_EDGE_WITH_KL_AT, RAW_EDGE_WITHOUT_KL_AT, RAW_EDGE_WITH_EXISTING_SOURCES)

RAW_NODES = (
    {"id": "MONDO:0005148", "name": "type 2 diabetes mellitus", "category": ["biolink:Disease"]},
    {"id": "CHEBI:45783", "name": None, "category": ["biolink:ChemicalEntity", "biolink:SmallMolecule"]},
    {"id": "NCBIGene:1017"},  # no name, no category at all
)


class NormalizeKgxEdgeTest(unittest.TestCase):
    def setUp(self):
        # Force the fallback qualifier path deterministically, regardless of
        # whatever another test module in the same process already cached.
        kgxn._bmt = None
        kgxn._qualifier_fields = None

    def test_knowledge_level_and_agent_type_are_top_level(self):
        doc = kgxn.normalize_kgx_edge(RAW_EDGE_WITH_KL_AT, set())
        self.assertEqual(doc["knowledge_level"], "knowledge_assertion")
        self.assertEqual(doc["agent_type"], "manual_agent")

    def test_knowledge_level_and_agent_type_are_not_duplicated_into_attributes(self):
        doc = kgxn.normalize_kgx_edge(RAW_EDGE_WITH_KL_AT, set())
        attribute_names = {a["original_attribute_name"] for a in doc["attributes"]}
        self.assertNotIn("knowledge_level", attribute_names)
        self.assertNotIn("agent_type", attribute_names)

    def test_missing_knowledge_level_and_agent_type_are_none_not_absent(self):
        # Gandalf's contract treats these as optional dedicated fields, not
        # optional *keys*: the loader calls .get() on them and expects None,
        # never a KeyError, when upstream never provided a value.
        doc = kgxn.normalize_kgx_edge(RAW_EDGE_WITHOUT_KL_AT, set())
        self.assertIn("knowledge_level", doc)
        self.assertIn("agent_type", doc)
        self.assertIsNone(doc["knowledge_level"])
        self.assertIsNone(doc["agent_type"])

    def test_qualifiers_extracted_via_real_fallback_path(self):
        doc = kgxn.normalize_kgx_edge(RAW_EDGE_WITH_KL_AT, set())
        qualifier_ids = {q["qualifier_type_id"] for q in doc["qualifiers"]}
        self.assertEqual(
            qualifier_ids,
            {"biolink:object_aspect_qualifier", "biolink:object_direction_qualifier"},
        )

    def test_sources_prepend_dogpark_tier0_aggregator(self):
        doc = kgxn.normalize_kgx_edge(RAW_EDGE_WITH_KL_AT, set())
        self.assertEqual(doc["sources"][0]["resource_id"], "infores:dogpark-tier0")
        self.assertEqual(doc["sources"][0]["resource_role"], "aggregator_knowledge_source")

    @unittest.skipUnless(HAVE_GANDALF, "gandalf-csr not installed; skipping live contract check")
    def test_matches_gandalfs_real_normalized_edge_contract(self):
        """Runs Gandalf's own validator (gandalf.sources.base.validate_normalized_edge)
        directly against our output, so drift between the two sides of this
        contract is a test failure here, not a silent surprise at build time.
        """
        for raw in ALL_RAW_EDGES:
            doc = kgxn.normalize_kgx_edge(raw, set())
            gandalf_base.validate_normalized_edge(doc)


class NormalizeKgxNodeTest(unittest.TestCase):
    def test_core_fields(self):
        doc = kgxn.normalize_kgx_node(
            {"id": "MONDO:0005148", "name": "type 2 diabetes mellitus", "category": ["biolink:Disease"]}
        )
        self.assertEqual(doc["id"], "MONDO:0005148")
        self.assertEqual(doc["name"], "type 2 diabetes mellitus")
        self.assertEqual(doc["categories"], ["biolink:Disease"])
        self.assertEqual(doc["attributes"], [])

    def test_missing_name_and_category_default_sensibly(self):
        doc = kgxn.normalize_kgx_node({"id": "NCBIGene:1017"})
        self.assertIsNone(doc["name"])
        self.assertEqual(doc["categories"], [])

    @unittest.skipUnless(HAVE_GANDALF, "gandalf-csr not installed; skipping live contract check")
    def test_matches_gandalfs_real_normalized_node_contract(self):
        for raw in RAW_NODES:
            doc = kgxn.normalize_kgx_node(raw)
            gandalf_base.validate_normalized_node(doc)


if __name__ == "__main__":
    unittest.main()

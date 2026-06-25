import sys
import types
import unittest


class FakeToolkit:
    def is_qualifier(self, field):
        return field.endswith("_qualifier")


sys.modules.setdefault("bmt", types.SimpleNamespace(Toolkit=lambda: FakeToolkit()))

from hub.dataload.utils.process_attributes import process_attributes  # noqa: E402


class ProcessAttributesTest(unittest.TestCase):
    def test_moves_unmapped_fields_to_attribute_objects(self):
        node = {
            "id": "MONDO:0000001",
            "name": "example",
            "category": ["biolink:Disease"],
            "biolink:source_web_page": "https://example.org/source",
        }

        process_attributes(node, "nodes")

        self.assertEqual(
            node["attributes"],
            [
                {
                    "attribute_type_id": "biolink:source_web_page",
                    "value": "https://example.org/source",
                }
            ],
        )
        self.assertNotIn("biolink:source_web_page", node)

    def test_omits_empty_attributes(self):
        node = {
            "id": "MONDO:0000001",
            "name": "example",
            "category": ["biolink:Disease"],
        }

        process_attributes(node, "nodes")

        self.assertNotIn("attributes", node)

    def test_normalizes_existing_dict_attributes(self):
        edge = {
            "id": "edge-1",
            "subject": "CHEBI:1",
            "object": "MONDO:1",
            "predicate": "related_to",
            "attributes": {
                "biolink:knowledge_level": "prediction",
            },
            "custom_score": 0.42,
        }

        process_attributes(edge, "edges")

        self.assertEqual(
            edge["attributes"],
            [
                {
                    "attribute_type_id": "biolink:knowledge_level",
                    "value": "prediction",
                },
                {
                    "attribute_type_id": "custom_score",
                    "value": 0.42,
                },
            ],
        )

    def test_preserves_existing_attribute_objects(self):
        edge = {
            "id": "edge-1",
            "subject": "CHEBI:1",
            "object": "MONDO:1",
            "predicate": "related_to",
            "attributes": [
                {
                    "attribute_type_id": "biolink:agent_type",
                    "value": "manual_agent",
                    "value_type_id": "metatype:String",
                }
            ],
            "extra_note": "kept as an attribute",
        }

        process_attributes(edge, "edges")

        self.assertEqual(
            edge["attributes"],
            [
                {
                    "attribute_type_id": "biolink:agent_type",
                    "value": "manual_agent",
                    "value_type_id": "metatype:String",
                },
                {
                    "attribute_type_id": "extra_note",
                    "value": "kept as an attribute",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()

import sys
import types
import unittest
from unittest import mock


class FakeBaseSourceUploader:
    """Mimics the part of BaseSourceUploader that KGXUploader builds on."""

    def generate_doc_src_master(self):
        doc = {"_id": self.name}
        if hasattr(self.__class__, "__metadata__"):
            doc.update(self.__class__.__metadata__)
        return doc


fake_uploader_module = types.ModuleType("biothings.hub.dataload.uploader")
fake_uploader_module.BaseSourceUploader = FakeBaseSourceUploader

fake_dumper_module = types.ModuleType("biothings.hub.dataload.dumper")
fake_dumper_module.LastModifiedHTTPDumper = object

fake_config_module = types.ModuleType("config")
fake_config_module.logger = mock.Mock()

with mock.patch.dict(
    sys.modules,
    {
        "biothings.hub.dataload.uploader": fake_uploader_module,
        "biothings.hub.dataload.dumper": fake_dumper_module,
        "config": fake_config_module,
    },
):
    from hub.dataload import kgDumper as kg_dumper_module
    from hub.dataload.uploader import kgUploader as kg_uploader_module

# mock.patch.dict restores sys.modules on exit, dropping everything imported
# above, so hold on to the module objects rather than re-importing by name.
KGX_METADATA_FIELD = kg_dumper_module.KGX_METADATA_FIELD
KGXUploader = kg_uploader_module.KGXUploader


GRAPH_URL = "https://example.org/graph-metadata.json"
RELEASE_URL = "https://example.org/latest-release.json"
MANIFEST_SRC_META = {"graph": GRAPH_URL, "release": RELEASE_URL, "license": "CC0"}
# as every plugin manifest declares them, relative to the dumper's data_url
RELATIVE_SRC_META = {"graph": "graph-metadata.json", "release": "../latest-release.json", "license": "CC0"}


def make_uploader(src_doc=None, parser_metadata=None, src_meta=None):
    """A stand-in for a generated KGXUploader subclass at master-doc time."""

    class Uploader(KGXUploader):
        name = "example_edges"
        __metadata__ = {"src_meta": dict(MANIFEST_SRC_META if src_meta is None else src_meta)}

    uploader = Uploader()
    uploader.fullname = "example.example_edges"
    uploader.src_doc = dict(src_doc or {})
    uploader.parser_metadata = dict(parser_metadata or {})
    return uploader


class KGXUploaderMetadataTest(unittest.TestCase):
    def test_uses_dump_time_metadata(self):
        uploader = make_uploader(
            src_doc={
                KGX_METADATA_FIELD: {
                    "graph": {"nodes": 12},
                    "release": {"version": "2026_07_21"},
                }
            }
        )

        doc = uploader.generate_doc_src_master()

        self.assertEqual(doc["src_meta"]["graph"], {"nodes": 12})
        self.assertEqual(doc["src_meta"]["release"], {"version": "2026_07_21"})
        self.assertEqual(doc["src_meta"]["license"], "CC0")

    def test_omits_relative_declarations_without_dump_time_metadata(self):
        """The manifests' real declarations: unresolvable here, so never published as-is."""
        uploader = make_uploader(src_meta=dict(RELATIVE_SRC_META))

        fake_config_module.logger.reset_mock()
        doc = uploader.generate_doc_src_master()

        self.assertEqual(fake_config_module.logger.warning.call_count, 2)

        self.assertNotIn("graph", doc["src_meta"])
        self.assertNotIn("release", doc["src_meta"])
        self.assertEqual(doc["src_meta"]["license"], "CC0")

    def test_omits_only_the_keys_without_dump_time_metadata(self):
        uploader = make_uploader(
            src_doc={KGX_METADATA_FIELD: {"release": {"version": "2026_07_21"}}},
            src_meta=dict(RELATIVE_SRC_META),
        )

        doc = uploader.generate_doc_src_master()

        self.assertNotIn("graph", doc["src_meta"])
        self.assertEqual(doc["src_meta"]["release"], {"version": "2026_07_21"})

    def test_passes_inlined_documents_through(self):
        uploader = make_uploader(src_meta={"release": {"version": "pinned"}})

        doc = uploader.generate_doc_src_master()

        self.assertEqual(doc["src_meta"]["release"], {"version": "pinned"})

    def test_picks_up_a_new_release_on_a_later_run(self):
        """__metadata__ is overwritten on each run, so the manifest declarations must survive it."""
        uploader = make_uploader(src_doc={KGX_METADATA_FIELD: {"release": {"version": "2026_07_20"}}})

        first = uploader.generate_doc_src_master()
        uploader.src_doc = {KGX_METADATA_FIELD: {"release": {"version": "2026_07_21"}}}
        second = uploader.generate_doc_src_master()

        self.assertEqual(first["src_meta"]["release"], {"version": "2026_07_20"})
        self.assertEqual(second["src_meta"]["release"], {"version": "2026_07_21"})

    def test_merges_parser_metadata(self):
        uploader = make_uploader(
            src_doc={KGX_METADATA_FIELD: {"release": {"version": "2026_07_21"}}},
            parser_metadata={"qualifier_fields": ["object_direction"]},
            src_meta={"release": RELEASE_URL},
        )

        doc = uploader.generate_doc_src_master()

        self.assertEqual(doc["src_meta"]["qualifier_fields"], ["object_direction"])
        self.assertEqual(doc["src_meta"]["release"], {"version": "2026_07_21"})


if __name__ == "__main__":
    unittest.main()

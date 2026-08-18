import sys
import types
import unittest
from unittest import mock

import requests


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
    def test_uses_dump_time_metadata_without_fetching(self):
        uploader = make_uploader(
            src_doc={
                KGX_METADATA_FIELD: {
                    "graph": {"nodes": 12},
                    "release": {"version": "2026_07_21"},
                }
            }
        )

        with mock.patch.object(kg_uploader_module.requests, "get") as fetch:
            doc = uploader.generate_doc_src_master()

        fetch.assert_not_called()
        self.assertEqual(doc["src_meta"]["graph"], {"nodes": 12})
        self.assertEqual(doc["src_meta"]["release"], {"version": "2026_07_21"})
        self.assertEqual(doc["src_meta"]["license"], "CC0")

    def test_falls_back_to_fetching_without_dump_time_metadata(self):
        uploader = make_uploader()

        with mock.patch.object(kg_uploader_module.requests, "get") as fetch:
            fetch.return_value.json.return_value = {"version": "2026_07_21"}
            doc = uploader.generate_doc_src_master()

        self.assertEqual([call.args[0] for call in fetch.call_args_list], [GRAPH_URL, RELEASE_URL])
        self.assertEqual(doc["src_meta"]["release"], {"version": "2026_07_21"})

    def test_leaves_url_in_place_when_fallback_fetch_fails(self):
        uploader = make_uploader()

        with mock.patch.object(kg_uploader_module.requests, "get") as fetch:
            fetch.side_effect = requests.exceptions.ConnectionError("boom")
            doc = uploader.generate_doc_src_master()

        self.assertEqual(doc["src_meta"]["release"], RELEASE_URL)

    def test_picks_up_a_new_release_on_a_later_run(self):
        """__metadata__ is overwritten on each run, so the manifest URLs must survive it."""
        uploader = make_uploader(src_doc={KGX_METADATA_FIELD: {"release": {"version": "2026_07_20"}}})

        with mock.patch.object(kg_uploader_module.requests, "get") as fetch:
            first = uploader.generate_doc_src_master()
            uploader.src_doc = {KGX_METADATA_FIELD: {"release": {"version": "2026_07_21"}}}
            second = uploader.generate_doc_src_master()

        self.assertEqual(first["src_meta"]["release"], {"version": "2026_07_20"})
        self.assertEqual(second["src_meta"]["release"], {"version": "2026_07_21"})
        self.assertEqual([call.args[0] for call in fetch.call_args_list], [GRAPH_URL, GRAPH_URL])

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

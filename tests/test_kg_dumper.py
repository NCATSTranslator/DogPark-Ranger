import logging
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


class FakeLastModifiedHTTPDumper:
    def remote_is_better(self, remotefile, localfile):
        self.sdk_calls.append((remotefile, localfile))
        return self.sdk_result

    def post_dump(self, *args, **kwargs):
        self.base_post_dump_calls = getattr(self, "base_post_dump_calls", 0) + 1


fake_dumper_module = types.ModuleType("biothings.hub.dataload.dumper")
fake_dumper_module.LastModifiedHTTPDumper = FakeLastModifiedHTTPDumper
with mock.patch.dict(sys.modules, {"biothings.hub.dataload.dumper": fake_dumper_module}):
    from hub.dataload.kgDumper import KGX_METADATA_FIELD, KgDumper


GRAPH_URL = "https://example.org/graph-metadata.json"
RELEASE_URL = "https://example.org/latest-release.json"


class FakeResponse:
    def __init__(self, payload=None, ok=True, headers=None):
        self.payload = payload
        self.ok = ok
        self.headers = headers or {}

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeClient:
    def __init__(self, payloads, head_responses=None):
        self.payloads = payloads
        self.head_responses = head_responses or {}
        self.urls = []
        self.head_urls = []

    def get(self, url):
        self.urls.append(url)
        response = self.payloads[url]
        if isinstance(response, Exception):
            raise response
        return response if isinstance(response, FakeResponse) else FakeResponse(response)

    def head(self, url, allow_redirects=True):
        self.head_urls.append(url)
        return self.head_responses[url]


def make_release_dumper(src_meta, payloads=None, head_responses=None, current_release=None, src_urls=None):
    """A stand-in for a generated KgDumper subclass at release-check time."""

    class Dumper(KgDumper):
        __metadata__ = {"src_meta": src_meta}
        logger = logging.getLogger("test")
        src_name = "example"
        SRC_URLS = src_urls or []

    dumper = Dumper()
    dumper.client = FakeClient(payloads or {}, head_responses)
    dumper.current_release = current_release
    dumper.src_doc = {}
    return dumper


class FakeDumper:
    __metadata__ = {"src_meta": {"release": RELEASE_URL}}
    logger = logging.getLogger("test")
    src_name = "example"
    release = "2026_07_20"
    current_release = "2026_07_20"
    _release_check_failed = False


def make_capture_dumper(src_meta, payloads, src_doc=None):
    """A stand-in for a generated KgDumper subclass at post_dump time."""

    class Dumper(KgDumper):
        __metadata__ = {"src_meta": src_meta}
        logger = logging.getLogger("test")
        src_name = "example"
        release = "2026_07_21"

    dumper = Dumper()
    dumper.client = FakeClient(payloads)
    dumper.src_doc = dict(src_doc or {})
    return dumper


class KgDumperTest(unittest.TestCase):
    def test_skips_existing_archive_for_unchanged_release(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory, "example.tar.zst")
            archive.touch()

            self.assertFalse(KgDumper.remote_is_better(FakeDumper(), "https://example.org/data", archive))

    def test_downloads_existing_archive_for_changed_release(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory, "example.tar.zst")
            archive.touch()
            dumper = FakeDumper()
            dumper.release = "2026_07_21"

            self.assertTrue(KgDumper.remote_is_better(dumper, "https://example.org/data", archive))

    def test_downloads_when_local_archive_is_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            missing_archive = Path(directory, "missing-example.tar.zst")
            self.assertTrue(KgDumper.remote_is_better(FakeDumper(), "https://example.org/data", missing_archive))

    def test_skips_when_release_check_failed(self):
        dumper = FakeDumper()
        dumper._release_check_failed = True

        self.assertFalse(KgDumper.remote_is_better(dumper, "https://example.org/data", None))

    def test_preserves_sdk_timestamp_check_without_release_metadata(self):
        class VersionlessDumper(KgDumper):
            __metadata__ = {"src_meta": {}}
            _release_check_failed = False
            sdk_result = True

        dumper = VersionlessDumper()
        dumper.sdk_calls = []

        self.assertTrue(dumper.remote_is_better("https://example.org/data", "/tmp/example.tar.zst"))
        self.assertEqual(dumper.sdk_calls, [("https://example.org/data", "/tmp/example.tar.zst")])


class KgxMetadataCaptureTest(unittest.TestCase):
    def test_captures_declared_metadata_documents(self):
        dumper = make_capture_dumper(
            {"graph": GRAPH_URL, "release": RELEASE_URL},
            {
                GRAPH_URL: {"nodes": 12},
                RELEASE_URL: {"version": "2026_07_21"},
            },
        )

        dumper.capture_kgx_metadata()

        self.assertEqual(
            dumper.src_doc[KGX_METADATA_FIELD],
            {"graph": {"nodes": 12}, "release": {"version": "2026_07_21"}},
        )
        self.assertEqual(sorted(dumper.client.urls), sorted([GRAPH_URL, RELEASE_URL]))

    def test_uses_inlined_manifest_metadata_without_fetching(self):
        dumper = make_capture_dumper({"release": {"version": "2026_07_21"}}, {})

        dumper.capture_kgx_metadata()

        self.assertEqual(dumper.src_doc[KGX_METADATA_FIELD], {"release": {"version": "2026_07_21"}})
        self.assertEqual(dumper.client.urls, [])

    def test_ignores_metadata_that_is_not_a_url_or_document(self):
        dumper = make_capture_dumper({"release": RELEASE_URL, "license": "CC0"}, {RELEASE_URL: {"version": "1"}})

        dumper.capture_kgx_metadata()

        self.assertEqual(dumper.src_doc[KGX_METADATA_FIELD], {"release": {"version": "1"}})

    def test_writes_nothing_when_no_metadata_is_declared(self):
        dumper = make_capture_dumper({"license": "CC0"}, {})

        dumper.capture_kgx_metadata()

        self.assertNotIn(KGX_METADATA_FIELD, dumper.src_doc)

    def test_retains_previous_document_when_fetch_raises(self):
        previous = {"release": {"version": "2026_07_20"}}
        dumper = make_capture_dumper(
            {"release": RELEASE_URL},
            {RELEASE_URL: ConnectionError("boom")},
            src_doc={KGX_METADATA_FIELD: previous},
        )

        dumper.capture_kgx_metadata()

        self.assertEqual(dumper.src_doc[KGX_METADATA_FIELD], previous)

    def test_retains_previous_document_on_error_response(self):
        previous = {"release": {"version": "2026_07_20"}}
        dumper = make_capture_dumper(
            {"release": RELEASE_URL},
            {RELEASE_URL: FakeResponse(payload=None, ok=False)},
            src_doc={KGX_METADATA_FIELD: previous},
        )

        dumper.capture_kgx_metadata()

        self.assertEqual(dumper.src_doc[KGX_METADATA_FIELD], previous)

    def test_retains_previous_document_when_payload_is_not_an_object(self):
        previous = {"release": {"version": "2026_07_20"}}
        dumper = make_capture_dumper(
            {"release": RELEASE_URL},
            {RELEASE_URL: ["not", "an", "object"]},
            src_doc={KGX_METADATA_FIELD: previous},
        )

        dumper.capture_kgx_metadata()

        self.assertEqual(dumper.src_doc[KGX_METADATA_FIELD], previous)

    def test_post_dump_runs_base_hook_then_captures(self):
        class DumpingSource(KgDumper):
            __metadata__ = {"src_meta": {"release": RELEASE_URL}}
            logger = logging.getLogger("test")
            src_name = "example"
            release = "2026_07_21"

        dumper = DumpingSource()
        dumper.client = FakeClient({RELEASE_URL: {"version": "2026_07_21"}})
        dumper.src_doc = {}

        dumper.post_dump()

        self.assertEqual(dumper.base_post_dump_calls, 1)
        self.assertEqual(dumper.src_doc[KGX_METADATA_FIELD], {"release": {"version": "2026_07_21"}})


class KgxReleaseTest(unittest.TestCase):
    """Ported from tests/test_metadata_parser.py when get_kgx_release moved onto KgDumper."""

    def test_gets_release_from_metadata_url(self):
        dumper = make_release_dumper({"graph": GRAPH_URL}, {GRAPH_URL: {"version": "2026_06_21"}})

        self.assertEqual(dumper.get_kgx_release(), "2026_06_21")
        self.assertEqual(dumper.client.urls, [GRAPH_URL])

    def test_gets_release_from_injected_metadata(self):
        dumper = make_release_dumper({"graph": {"version": "2026_06_21"}})

        self.assertEqual(dumper.get_kgx_release(), "2026_06_21")
        self.assertEqual(dumper.client.urls, [])

    def test_gets_release_automat_style(self):
        dumper = make_release_dumper({"release": RELEASE_URL}, {RELEASE_URL: {"release_version": "2026_06_21"}})

        self.assertEqual(dumper.get_kgx_release(), "2026_06_21")

    def test_gets_release_legacy_dingo_style(self):
        dumper = make_release_dumper(
            {"release": RELEASE_URL},
            {RELEASE_URL: {"transform": {"source_version": "2024-11-25", "transform_version": "4a6fdadc"}}},
        )

        self.assertEqual(dumper.get_kgx_release(), "2024-11-25-4a6fdadc")

    def test_refreshes_release_on_every_check(self):
        """set_release() delimits a dump cycle, so each check re-reads the metadata."""
        dumper = make_release_dumper({"release": RELEASE_URL}, {RELEASE_URL: {"release_version": "2026_06_21"}})

        dumper.set_release()
        self.assertEqual(dumper.release, "2026_06_21")

        dumper.client.payloads[RELEASE_URL] = {"release_version": "2026_07_20"}
        dumper.set_release()
        self.assertEqual(dumper.release, "2026_07_20")
        self.assertEqual(dumper.client.urls, [RELEASE_URL, RELEASE_URL])

    def test_prefers_release_over_graph_and_retains_current_when_unavailable(self):
        dumper = make_release_dumper(
            {"release": RELEASE_URL, "graph": GRAPH_URL},
            {
                RELEASE_URL: FakeResponse(ok=False),
                GRAPH_URL: {"version": "different-version-format"},
            },
            current_release="2026_06_21",
        )

        self.assertEqual(dumper.get_kgx_release(), "2026_06_21")
        self.assertTrue(dumper._release_check_failed)
        self.assertEqual(dumper.client.urls, [RELEASE_URL])

    def test_fails_initial_check_when_metadata_is_unavailable(self):
        dumper = make_release_dumper(
            {"release": RELEASE_URL},
            {RELEASE_URL: FakeResponse(ok=False)},
            current_release=None,
        )

        with self.assertRaisesRegex(ValueError, "Unable to read KGX release metadata"):
            dumper.get_kgx_release()

    def test_uses_last_modified_only_without_release_or_graph_metadata(self):
        data_url = "https://example.org/data.tar.zst"
        dumper = make_release_dumper(
            {},
            head_responses={data_url: FakeResponse(headers={"Last-Modified": "Tue, 21 Jul 2026 12:00:00 GMT"})},
            src_urls=[data_url],
        )

        self.assertEqual(dumper.get_kgx_release(), "2026-07-21")
        self.assertEqual(dumper.client.head_urls, [data_url])

    def test_release_check_and_capture_share_one_fetch(self):
        """The whole point of folding the release logic in: resolve each document once."""
        dumper = make_release_dumper(
            {"release": RELEASE_URL, "graph": GRAPH_URL},
            {
                RELEASE_URL: {"release_version": "2026_06_21"},
                GRAPH_URL: {"nodes": 12},
            },
        )

        dumper.set_release()
        dumper.capture_kgx_metadata()

        self.assertEqual(dumper.release, "2026_06_21")
        # release fetched once for the check and reused by capture; graph only by capture
        self.assertEqual(sorted(dumper.client.urls), sorted([GRAPH_URL, RELEASE_URL]))
        self.assertEqual(
            dumper.src_doc[KGX_METADATA_FIELD],
            {"graph": {"nodes": 12}, "release": {"release_version": "2026_06_21"}},
        )


if __name__ == "__main__":
    unittest.main()

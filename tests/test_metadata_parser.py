import logging
import unittest

from hub.dataload.metadata_parser import get_kgx_release


class FakeResponse:
    def __init__(self, payload=None, ok=True):
        self.payload = payload
        self.ok = ok

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
        return response if isinstance(response, FakeResponse) else FakeResponse(response)

    def head(self, url, allow_redirects=True):
        self.head_urls.append(url)
        return self.head_responses[url]


class MetadataParserTest(unittest.TestCase):
    def test_gets_release_from_metadata_url(self):
        class Dumper:
            __metadata__ = {
                "src_meta": {
                    "graph": "https://example.org/graph-metadata.json",
                }
            }
            client = FakeClient(
                {
                    "https://example.org/graph-metadata.json": {
                        "version": "2026_06_21",
                    },
                }
            )
            logger = logging.getLogger("test")

        self.assertEqual(get_kgx_release(Dumper()), "2026_06_21")
        self.assertEqual(Dumper.client.urls, ["https://example.org/graph-metadata.json"])
        self.assertNotIn("generated_version", Dumper.__metadata__["src_meta"])

    def test_gets_release_from_injected_metadata(self):
        class Dumper:
            __metadata__ = {
                "src_meta": {
                    "graph": {
                        "version": "2026_06_21",
                    },
                }
            }
            client = FakeClient({})
            logger = logging.getLogger("test")

        self.assertEqual(get_kgx_release(Dumper()), "2026_06_21")
        self.assertEqual(Dumper.client.urls, [])
        self.assertNotIn("generated_version", Dumper.__metadata__["src_meta"])

    def test_refreshes_release_on_every_check(self):
        metadata_url = "https://example.org/latest-release.json"

        class Dumper:
            __metadata__ = {"src_meta": {"release": metadata_url}}
            client = FakeClient({metadata_url: {"release_version": "2026_06_21"}})
            logger = logging.getLogger("test")

        dumper = Dumper()
        self.assertEqual(get_kgx_release(dumper), "2026_06_21")

        Dumper.client.payloads[metadata_url] = {"release_version": "2026_07_20"}
        self.assertEqual(get_kgx_release(dumper), "2026_07_20")
        self.assertEqual(Dumper.client.urls, [metadata_url, metadata_url])

    def test_retains_current_release_when_metadata_is_temporarily_unavailable(self):
        metadata_url = "https://example.org/latest-release.json"
        graph_url = "https://example.org/graph-metadata.json"

        class Dumper:
            __metadata__ = {"src_meta": {"release": metadata_url, "graph": graph_url}}
            client = FakeClient(
                {
                    metadata_url: FakeResponse(ok=False),
                    graph_url: {"version": "different-version-format"},
                }
            )
            current_release = "2026_06_21"
            logger = logging.getLogger("test")

        dumper = Dumper()
        self.assertEqual(get_kgx_release(dumper), "2026_06_21")
        self.assertTrue(dumper._release_check_failed)
        self.assertEqual(Dumper.client.urls, [metadata_url])

    def test_fails_initial_check_when_metadata_is_unavailable(self):
        metadata_url = "https://example.org/latest-release.json"

        class Dumper:
            __metadata__ = {"src_meta": {"release": metadata_url}}
            client = FakeClient({metadata_url: FakeResponse(ok=False)})
            current_release = None
            logger = logging.getLogger("test")

        with self.assertRaisesRegex(ValueError, "Unable to fetch KGX release metadata"):
            get_kgx_release(Dumper())

    def test_uses_last_modified_only_without_release_or_graph_metadata(self):
        data_url = "https://example.org/data.tar.zst"

        class Dumper:
            __metadata__ = {"src_meta": {}}
            SRC_URLS = [data_url]
            client = FakeClient(
                {},
                {
                    data_url: FakeResponse(),
                },
            )
            client.head_responses[data_url].headers = {
                "Last-Modified": "Tue, 21 Jul 2026 12:00:00 GMT"
            }
            current_release = None
            logger = logging.getLogger("test")

        self.assertEqual(get_kgx_release(Dumper()), "2026-07-21")
        self.assertEqual(Dumper.client.head_urls, [data_url])


if __name__ == "__main__":
    unittest.main()

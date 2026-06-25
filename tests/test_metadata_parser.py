import logging
import unittest

from hub.dataload.metadata_parser import get_kgx_release


class FakeResponse:
    ok = True

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, payloads):
        self.payloads = payloads
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return FakeResponse(self.payloads[url])


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
        self.assertEqual(
            Dumper.__metadata__["src_meta"]["generated_version"],
            "2026_06_21",
        )

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
        self.assertEqual(
            Dumper.__metadata__["src_meta"]["generated_version"],
            "2026_06_21",
        )


if __name__ == "__main__":
    unittest.main()

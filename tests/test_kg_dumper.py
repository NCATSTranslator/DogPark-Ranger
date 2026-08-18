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


fake_dumper_module = types.ModuleType("biothings.hub.dataload.dumper")
fake_dumper_module.LastModifiedHTTPDumper = FakeLastModifiedHTTPDumper
with mock.patch.dict(sys.modules, {"biothings.hub.dataload.dumper": fake_dumper_module}):
    from hub.dataload.kgDumper import KgDumper


class FakeDumper:
    __metadata__ = {"src_meta": {"release": "https://example.org/latest-release.json"}}
    logger = logging.getLogger("test")
    src_name = "example"
    release = "2026_07_20"
    current_release = "2026_07_20"
    _release_check_failed = False


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


if __name__ == "__main__":
    unittest.main()

import copy
import unittest

from hub.snapshot_config import TEST_ES_URL, add_test_snapshots


class SnapshotConfigTests(unittest.TestCase):
    def configs(self):
        indices = {"env": {"itrb-ci": {"host": "http://ci.invalid:9200"}}}
        snapshots = {"env": {"ci_snapshots": {
            "indexer": {"name": "itrb-ci", "args": {"hosts": "http://ci.invalid:9200"}},
            "name": "ci_snapshots",
            "cloud": {"type": "aws", "access_key": "test-key", "secret_key": "test-secret"},
            "repository": {"name": "dogpark_repository", "type": "s3",
                           "settings": {"bucket": "example-bucket", "base_path": "snapshots", "readonly": False}},
            "monitor_delay": 15,
        }}}
        return indices, snapshots

    def test_reuses_repository_but_removes_expanded_ci_connection(self):
        indices, snapshots = self.configs()
        original = copy.deepcopy((indices, snapshots))
        new_indices, new_snapshots = add_test_snapshots(indices, snapshots)
        target = new_snapshots["env"]["test_snapshots"]
        self.assertEqual(new_indices["env"]["itrb-test"]["host"], TEST_ES_URL)
        self.assertEqual(target["indexer"], "itrb-test")
        self.assertNotIn("name", target)
        self.assertTrue(target["repository"]["settings"]["readonly"])
        self.assertEqual(target["repository"]["settings"]["bucket"], "example-bucket")
        self.assertEqual(target["cloud"], snapshots["env"]["ci_snapshots"]["cloud"])
        self.assertEqual((indices, snapshots), original)

    def test_preserves_explicit_test_auth_and_tls_configuration(self):
        indices, snapshots = self.configs()
        configured = {"host": "https://test.invalid:9243", "indexer": {
            "args": {"basic_auth": ["user", "password"], "ca_certs": "/certs/ca.pem"}}}
        indices["env"]["itrb-test"] = configured
        new_indices, _ = add_test_snapshots(indices, snapshots)
        self.assertEqual(new_indices["env"]["itrb-test"], configured)
        self.assertIsNot(new_indices["env"]["itrb-test"], configured)

    def test_missing_template_does_not_create_partial_settings(self):
        with self.assertRaisesRegex(ValueError, "configured.*snapshot repository"):
            add_test_snapshots({"env": {}}, {})

    def test_refuses_overwriting_existing_or_reusing_ci_environment(self):
        indices, snapshots = self.configs()
        snapshots["env"]["test_snapshots"] = {"indexer": "already-configured"}
        with self.assertRaisesRegex(ValueError, "already exists"):
            add_test_snapshots(indices, snapshots)
        snapshots["env"].pop("test_snapshots")
        with self.assertRaisesRegex(ValueError, "indexer environments must be different"):
            add_test_snapshots(indices, snapshots, indexer_env="itrb-ci")

import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

PROJECT_DIR = Path(__file__).resolve().parents[1]
HOOKS_DIR = PROJECT_DIR / "hooks"
sys.path.insert(0, str(PROJECT_DIR))
from hooks.dogpark_hook_utils import builds


def document(name="source", *, environment="builder", jobs=None):
    return {
        "_id": "build-1",
        "target_name": "not-the-index",
        "build_config": {"_id": name},
        "started_at": "2026-10-29T12:00:00Z",
        "index": {"concrete-index": {"environment": environment}},
        "jobs": jobs if jobs is not None else [{"status": "success"}],
    }


class BuildUtilsTests(unittest.TestCase):
    def test_imports_in_fresh_hub_namespace_without_biothings_or_file_global(self):
        # Exercise the deployment contract in a fresh process, without relying
        # on this test runner's module cache or on the current working directory.
        script = """
import pathlib
import sys
hooks = pathlib.Path(sys.argv[1])
assert 'biothings' not in sys.modules
namespace = {'__name__': 'hub_shell', 'list': lambda: None}
exec('from hooks.dogpark_hook_utils import builds', namespace)
assert 'biothings' not in sys.modules
assert 'elasticsearch' not in sys.modules
utils = namespace['builds']
utils.DOGPARK_RELEASE_DATASETS = ('missing',)
assert utils.dogpark_select_latest_builds([])['pending'][0]['config'] == 'missing'
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(HOOKS_DIR)],
            cwd=HOOKS_DIR.parent / "tests",
            env={
                **os.environ,
                "PYTHONPATH": str(PROJECT_DIR),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_load_uses_supplied_collection_without_hub_initialization(self):
        collection = Mock()
        collection.find.return_value = [document()]
        result = builds.load_build_documents(collection)
        self.assertEqual(result[0]["_id"], "build-1")
        collection.find.assert_called_once()
        self.assertEqual(collection.find.call_args.args[0], {})
        self.assertEqual(collection.find.call_args.args[1]["index"], 1)

    def test_selection_uses_explicit_datasets_without_release_alias_policy(self):
        selected = builds.select_latest_builds(
            [document("ubergraph_nodes"), document("other")], ("ubergraph_nodes",)
        )
        self.assertEqual(
            [item["config"] for item in selected["selected"]], ["ubergraph_nodes"]
        )
        self.assertEqual(selected["pending"], [])

    def test_config_name_normalization_is_explicit(self):
        record = document(" source ")
        exact = builds.select_latest_builds([record], ["source"])
        normalized = builds.select_latest_builds(
            [record], ["source"], normalize_config_names=True
        )
        self.assertEqual(exact["pending"][0]["reason"], "missing_build_config")
        self.assertEqual(normalized["selected"][0]["build_id"], record["_id"])

    def test_malformed_history_does_not_fall_back_to_sortable_build(self):
        malformed = {**document(), "_id": "unknown-age", "started_at": "invalid"}
        result = builds.select_latest_builds([document(), malformed], ["source"])
        self.assertEqual(result["selected"], [])
        self.assertEqual(result["pending"][0]["reason"], "malformed_build_history")

    def test_resolution_uses_registration_and_selected_environment(self):
        record = document()
        record["index"]["server-index"] = {"environment": "server"}
        probe = Mock(return_value=True)
        self.assertEqual(
            builds.resolve_build_index(record, "builder", index_exists=probe),
            ("concrete-index", None),
        )
        probe.assert_called_once_with("concrete-index")
        self.assertEqual(
            builds.resolve_build_index(record, "server"), ("server-index", None)
        )

    def test_legacy_environment_fallback_can_be_disabled(self):
        record = document(environment=None)
        self.assertEqual(
            builds.resolve_build_index(record, "builder"), ("concrete-index", None)
        )
        name, issue = builds.resolve_build_index(
            record, "builder", allow_legacy_environment=False
        )
        self.assertIsNone(name)
        self.assertEqual(issue["reason"], "index_not_registered_for_environment")

    def test_missing_exact_environment_index_does_not_fall_back_to_legacy(self):
        record = document()
        record["index"]["legacy-index"] = {}
        probe = Mock(side_effect=lambda name: name == "legacy-index")
        name, issue = builds.resolve_build_index(record, "builder", index_exists=probe)
        self.assertIsNone(name)
        self.assertEqual(issue["reason"], "index_not_found")
        probe.assert_called_once_with("concrete-index")

    def test_multiple_registrations_require_an_unambiguous_existence_check(self):
        record = document()
        record["index"]["another"] = {"environment": "builder"}
        self.assertEqual(
            builds.resolve_build_index(record, "builder")[1]["reason"],
            "ambiguous_target_indices",
        )
        self.assertEqual(
            builds.resolve_build_index(
                record, "builder", index_exists=lambda name: name == "another"
            ),
            ("another", None),
        )

    def test_probe_failure_is_not_reported_as_missing_index(self):
        with self.assertRaisesRegex(RuntimeError, "unreachable"):
            builds.resolve_build_index(
                document(),
                "builder",
                index_exists=Mock(side_effect=RuntimeError("unreachable")),
            )

    def test_legacy_readiness_is_explicit_and_can_be_disabled(self):
        record = document(
            jobs=[
                {
                    "step": "index",
                    "status": "success",
                    "logfile": "/logs/index_concrete-index.log",
                },
                {
                    "step": "post-index",
                    "status": "cancelled",
                    "logfile": "/logs/index_concrete-index.log",
                },
            ]
        )
        accepted = builds.check_build_readiness(record, "concrete-index")
        self.assertTrue(accepted["ready"])
        self.assertEqual(
            accepted["warning"], "cancelled_post_index_after_successful_index"
        )
        self.assertFalse(builds.check_build_readiness(record, "another-index")["ready"])
        self.assertFalse(
            builds.check_build_readiness(
                record, "concrete-index", allow_stale_cancelled_post_index=False
            )["ready"]
        )


class ReadinessTests(unittest.TestCase):
    def test_latest_indexing_attempt_and_environment_control_readiness(self):
        document = {
            "_id": "alliance_build", "build_config": {"name": "alliance"},
            "started_at": "2026-10-29T12:00:00Z",
            "index": {"alliance_build": {"environment": "builder"}},
            "jobs": [{"step": "index", "status": "success"}, {"step": "post-index", "status": "success"}],
        }
        with patch.object(builds, "load_build_documents", return_value=[document]):
            self.assertEqual(builds.get_available_builds("builder"), ["alliance_build"])
            self.assertEqual(builds.get_available_builds("ci"), [])
            document["jobs"].append({"step": "snapshot", "status": "in progress"})
            self.assertEqual(builds.get_available_builds("builder"), ["alliance_build"])
            document["jobs"].append({"step": "pre-index", "status": "in progress"})
            self.assertEqual(builds.get_available_builds("builder"), [])

    def test_newest_unready_build_does_not_fall_back_to_older_one(self):
        old = {
            "_id": "alliance_old", "build_config": {"name": "alliance"},
            "started_at": "2026-10-28T12:00:00Z",
            "index": {"alliance_old": {"environment": "builder"}},
            "jobs": [{"step": "post-index", "status": "success"}],
        }
        new = {**old, "_id": "alliance_new", "started_at": "2026-10-29T12:00:00Z", "index": {}}
        with patch.object(builds, "load_build_documents", return_value=[old, new]):
            self.assertEqual(builds.get_available_builds("builder"), [])


if __name__ == "__main__":
    unittest.main()

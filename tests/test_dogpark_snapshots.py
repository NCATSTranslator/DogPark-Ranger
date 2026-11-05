import asyncio
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from biothings.utils import es as biothings_es
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
from elasticsearch import Elasticsearch

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from hooks import dogpark_snapshots as hook


def response(body):
    return SimpleNamespace(body=body)


def snapshot(name="alliance_build", *, state="SUCCESS", uuid="snapshot-uuid", failed=0):
    return {
        "snapshot": name,
        "uuid": uuid,
        "state": state,
        "indices": [name],
        "shards": {"total": 1, "successful": 1 - failed, "failed": failed},
    }


def release_entry(name="alliance_build"):
    return {
        "build_id": name,
        "build_config": {"name": "alliance"},
        "index_name": name,
        "repository": "builder_alliance",
        "snapshot": name,
        "snapshot_uuid": "snapshot-uuid",
    }


def environment(indexer_env, repository_prefix):
    client = Mock()
    client.indices.get.return_value = response({})
    client.snapshot.get.return_value = response({"snapshots": [snapshot()]})
    client.options.return_value = client
    client.snapshot.restore.return_value = response({
        "snapshot": {"indices": ["alliance_build"], "shards": {"total": 1, "successful": 1, "failed": 0}}
    })
    config = Mock()
    config.format.side_effect = lambda build: SimpleNamespace(
        repo=f"{repository_prefix}_{build['build_config']['name']}", data={"settings": {}}
    )
    return SimpleNamespace(idxenv=indexer_env, client=client, repcfg=config)


class CreateAndReleaseTests(unittest.TestCase):
    def setUp(self):
        self.env = environment("builder", "builder")
        self.manager = Mock(register={"builder_snapshots": self.env})
        self.addCleanup(patch.stopall)
        patch.object(hook, "ssm", self.manager, create=True).start()
        self.select = patch.object(hook, "get_available_builds", return_value=["alliance_build"]).start()
        self.collection = Mock()
        self.collection.find.return_value = [{
            "_id": "alliance_build", "build_config": {"name": "alliance"},
            "index": {"alliance_build": {"environment": "builder"}},
        }]
        self.get_collection = patch.object(hook, "get_src_build_collection", return_value=self.collection).start()
        self.snapshot_state = patch.object(hook, "_snapshot_state", return_value="MISSING").start()

    def test_create_uses_indexer_env_and_only_existing_indices(self):
        self.select.return_value = ["alliance_build", "missing"]
        self.env.client.indices.get.return_value = response({"alliance_build": {}})
        tasks = hook.create_snapshots("builder_snapshots")
        self.select.assert_called_once_with("builder")
        self.manager.snapshot.assert_called_once_with("builder_snapshots", "alliance_build")
        self.assertEqual(tasks, [self.manager.snapshot.return_value])

    def test_explicit_indices_bypass_default_selection(self):
        self.env.client.indices.get.return_value = response({"explicit_build": {}})
        self.collection.find.return_value = [{
            "_id": "explicit_build", "build_config": {"name": "alliance"},
            "index": {"explicit_build": {"environment": "builder"}},
        }]
        hook.create_snapshots("builder_snapshots", ["explicit_build"])
        self.select.assert_not_called()
        self.manager.snapshot.assert_called_once_with("builder_snapshots", "explicit_build")

    def test_empty_explicit_and_default_batches_do_not_query_es(self):
        self.select.return_value = []
        self.assertEqual(hook.create_snapshots("builder_snapshots", []), [])
        self.assertEqual(hook.create_snapshots("builder_snapshots"), [])
        self.env.client.indices.get.assert_not_called()
        self.manager.snapshot.assert_not_called()

    def test_no_existing_indices_does_not_start_snapshots(self):
        self.assertEqual(hook.create_snapshots("builder_snapshots"), [])
        self.manager.snapshot.assert_not_called()

    def test_create_dry_run_reports_replacement_creation_and_missing_indices(self):
        self.env.client.indices.get.return_value = response({
            "alliance_build": {"aliases": {"builder_alias": {}}}, "bgee_build": {},
        })
        self.collection.find.return_value.append({
            "_id": "bgee_build", "build_config": {"name": "bgee"},
            "index": {"bgee_build": {"environment": "builder"}},
        })
        self.snapshot_state.side_effect = ["SUCCESS", "MISSING"]
        result = hook.create_snapshots("builder_snapshots", ["alliance_build", "bgee_build", "missing"], dry_run=True)
        self.assertEqual([row["action"] for row in result["indices"]], ["replace", "create", "skip"])
        self.assertEqual(result["indices"][0]["aliases"], ["builder_alias"])
        self.assertTrue(result["dry_run"])
        self.manager.snapshot.assert_not_called()
        self.env.client.snapshot.create.assert_not_called()
        self.env.client.snapshot.delete.assert_not_called()

    def test_create_dry_run_checks_default_selection(self):
        self.env.client.indices.get.return_value = response({"alliance_build": {}})
        result = hook.create_snapshots("builder_snapshots", dry_run=True)
        self.select.assert_called_once_with("builder")
        self.assertEqual(result["indices"][0]["action"], "create")
        self.manager.snapshot.assert_not_called()

    def test_create_blocks_unregistered_indices_and_running_snapshots(self):
        self.env.client.indices.get.return_value = response({"alliance_build": {}, "unregistered": {}})
        self.snapshot_state.return_value = "IN_PROGRESS"
        result = hook.create_snapshots("builder_snapshots", ["alliance_build", "unregistered"], dry_run=True)
        self.assertEqual([row["action"] for row in result["indices"]], ["blocked", "blocked"])
        with self.assertRaisesRegex(ValueError, "Cannot create snapshots"):
            hook.create_snapshots("builder_snapshots", ["alliance_build", "unregistered"])
        self.manager.snapshot.assert_not_called()

    def test_create_rejects_wrong_registration_and_readonly_repository(self):
        self.env.client.indices.get.return_value = response({"alliance_build": {}})
        build = self.collection.find.return_value[0]
        build["index"]["alliance_build"]["environment"] = "ci"
        result = hook.create_snapshots("builder_snapshots", dry_run=True)
        self.assertIn("registration", result["indices"][0]["reason"])
        build["index"]["alliance_build"]["environment"] = "builder"
        self.env.repcfg.format.side_effect = lambda build: SimpleNamespace(
            repo="readonly_repo", data={"settings": {"readonly": True}}
        )
        result = hook.create_snapshots("builder_snapshots", dry_run=True)
        self.assertIn("read-only", result["indices"][0]["reason"])
        with self.assertRaisesRegex(ValueError, "read-only"):
            hook.create_snapshots("builder_snapshots")
        self.snapshot_state.assert_not_called()
        self.manager.snapshot.assert_not_called()

    def test_create_empty_dry_run_and_invalid_names_do_not_query_es(self):
        result = hook.create_snapshots("builder_snapshots", [], dry_run=True)
        self.assertEqual(result["indices"], [])
        for names in ("alliance_build", ["*"], ["alliance_build"] * 2):
            with self.subTest(names=names), self.assertRaises((TypeError, ValueError)):
                hook.create_snapshots("builder_snapshots", names, dry_run=True)
        self.env.client.indices.get.assert_not_called()

    def test_create_rejects_alias_resolution(self):
        self.env.client.indices.get.return_value = response({"different_concrete_index": {}})
        with self.assertRaisesRegex(ValueError, "concrete index names"):
            hook.create_snapshots("builder_snapshots", ["alliance_build"])
        self.manager.snapshot.assert_not_called()

    def test_unknown_environment_and_es_errors_propagate(self):
        with self.assertRaises(KeyError):
            hook.create_snapshots("unknown")
        self.env.client.indices.get.side_effect = RuntimeError("ES unavailable")
        with self.assertRaisesRegex(RuntimeError, "ES unavailable"):
            hook.create_snapshots("builder_snapshots")
        self.manager.snapshot.assert_not_called()

    def test_release_is_reusable_and_includes_snapshot_identity(self):
        release = hook.get_snapshots_release("builder_snapshots")
        self.assertEqual(release, [release_entry()])
        self.assertEqual(json.loads(json.dumps(release)), release)
        self.select.assert_called_once_with("builder")
        self.collection.find.assert_called_once_with(
            {"_id": {"$in": ["alliance_build"]}}, {"_id": 1, "build_config": 1}
        )
        self.env.client.snapshot.get.assert_called_once_with(
            repository="builder_alliance", snapshot="alliance_build", ignore_unavailable=True
        )
        self.manager.snapshot.assert_not_called()
        self.env.client.snapshot.restore.assert_not_called()

    def test_release_deduplicates_explicit_names(self):
        self.assertEqual(hook.get_snapshots_release("builder_snapshots", ["alliance_build"] * 2), [release_entry()])
        self.select.assert_not_called()
        self.env.client.snapshot.get.assert_called_once()

    def test_release_skips_missing_unfinished_and_failed_snapshots(self):
        for snapshots in ([], [snapshot(state="IN_PROGRESS")], [snapshot(state="PARTIAL")],
                          [snapshot(state="FAILED")], [snapshot(failed=1)]):
            with self.subTest(snapshots=snapshots):
                self.env.client.snapshot.get.return_value = response({"snapshots": snapshots})
                self.assertEqual(hook.get_snapshots_release("builder_snapshots"), [])

    def test_release_keeps_completed_part_of_batch(self):
        self.collection.find.return_value.append({"_id": "missing", "build_config": {"name": "bgee"}})
        self.env.client.snapshot.get.side_effect = [response({"snapshots": [snapshot()]}), response({"snapshots": []})]
        release = hook.get_snapshots_release("builder_snapshots", ["alliance_build", "missing"])
        self.assertEqual(release, [release_entry()])

    def test_release_empty_selection_does_not_read_db_or_es(self):
        self.select.return_value = []
        self.assertEqual(hook.get_snapshots_release("builder_snapshots"), [])
        self.get_collection.assert_not_called()
        self.env.client.snapshot.get.assert_not_called()

    def test_missing_build_metadata_and_string_input_are_rejected(self):
        self.collection.find.return_value = []
        with self.assertRaisesRegex(ValueError, "No build metadata"):
            hook.get_snapshots_release("builder_snapshots")
        with self.assertRaises(TypeError):
            hook.get_snapshots_release("builder_snapshots", "alliance_build")
        self.env.client.snapshot.get.assert_not_called()

    def test_wrong_snapshot_contents_and_repository_errors_are_not_hidden(self):
        info = snapshot()
        info["indices"] = ["different_index"]
        self.env.client.snapshot.get.return_value = response({"snapshots": [info]})
        with self.assertRaisesRegex(ValueError, "does not contain exactly"):
            hook.get_snapshots_release("builder_snapshots")
        self.env.client.snapshot.get.side_effect = RuntimeError("Repository unavailable")
        with self.assertRaisesRegex(RuntimeError, "Repository unavailable"):
            hook.get_snapshots_release("builder_snapshots")


class InstallTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.origin = environment("builder", "builder")
        self.target = environment("ci", "ci")
        self.target.repcfg = SimpleNamespace(
            format=Mock(return_value=SimpleNamespace(repo="ci_repository"))
        )
        self.target.wtime = 0
        self.target.pinfo = SimpleNamespace(get_pinfo=Mock(return_value={"source": "ci_snapshots"}))

        async def defer(pinfo, func):
            future = asyncio.get_running_loop().create_future()
            try:
                future.set_result(func())
            except Exception as exc:
                future.set_exception(exc)
            return future

        self.target.job_manager = SimpleNamespace(
            loop=asyncio.get_running_loop(), defer_to_thread=AsyncMock(side_effect=defer)
        )
        self.manager = Mock(
            register={"builder_snapshots": self.origin, "ci_snapshots": self.target},
            index_manager={"ci": {"args": {"hosts": "http://ci.invalid:9200", "request_timeout": 25}}},
        )
        self.addCleanup(patch.stopall)
        patch.object(hook, "ssm", self.manager, create=True).start()
        self.select = patch.object(hook, "get_available_builds", side_effect=AssertionError("Must use explicit names")).start()
        self.get_collection = patch.object(hook, "get_src_build_collection", side_effect=AssertionError("No target DB lookup")).start()
        self.load_builds = patch.object(hook, "load_build_documents", side_effect=AssertionError("Explicit lists need no discovery")).start()
        self.warning = patch.object(hook.logger, "warning").start()
        self.indexers = {}

        def make_indexer(**kwargs):
            indexer = Mock()
            indexer.restore.return_value = {"accepted": True}
            indexer.get_restore_status.return_value = {"status": "DONE", "progress": "100%"}
            self.indexers[kwargs["index"]] = indexer
            return indexer

        self.factory = patch.object(hook, "ESIndexer", side_effect=make_indexer).start()

    def discover(self, documents):
        self.load_builds.side_effect = None
        self.load_builds.return_value = documents

    def build(self, name, source="alliance", started_at="2026-11-03T12:00:00Z"):
        return {"_id": name, "build_config": {"name": source}, "started_at": started_at}

    async def test_auto_discovers_newest_builds_in_target_repository_without_index_registration(self):
        self.discover([
            self.build("alliance_old", started_at="2026-11-02T12:00:00Z"),
            self.build("alliance_new"),
            self.build("bgee_new", source="bgee"),
            self.build("unmanaged_new", source="unmanaged"),
        ])
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [snapshot(kwargs["snapshot"])]
        })
        result = hook.install_snapshots("ci_snapshots", dry_run=True)
        self.assertEqual(
            [(row["index_name"], row["action"], row["repository"]) for row in result["indices"]],
            [("alliance_new", "restore", "ci_repository"), ("bgee_new", "restore", "ci_repository")],
        )
        self.load_builds.assert_called_once_with()
        self.select.assert_not_called()
        self.get_collection.assert_not_called()
        self.assertEqual(self.origin.client.mock_calls, [])
        self.factory.assert_not_called()
        self.target.job_manager.defer_to_thread.assert_not_awaited()

    async def test_auto_skips_missing_newest_snapshot_without_falling_back(self):
        self.discover([
            self.build("alliance_old", started_at="2026-11-02T12:00:00Z"),
            self.build("alliance_new"), self.build("bgee_new", source="bgee"),
        ])
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [] if kwargs["snapshot"] == "alliance_new" else [snapshot(kwargs["snapshot"])]
        })
        preview = hook.install_snapshots("ci_snapshots", dry_run=True)
        self.assertEqual([row["action"] for row in preview["indices"]], ["skip", "restore"])
        self.assertIn("missing", preview["indices"][0]["reason"])
        results = await asyncio.gather(*hook.install_snapshots("ci_snapshots"))
        self.assertEqual([item["index_name"] for item in results], ["bgee_new"])
        self.assertEqual(set(self.indexers), {"bgee_new"})
        self.assertEqual(self.target.client.indices.get.call_args.kwargs["index"], ["bgee_new"])
        self.assertNotIn("alliance_old", [call.kwargs["snapshot"] for call in self.target.client.snapshot.get.call_args_list])
        self.warning.assert_any_call("Skipping %s: %s", "alliance_new", "snapshot is missing or incomplete")

    async def test_auto_with_only_missing_or_incomplete_snapshots_does_not_query_destination_indices(self):
        self.discover([self.build("alliance_build")])
        for snapshots in ([], [snapshot(state="IN_PROGRESS")], [snapshot(state="PARTIAL")],
                          [snapshot(state="FAILED")], [snapshot(failed=1)]):
            with self.subTest(snapshots=snapshots):
                self.target.client.snapshot.get.return_value = response({"snapshots": snapshots})
                result = hook.install_snapshots("ci_snapshots", dry_run=True)
                self.assertEqual(result["indices"][0]["action"], "skip")
                self.assertEqual(hook.install_snapshots("ci_snapshots"), [])
        self.target.client.indices.get.assert_not_called()
        self.factory.assert_not_called()
        self.target.job_manager.defer_to_thread.assert_not_awaited()

    async def test_auto_keeps_existing_destination_indices_blocked(self):
        self.discover([self.build("alliance_build"), self.build("bgee_new", source="bgee")])
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [snapshot(kwargs["snapshot"])]
        })
        self.target.client.indices.get.return_value = response({"alliance_build": {"aliases": {}}})
        result = hook.install_snapshots("ci_snapshots", dry_run=True)
        self.assertEqual([row["action"] for row in result["indices"]], ["blocked", "restore"])
        with self.assertRaisesRegex(ValueError, "already exists"):
            hook.install_snapshots("ci_snapshots")
        self.factory.assert_not_called()
        self.target.job_manager.defer_to_thread.assert_not_awaited()

    async def test_auto_blocks_wrong_snapshot_contents(self):
        self.discover([self.build("alliance_build")])
        info = snapshot()
        info["indices"] = ["different_index"]
        self.target.client.snapshot.get.return_value = response({"snapshots": [info]})
        result = hook.install_snapshots("ci_snapshots", dry_run=True)
        self.assertEqual(result["indices"][0]["action"], "blocked")
        with self.assertRaisesRegex(ValueError, "does not contain exactly"):
            hook.install_snapshots("ci_snapshots")
        self.factory.assert_not_called()

    async def test_auto_empty_or_ambiguous_build_selection_does_not_query_es(self):
        for documents in ([], [self.build("alliance_a"), self.build("alliance_b")]):
            with self.subTest(documents=documents):
                self.discover(documents)
                self.assertEqual(hook.install_snapshots("ci_snapshots", dry_run=True)["indices"], [])
                self.assertEqual(hook.install_snapshots("ci_snapshots"), [])
        self.assertEqual(self.target.client.mock_calls, [])
        self.factory.assert_not_called()
        self.warning.assert_called()

    async def test_uses_biothings_restore_and_recovery_on_target(self):
        names = ["alliance_build"]
        tasks = hook.install_snapshots("ci_snapshots", names)
        self.assertTrue(all(isinstance(task, asyncio.Task) for task in tasks))
        results = await asyncio.gather(*tasks)
        self.assertEqual(results[0]["status"], "DONE")
        self.assertEqual(results[0]["index_name"], "alliance_build")
        self.factory.assert_called_once_with(
            index="alliance_build", es_host="http://ci.invalid:9200", check_index=False,
            timeout=25, max_retries=0, retry_on_timeout=False,
        )
        indexer = self.indexers["alliance_build"]
        indexer.restore.assert_called_once_with(
            "ci_repository", "alliance_build", index_name="alliance_build", purge=False
        )
        indexer.get_restore_status.assert_called_once_with("alliance_build")
        indexer._es.close.assert_called_once()
        self.assertEqual(self.target.job_manager.defer_to_thread.await_count, 3)
        self.target.client.snapshot.restore.assert_not_called()
        self.assertEqual(self.origin.client.mock_calls, [])
        self.select.assert_not_called()
        self.get_collection.assert_not_called()
        self.load_builds.assert_not_called()
        self.assertEqual(names, ["alliance_build"])

    async def test_empty_index_list_does_nothing(self):
        self.assertEqual(hook.install_snapshots("ci_snapshots", []), [])
        self.assertEqual(hook.install_snapshots("ci_snapshots", [], dry_run=True)["indices"], [])
        self.assertEqual(self.target.client.mock_calls, [])
        self.factory.assert_not_called()
        self.load_builds.assert_not_called()

    async def test_restores_multiple_named_snapshots(self):
        names = ["alliance_build", "bgee_build"]
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [snapshot(kwargs["snapshot"])]
        })
        results = await asyncio.gather(*hook.install_snapshots("ci_snapshots", names))
        self.assertEqual({item["index_name"] for item in results}, set(names))
        for name in names:
            self.indexers[name].restore.assert_called_once_with(
                "ci_repository", name, index_name=name, purge=False
            )
            self.indexers[name]._es.close.assert_called_once()

    async def test_dry_run_returns_ready_names_without_starting_jobs(self):
        result = hook.install_snapshots("ci_snapshots", ["alliance_build"], dry_run=True)
        self.assertTrue(result["dry_run"])
        self.assertTrue(result["restore_aliases"])
        self.assertEqual(result["indices"][0]["action"], "restore")
        self.assertEqual(result["indices"][0]["repository"], "ci_repository")
        self.assertEqual(result["indices"][0]["snapshot_uuid"], "snapshot-uuid")
        self.factory.assert_not_called()
        self.target.job_manager.defer_to_thread.assert_not_awaited()
        self.target.client.snapshot.restore.assert_not_called()
        self.get_collection.assert_not_called()

    async def test_dry_run_reports_all_missing_and_existing_targets(self):
        names = ["alliance_build", "missing", "already_present"]
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [] if kwargs["snapshot"] == "missing" else [snapshot(kwargs["snapshot"])]
        })
        self.target.client.indices.get.return_value = response({"already_present": {"aliases": {}}})
        result = hook.install_snapshots("ci_snapshots", names, dry_run=True)
        self.assertEqual([row["action"] for row in result["indices"]], ["restore", "blocked", "blocked"])
        self.assertIn("missing", result["indices"][1]["reason"])
        self.assertIn("already exists", result["indices"][2]["reason"])
        with self.assertRaisesRegex(ValueError, "Cannot install snapshots"):
            hook.install_snapshots("ci_snapshots", names)
        self.factory.assert_not_called()
        self.target.job_manager.defer_to_thread.assert_not_awaited()

    async def test_missing_snapshot_aborts_whole_batch_before_restoring(self):
        self.target.client.snapshot.get.side_effect = [
            response({"snapshots": [snapshot()]}), response({"snapshots": []}),
        ]
        with self.assertRaisesRegex(ValueError, "missing or incomplete"):
            hook.install_snapshots("ci_snapshots", ["alliance_build", "bgee_build"])
        self.factory.assert_not_called()

    async def test_missing_or_incomplete_target_snapshot_is_rejected(self):
        for snapshots in ([], [snapshot(state="IN_PROGRESS")], [snapshot(state="PARTIAL")],
                          [snapshot(state="FAILED")], [snapshot(failed=1)]):
            with self.subTest(snapshots=snapshots):
                self.target.client.snapshot.get.return_value = response({"snapshots": snapshots})
                with self.assertRaisesRegex(ValueError, "missing or incomplete"):
                    hook.install_snapshots("ci_snapshots", ["alliance_build"])
        self.factory.assert_not_called()

    async def test_existing_indices_and_aliases_are_not_overwritten(self):
        for existing in ({"alliance_build": {"aliases": {}}},
                         {"existing_concrete_index": {"aliases": {"alliance_build": {}}}}):
            with self.subTest(existing=existing):
                self.target.client.indices.get.return_value = response(existing)
                result = hook.install_snapshots("ci_snapshots", ["alliance_build"], dry_run=True)
                self.assertEqual(result["indices"][0]["action"], "blocked")
                with self.assertRaisesRegex(ValueError, "already exists"):
                    hook.install_snapshots("ci_snapshots", ["alliance_build"])
        self.assertEqual(self.target.client.indices.get.call_args.kwargs["expand_wildcards"], "all")
        self.factory.assert_not_called()
        self.target.client.indices.delete.assert_not_called()
        self.target.client.indices.close.assert_not_called()

    async def test_duplicate_and_wildcard_targets_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Duplicate index"):
            hook.install_snapshots("ci_snapshots", ["alliance_build"] * 2)
        for value in ("", "*", "a,b", "_all", None, release_entry()):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "exact index names"):
                hook.install_snapshots("ci_snapshots", [value])
        self.factory.assert_not_called()
        self.target.client.snapshot.get.assert_not_called()

    async def test_string_instead_of_index_list_is_rejected(self):
        with self.assertRaisesRegex(TypeError, "list of index names"):
            hook.install_snapshots("ci_snapshots", "alliance_build")
        self.assertEqual(self.target.client.mock_calls, [])

    async def test_snapshot_containing_other_indices_is_rejected(self):
        info = snapshot()
        info["indices"].append("another_index")
        self.target.client.snapshot.get.return_value = response({"snapshots": [info]})
        result = hook.install_snapshots("ci_snapshots", ["alliance_build"], dry_run=True)
        self.assertEqual(result["indices"][0]["action"], "blocked")
        with self.assertRaisesRegex(ValueError, "does not contain exactly"):
            hook.install_snapshots("ci_snapshots", ["alliance_build"])
        self.factory.assert_not_called()

    async def run_with_indexer(self, indexer):
        self.factory.side_effect = None
        self.factory.return_value = indexer
        return await asyncio.gather(*hook.install_snapshots("ci_snapshots", ["alliance_build"]))

    def indexer(self):
        return Mock(restore=Mock(return_value={"accepted": True}), get_restore_status=Mock())

    async def test_acceptance_is_not_completion_and_progress_is_polled(self):
        indexer = self.indexer()
        indexer.get_restore_status.side_effect = [
            {"status": "INIT", "progress": "0%"},
            {"status": "IN_PROGRESS", "progress": "50%"},
            {"status": "DONE", "progress": "100%"},
        ]
        with patch.object(hook.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = await self.run_with_indexer(indexer)
        self.assertEqual(result[0]["status"], "DONE")
        self.assertEqual(sleep.await_count, 2)
        indexer.restore.assert_called_once()
        self.assertEqual(indexer.get_restore_status.call_count, 3)

    async def test_failed_recovery_fails_task_and_preserves_reason(self):
        indexer = self.indexer()
        indexer.get_restore_status.return_value = {"status": "FAILED", "reason": "disk watermark"}
        with self.assertRaisesRegex(RuntimeError, "disk watermark"):
            await self.run_with_indexer(indexer)
        indexer._es.close.assert_called_once()

    async def test_unaccepted_restore_does_not_start_polling(self):
        indexer = self.indexer()
        indexer.restore.return_value = {"accepted": False}
        with self.assertRaisesRegex(RuntimeError, "not accepted"):
            await self.run_with_indexer(indexer)
        indexer.get_restore_status.assert_not_called()
        indexer._es.close.assert_called_once()

    async def test_restore_request_failure_is_not_retried(self):
        indexer = self.indexer()
        indexer.restore.side_effect = RuntimeError("connection lost")
        with self.assertRaisesRegex(RuntimeError, "connection lost"):
            await self.run_with_indexer(indexer)
        indexer.restore.assert_called_once()
        indexer.get_restore_status.assert_not_called()
        indexer._es.close.assert_called_once()

    async def test_transient_monitor_failure_does_not_resubmit_restore(self):
        indexer = self.indexer()
        indexer.get_restore_status.side_effect = [RuntimeError("temporary"), {"status": "DONE"}]
        await self.run_with_indexer(indexer)
        indexer.restore.assert_called_once()
        self.assertEqual(indexer.get_restore_status.call_count, 2)

    async def test_repeated_monitor_failures_fail_task(self):
        indexer = self.indexer()
        indexer.get_restore_status.side_effect = RuntimeError("ES unavailable")
        with self.assertRaisesRegex(RuntimeError, "ES unavailable"):
            await self.run_with_indexer(indexer)
        self.assertEqual(indexer.get_restore_status.call_count, 3)
        indexer.restore.assert_called_once()
        indexer._es.close.assert_called_once()

    async def test_real_biothings_helpers_build_async_restore_and_check_recovery(self):
        meta = ApiResponseMeta(
            status=200, http_version="1.1", duration=0,
            headers=HttpHeaders({"x-elastic-product": "Elasticsearch"}),
            node=NodeConfig("http", "unused.invalid", 9200),
        )
        posts = []

        def perform_request(method, target, **kwargs):
            if method == "GET":
                if target == "/":
                    return meta, {"version": {"number": "9.3.1"}}
                if target.startswith("/_snapshot/"):
                    return meta, {"snapshots": [snapshot()]}
                if "/_recovery" in target:
                    return meta, {"alliance_build": {"shards": [{"id": 0, "primary": True, "stage": "DONE"}]}}
                if "/_settings" in target:
                    return meta, {"alliance_build": {"settings": {"index": {"number_of_shards": "1"}}}}
                return meta, {}
            self.assertEqual(method, "POST")
            self.assertEqual(target, "/_snapshot/ci_repository/alliance_build/_restore")
            body = kwargs["body"]
            self.assertEqual(body["indices"], "alliance_build")
            self.assertEqual(body["rename_replacement"], "alliance_build")
            self.assertFalse(body["include_global_state"])
            self.assertNotIn("include_aliases", body)
            self.assertNotIn("index_settings", body)
            posts.append(target)
            return meta, {"accepted": True}

        self.factory.side_effect = biothings_es.ESIndexer
        with Elasticsearch("http://unused.invalid:9200") as client:
            self.target.client = client
            with patch.object(biothings_es, "get_es", return_value=client), patch.object(
                client.transport, "perform_request", side_effect=perform_request
            ):
                results = await asyncio.gather(*hook.install_snapshots("ci_snapshots", ["alliance_build"]))
        self.assertEqual(results[0]["status"], "DONE")
        self.assertEqual(len(posts), 1)


if __name__ == "__main__":
    unittest.main()

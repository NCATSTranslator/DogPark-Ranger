import asyncio
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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
        "index_name": name,
        "repository": "dogpark_repository",
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


class CreateTests(unittest.TestCase):
    def setUp(self):
        self.env = environment("builder", "builder")
        self.manager = Mock(register={"builder_snapshots": self.env})
        self.addCleanup(patch.stopall)
        patch.object(hook, "ssm", self.manager, create=True).start()
        self.select = patch.object(hook._snapshot_builds, "get_available_builds", return_value=["alliance_build"]).start()
        self.collection = Mock()
        self.collection.find.return_value = [{
            "_id": "alliance_build", "build_config": {"name": "alliance"},
            "index": {"alliance_build": {"environment": "builder"}},
        }]
        self.get_collection = patch.object(hook._snapshot_builds, "get_src_build_collection", return_value=self.collection).start()
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


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.env = environment("ci", "ci")
        self.env.repcfg.format.side_effect = None
        self.env.repcfg.format.return_value = SimpleNamespace(repo="dogpark_repository")
        self.manager = Mock(register={"ci_snapshots": self.env})
        self.addCleanup(patch.stopall)
        patch.object(hook, "ssm", self.manager, create=True).start()
        for name in ("get_available_builds", "get_src_build_collection", "load_build_documents"):
            patch.object(hook._snapshot_builds, name, side_effect=AssertionError("Inspection must not read Hub builds")).start()

    def test_lists_old_and_new_snapshots_without_build_records_or_live_indices(self):
        names = ["unmanaged_build", "alliance_old", "alliance_new"]
        self.env.client.snapshot.get.return_value = response({"snapshots": [snapshot(name) for name in names]})
        release = hook.get_snapshots_release("ci_snapshots")
        self.assertEqual(release, [release_entry(name) for name in sorted(names)])
        self.assertEqual(json.loads(json.dumps(release)), release)
        self.env.repcfg.format.assert_called_once_with()
        self.env.client.snapshot.get.assert_called_once_with(
            repository="dogpark_repository", snapshot="_all", ignore_unavailable=True
        )
        self.env.client.indices.get.assert_not_called()
        self.manager.snapshot.assert_not_called()
        self.env.client.snapshot.create.assert_not_called()
        self.env.client.snapshot.delete.assert_not_called()
        self.env.client.snapshot.restore.assert_not_called()

    def test_explicit_list_queries_only_requested_snapshots_and_keeps_available_part(self):
        release = hook.get_snapshots_release("ci_snapshots", ["alliance_build", "missing"])
        self.assertEqual(release, [release_entry()])
        self.env.client.snapshot.get.assert_called_once_with(
            repository="dogpark_repository", snapshot=["alliance_build", "missing"], ignore_unavailable=True
        )

    def test_skips_unfinished_failed_and_partial_snapshots(self):
        self.env.client.snapshot.get.return_value = response({"snapshots": [
            snapshot(), snapshot("running", state="IN_PROGRESS"),
            snapshot("partial", state="PARTIAL"), snapshot("failed", state="FAILED"),
            snapshot("failed_shard", failed=1),
        ]})
        self.assertEqual(hook.get_snapshots_release("ci_snapshots"), [release_entry()])

    def test_skips_snapshots_that_do_not_match_the_install_convention(self):
        for indices in ([], ["different_index"], ["alliance_build", "another_index"]):
            for names in (None, ["alliance_build"]):
                with self.subTest(indices=indices, names=names):
                    info = snapshot()
                    info["indices"] = indices
                    self.env.client.snapshot.get.return_value = response({"snapshots": [info]})
                    with self.assertLogs(hook.logger, level="WARNING") as logs:
                        self.assertEqual(hook.get_snapshots_release("ci_snapshots", names), [])
                    self.assertIn("expected exactly the index alliance_build", logs.output[0])

    def test_empty_repository_and_missing_explicit_names_return_empty_list(self):
        self.env.client.snapshot.get.return_value = response({"snapshots": []})
        self.assertEqual(hook.get_snapshots_release("ci_snapshots"), [])
        self.assertEqual(hook.get_snapshots_release("ci_snapshots", ["missing"]), [])

    def test_explicit_empty_list_does_not_query_repository(self):
        self.assertEqual(hook.get_snapshots_release("ci_snapshots", []), [])
        self.env.repcfg.format.assert_not_called()
        self.env.client.snapshot.get.assert_not_called()

    def test_invalid_names_are_rejected_before_querying_repository(self):
        for names in ("alliance_build", {"alliance_build": True}, ["*"], ["a,b"],
                      ["_all"], [""], [None], ["alliance_build"] * 2):
            with self.subTest(names=names), self.assertRaises((TypeError, ValueError)):
                hook.get_snapshots_release("ci_snapshots", names)
        self.env.client.snapshot.get.assert_not_called()

    def test_builder_and_ci_can_inspect_the_same_repository(self):
        builder = environment("builder", "builder")
        builder.repcfg.format.side_effect = None
        builder.repcfg.format.return_value = SimpleNamespace(repo="dogpark_repository")
        self.manager.register["builder_snapshots"] = builder
        self.assertEqual(
            hook.get_snapshots_release("ci_snapshots"), hook.get_snapshots_release("builder_snapshots")
        )
        builder.client.indices.get.assert_not_called()
        self.env.client.indices.get.assert_not_called()

    def test_repository_and_configuration_errors_propagate(self):
        self.env.repcfg.format.side_effect = ValueError("Build-dependent repository template")
        with self.assertRaisesRegex(ValueError, "Build-dependent"):
            hook.get_snapshots_release("ci_snapshots")
        self.env.client.snapshot.get.assert_not_called()
        self.env.repcfg.format.side_effect = None
        self.env.client.snapshot.get.side_effect = RuntimeError("Repository unavailable")
        with self.assertRaisesRegex(RuntimeError, "Repository unavailable"):
            hook.get_snapshots_release("ci_snapshots")

    def test_real_client_queries_repository_once_for_all_or_explicit_snapshots(self):
        meta = ApiResponseMeta(
            status=200, http_version="1.1", duration=0,
            headers=HttpHeaders({"x-elastic-product": "Elasticsearch"}),
            node=NodeConfig("http", "unused.invalid", 9200),
        )
        with Elasticsearch("http://unused.invalid:9200") as client:
            self.env.client = client
            for names, selector in ((None, "_all"), (["alliance_build", "missing"], "alliance_build,missing")):
                with self.subTest(names=names), patch.object(
                    client.transport, "perform_request", return_value=(meta, {"snapshots": [snapshot()]})
                ) as request:
                    self.assertEqual(hook.get_snapshots_release("ci_snapshots", names), [release_entry()])
                    request.assert_called_once()
                    method, target = request.call_args.args
                    self.assertEqual(method, "GET")
                    self.assertEqual(target, f"/_snapshot/dogpark_repository/{selector}?ignore_unavailable=true")


class InstallTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.origin = environment("builder", "builder")
        self.target = environment("ci", "ci")
        self.target.repcfg = SimpleNamespace(
            format=Mock(return_value=SimpleNamespace(repo="ci_repository"))
        )
        self.target.wtime = 0
        self.target.pinfo = SimpleNamespace(get_pinfo=Mock(return_value={"source": "ci_snapshots"}))
        self.target.client.snapshot.restore.return_value = response({"accepted": True})
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [snapshot(kwargs["snapshot"])]
        })
        self.target.client.cluster.health.side_effect = lambda **kwargs: self.health(kwargs["index"])

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
        self.manager = Mock(register={"builder_snapshots": self.origin, "ci_snapshots": self.target})
        self.addCleanup(patch.stopall)
        patch.object(hook, "ssm", self.manager, create=True).start()
        self.select = patch.object(hook._snapshot_builds, "get_available_builds", side_effect=AssertionError("Must use explicit names")).start()
        self.get_collection = patch.object(hook._snapshot_builds, "get_src_build_collection", side_effect=AssertionError("No target DB lookup")).start()
        self.load_builds = patch.object(hook._snapshot_builds, "load_build_documents", side_effect=AssertionError("Explicit lists need no discovery")).start()
        self.warning = patch.object(hook.logger, "warning").start()
        self.expected_staging = {
            "cluster_uuid": "ci-cluster", "alias_profile": "default", "sources": {"alliance_build": "alliance"},
            "staged": {"alliance": {"alliance_old": "old-uuid"}},
        }
        self.real_stage_indices = hook._snapshot_aliases.stage_indices
        self.staging = patch.object(hook._snapshot_aliases, "stage_indices", return_value={
            "actions": [{"add": {"index": "alliance_build", "alias": "dingo_staging"}}],
            "missing_indices": ["alliance_build"], "staging_state": self.expected_staging,
        }).start()

    def health(self, name="alliance_build", *, primary=1, shards=1, status="yellow"):
        names = [name] if isinstance(name, str) else name
        return response({"indices": {index_name: {
            "status": status, "number_of_shards": shards, "active_primary_shards": primary,
        } for index_name in names}})

    def staging_mutations(self):
        return [call for call in self.staging.call_args_list if not call.kwargs.get("dry_run", False)]

    def discover(self, documents):
        self.load_builds.side_effect = None
        self.load_builds.return_value = documents

    def build(self, name, source="alliance", started_at="2026-11-03T12:00:00Z"):
        return {"_id": name, "build_config": {"name": source}, "started_at": started_at}

    async def install(self, names=None, **kwargs):
        batches = await asyncio.gather(*hook.install_snapshots("ci_snapshots", names, **kwargs))
        return batches[0]["indices"] if batches else []

    async def test_auto_discovers_newest_builds_without_target_index_registration(self):
        self.discover([
            self.build("alliance_old", started_at="2026-11-02T12:00:00Z"),
            self.build("alliance_new"), self.build("bgee_new", source="bgee"),
            self.build("unmanaged_new", source="unmanaged"),
        ])
        result = hook.install_snapshots("ci_snapshots", dry_run=True)
        self.assertEqual([row["index_name"] for row in result["indices"]], ["alliance_new", "bgee_new"])
        self.load_builds.assert_called_once_with()
        self.select.assert_not_called()
        self.get_collection.assert_not_called()
        self.assertEqual(self.origin.client.mock_calls, [])
        self.target.job_manager.defer_to_thread.assert_not_awaited()

    async def test_auto_includes_special_datasets(self):
        self.discover([
            self.build("ubergraph_nodes_new", source="ubergraph_nodes"),
            self.build("ubergraph_nodes_info_new", source="ubergraph_nodes_info"),
        ])
        result = hook.install_snapshots("ci_snapshots", dry_run=True)
        self.assertEqual(set(row["index_name"] for row in result["indices"]), {
            "ubergraph_nodes_new", "ubergraph_nodes_info_new",
        })

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
        results = await self.install()
        self.assertEqual([item["index_name"] for item in results], ["bgee_new"])
        self.assertEqual(self.target.client.indices.get.call_args.kwargs["index"], ["bgee_new"])
        self.assertNotIn("alliance_old", [call.kwargs["snapshot"] for call in self.target.client.snapshot.get.call_args_list])
        self.assertEqual(self.staging_mutations()[0].args[1], ["bgee_new"])

    async def test_auto_with_only_missing_snapshots_does_not_query_destination(self):
        self.discover([self.build("alliance_build")])
        for snapshots in ([], [snapshot(state="IN_PROGRESS")], [snapshot(state="PARTIAL")],
                          [snapshot(state="FAILED")], [snapshot(failed=1)]):
            with self.subTest(snapshots=snapshots):
                self.target.client.snapshot.get.side_effect = None
                self.target.client.snapshot.get.return_value = response({"snapshots": snapshots})
                self.assertEqual(hook.install_snapshots("ci_snapshots", dry_run=True)["indices"][0]["action"], "skip")
                self.assertEqual(hook.install_snapshots("ci_snapshots"), [])
        self.target.client.indices.get.assert_not_called()
        self.staging.assert_not_called()
        self.target.job_manager.defer_to_thread.assert_not_awaited()

    async def test_auto_empty_or_ambiguous_selection_does_not_query_es(self):
        for documents in ([], [self.build("alliance_a"), self.build("alliance_b")]):
            self.discover(documents)
            self.assertEqual(hook.install_snapshots("ci_snapshots", dry_run=True)["indices"], [])
            self.assertEqual(hook.install_snapshots("ci_snapshots"), [])
        self.assertEqual(self.target.client.mock_calls, [])
        self.warning.assert_called()

    async def test_restore_uses_configured_target_client_without_inherited_aliases(self):
        names = ["alliance_build"]
        tasks = hook.install_snapshots("ci_snapshots", names)
        self.assertEqual(len(tasks), 1)
        self.assertTrue(all(isinstance(task, asyncio.Task) for task in tasks))
        batch = (await asyncio.gather(*tasks))[0]
        results = batch["indices"]
        self.assertTrue(batch["staged"])
        self.assertEqual(batch["staging"], self.staging.return_value)
        self.assertEqual(results[0]["status"], "DONE")
        self.assertTrue(results[0]["staged"])
        self.target.client.options.assert_called_once_with(max_retries=0, retry_on_timeout=False)
        self.target.client.snapshot.restore.assert_called_once_with(
            repository="ci_repository", snapshot="alliance_build", indices="alliance_build",
            include_aliases=False, include_global_state=False,
        )
        self.target.client.cluster.health.assert_called_once_with(index="alliance_build", level="indices")
        self.assertEqual(self.origin.client.mock_calls, [])
        self.get_collection.assert_not_called()
        self.load_builds.assert_not_called()
        self.target.client.close.assert_not_called()
        self.assertEqual(names, ["alliance_build"])

    async def test_empty_index_list_does_nothing(self):
        self.assertEqual(hook.install_snapshots("ci_snapshots", []), [])
        self.assertEqual(hook.install_snapshots("ci_snapshots", [], dry_run=True)["indices"], [])
        self.assertEqual(self.target.client.mock_calls, [])
        self.staging.assert_not_called()
        self.load_builds.assert_not_called()

    async def test_dry_run_contains_staging_actions_and_captured_holders(self):
        result = hook.install_snapshots("ci_snapshots", ["alliance_build"], dry_run=True)
        self.assertTrue(result["dry_run"])
        self.assertTrue(result["stage"])
        self.assertFalse(result["restore_aliases"])
        self.assertEqual(result["expected_staging"], self.expected_staging)
        self.assertEqual(result["staging"]["actions"], self.staging.return_value["actions"])
        self.assertEqual(result["indices"][0]["snapshot_uuid"], "snapshot-uuid")
        self.staging.assert_called_once_with(
            self.target.client, ["alliance_build"], sources=None, alias_profile="default", dry_run=True,
        )
        self.target.job_manager.defer_to_thread.assert_not_awaited()
        self.assertEqual(self.staging_mutations(), [])

    async def test_stage_false_accepts_custom_names_without_datasource_lookup(self):
        self.staging.side_effect = AssertionError("No staging when disabled")
        result = await self.install(["custom_oneoff"], stage=False, alias_profile="unused-profile")
        self.assertFalse(result[0]["staged"])
        self.staging.assert_not_called()
        self.load_builds.assert_not_called()
        self.target.client.snapshot.restore.assert_called_once()

    async def test_custom_sources_mapping_is_forwarded_without_hub_lookup(self):
        sources = {"custom_oneoff": "alliance"}
        await self.install(["custom_oneoff"], sources=sources)
        self.assertEqual(self.staging.call_args_list[0].kwargs, {
            "sources": sources, "alias_profile": "default", "dry_run": True,
        })
        call = self.staging_mutations()[0]
        self.assertEqual(call.kwargs, {
            "sources": sources, "alias_profile": "default", "expected_staging": self.expected_staging,
        })
        self.load_builds.assert_not_called()

    async def test_testing_profile_is_forwarded_to_preview_and_staging(self):
        self.expected_staging["alias_profile"] = "testing"
        preview = hook.install_snapshots(
            "ci_snapshots", ["alliance_build"], alias_profile="testing", dry_run=True,
        )
        self.assertEqual(preview["alias_profile"], "testing")
        self.assertEqual(preview["expected_staging"]["alias_profile"], "testing")
        await self.install(["alliance_build"], alias_profile="testing")
        self.assertTrue(all(call.kwargs["alias_profile"] == "testing" for call in self.staging.call_args_list))
        self.assertEqual(self.staging_mutations()[0].kwargs["expected_staging"]["alias_profile"], "testing")

    async def test_one_staging_update_waits_for_all_restores_and_primary_recovery(self):
        events = []
        bgee_checks = 0

        def health(**kwargs):
            nonlocal bgee_checks
            name = kwargs["index"]
            events.append(("health", name))
            self.assertEqual(self.staging_mutations(), [])
            if name == "bgee_build":
                bgee_checks += 1
                if bgee_checks == 1:
                    return self.health(name, primary=0, status="red")
            return self.health(name)

        def stage(*args, **kwargs):
            if not kwargs.get("dry_run", False):
                events.append(("stage", args[1]))
            return {"actions": [], "staging_state": self.expected_staging}

        self.target.client.cluster.health.side_effect = health
        self.staging.side_effect = stage
        tasks = hook.install_snapshots("ci_snapshots", ["alliance_build", "bgee_build"])
        results = (await asyncio.gather(*tasks))[0]["indices"]
        self.assertEqual(set(item["index_name"] for item in results), {"alliance_build", "bgee_build"})
        self.assertEqual(len(self.staging_mutations()), 1)
        self.assertEqual(events[-1], ("stage", ["alliance_build", "bgee_build"]))
        self.assertEqual(bgee_checks, 2)
        self.assertEqual(len(self.staging.call_args_list), 2)

    async def test_partial_restore_failure_leaves_staging_unchanged(self):
        self.target.client.snapshot.restore.side_effect = [response({"accepted": True}), RuntimeError("disk full")]
        tasks = hook.install_snapshots("ci_snapshots", ["alliance_build", "bgee_build"])
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertTrue(all(isinstance(result, RuntimeError) for result in results))
        self.assertIn("disk full", str(results[0]))
        self.assertEqual(self.staging_mutations(), [])
        self.target.client.cluster.health.assert_called_once_with(index="alliance_build", level="indices")

    async def test_cancelling_batch_before_recovery_finishes_never_stages(self):
        monitoring = asyncio.Event()
        self.target.pinfo.get_pinfo.side_effect = lambda step, name: {"step": step}

        async def defer(pinfo, func):
            if pinfo["step"] == "recovery":
                monitoring.set()
                await asyncio.Event().wait()
            future = asyncio.get_running_loop().create_future()
            future.set_result(func())
            return future

        self.target.job_manager.defer_to_thread.side_effect = defer
        tasks = hook.install_snapshots("ci_snapshots", ["alliance_build"])
        await monitoring.wait()
        tasks[0].cancel()
        with self.assertRaises(asyncio.CancelledError):
            await tasks[0]
        self.target.client.snapshot.restore.assert_called_once()
        self.assertEqual(self.staging_mutations(), [])

    async def test_stale_staging_failure_is_reported_without_retry_or_restore_resubmission(self):
        def stage(*args, **kwargs):
            if not kwargs.get("dry_run", False):
                raise ValueError("Staging changed since installation started")
            return {"actions": [], "staging_state": self.expected_staging}

        self.staging.side_effect = stage
        with self.assertRaisesRegex(ValueError, "Staging changed"):
            await self.install(["alliance_build"])
        self.target.client.snapshot.restore.assert_called_once()
        self.assertEqual(len(self.staging_mutations()), 1)

    async def test_missing_snapshot_aborts_whole_batch_before_restoring(self):
        self.target.client.snapshot.get.side_effect = [
            response({"snapshots": [snapshot()]}), response({"snapshots": []}),
        ]
        with self.assertRaisesRegex(ValueError, "missing or incomplete"):
            hook.install_snapshots("ci_snapshots", ["alliance_build", "bgee_build"])
        self.target.client.snapshot.restore.assert_not_called()

    async def test_dry_run_reports_all_missing_and_existing_targets(self):
        names = ["alliance_build", "missing", "already_present"]
        self.target.client.snapshot.get.side_effect = lambda **kwargs: response({
            "snapshots": [] if kwargs["snapshot"] == "missing" else [snapshot(kwargs["snapshot"])]
        })
        self.target.client.indices.get.return_value = response({"already_present": {"aliases": {}}})
        result = hook.install_snapshots("ci_snapshots", names, dry_run=True)
        self.assertEqual([row["action"] for row in result["indices"]], ["restore", "blocked", "blocked"])
        with self.assertRaisesRegex(ValueError, "Cannot install snapshots"):
            hook.install_snapshots("ci_snapshots", names)
        self.target.client.snapshot.restore.assert_not_called()
        self.assertEqual(self.staging_mutations(), [])

    async def test_existing_indices_and_aliases_are_not_overwritten(self):
        for existing in ({"alliance_build": {"aliases": {}}},
                         {"existing_index": {"aliases": {"alliance_build": {}}}}):
            self.target.client.indices.get.return_value = response(existing)
            result = hook.install_snapshots("ci_snapshots", ["alliance_build"], dry_run=True)
            self.assertEqual(result["indices"][0]["action"], "blocked")
            with self.assertRaisesRegex(ValueError, "already exists"):
                hook.install_snapshots("ci_snapshots", ["alliance_build"])
        self.target.client.indices.delete.assert_not_called()
        self.target.client.indices.close.assert_not_called()
        self.target.client.snapshot.restore.assert_not_called()

    async def test_duplicate_wildcard_and_nonlist_targets_are_rejected(self):
        for value in (["alliance_build"] * 2, [""], ["*"], ["a,b"], ["_all"], [None], [release_entry()], "alliance_build"):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                hook.install_snapshots("ci_snapshots", value)
        self.target.client.snapshot.get.assert_not_called()
        self.staging.assert_not_called()

    async def test_incomplete_or_wrong_snapshot_contents_are_rejected(self):
        wrong = snapshot()
        wrong["indices"].append("another_index")
        for snapshots in ([], [snapshot(state="IN_PROGRESS")], [snapshot(state="PARTIAL")],
                          [snapshot(state="FAILED")], [snapshot(failed=1)], [wrong]):
            self.target.client.snapshot.get.side_effect = None
            self.target.client.snapshot.get.return_value = response({"snapshots": snapshots})
            with self.subTest(snapshots=snapshots), self.assertRaises(ValueError):
                hook.install_snapshots("ci_snapshots", ["alliance_build"])
        self.target.client.snapshot.restore.assert_not_called()

    async def test_snapshot_uuid_is_checked_again_before_submission(self):
        self.target.client.snapshot.get.side_effect = [
            response({"snapshots": [snapshot()]}),
            response({"snapshots": [snapshot(uuid="replacement-uuid")]}),
        ]
        with self.assertRaisesRegex(ValueError, "Snapshot changed after preflight"):
            await self.install(["alliance_build"])
        self.target.client.snapshot.restore.assert_not_called()
        self.assertEqual(self.staging_mutations(), [])

    async def test_primary_count_must_match_snapshot_even_if_some_recovery_is_done(self):
        self.target.client.cluster.health.side_effect = [
            self.health(primary=1, shards=2, status="yellow"),
            self.health(primary=0, status="red"),
            self.health(primary=1, shards=1, status="yellow"),
        ]
        result = await self.install(["alliance_build"])
        self.assertEqual(result[0]["status"], "DONE")
        self.assertEqual(self.target.client.cluster.health.call_count, 3)
        self.assertEqual(len(self.staging_mutations()), 1)

    async def test_recovery_timeout_does_not_stage_or_resubmit_restore(self):
        self.target.client.cluster.health.side_effect = lambda **kwargs: self.health(primary=0, status="red")
        with self.assertRaisesRegex(TimeoutError, "Primary-shard recovery timed out"):
            await self.install(["alliance_build"], recovery_timeout=0.005)
        self.target.client.snapshot.restore.assert_called_once()
        self.assertEqual(self.staging_mutations(), [])

    async def test_invalid_recovery_timeouts_are_rejected_before_lookup(self):
        for timeout in (0, -1, float("inf"), float("nan"), True, "10"):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, "positive number"):
                hook.install_snapshots("ci_snapshots", ["alliance_build"], recovery_timeout=timeout)
        self.target.client.snapshot.get.assert_not_called()

    async def test_unaccepted_restore_does_not_start_polling(self):
        self.target.client.snapshot.restore.return_value = response({"accepted": False})
        with self.assertRaisesRegex(RuntimeError, "not accepted"):
            await self.install(["alliance_build"])
        self.target.client.cluster.health.assert_not_called()
        self.assertEqual(self.staging_mutations(), [])

    async def test_restore_request_failure_is_not_retried(self):
        self.target.client.snapshot.restore.side_effect = RuntimeError("connection lost")
        with self.assertRaisesRegex(RuntimeError, "connection lost"):
            await self.install(["alliance_build"])
        self.target.client.snapshot.restore.assert_called_once()
        self.target.client.cluster.health.assert_not_called()
        self.assertEqual(self.staging_mutations(), [])

    async def test_transient_monitor_failure_does_not_resubmit_restore(self):
        self.target.client.cluster.health.side_effect = [RuntimeError("temporary"), self.health()]
        await self.install(["alliance_build"])
        self.target.client.snapshot.restore.assert_called_once()
        self.assertEqual(self.target.client.cluster.health.call_count, 2)

    async def test_repeated_monitor_failures_fail_without_staging(self):
        self.target.client.cluster.health.side_effect = RuntimeError("ES unavailable")
        with self.assertRaisesRegex(RuntimeError, "ES unavailable"):
            await self.install(["alliance_build"])
        self.assertEqual(self.target.client.cluster.health.call_count, 3)
        self.target.client.snapshot.restore.assert_called_once()
        self.assertEqual(self.staging_mutations(), [])

    async def test_real_staging_service_swaps_special_alias_after_recovery(self):
        await self.check_real_staging_profile("default")

    async def test_testing_staging_keeps_default_aliases_unchanged(self):
        await self.check_real_staging_profile("testing")

    async def check_real_staging_profile(self, profile):
        name = "ubergraph_nodes_20261105_abcd"
        previous = "ubergraph_nodes_20261104_abcd"
        alias = hook._snapshot_aliases.alias_policy("ubergraph_nodes", alias_profile=profile)["staging_alias"]
        inventory = {previous: {
            "settings": {"index": {"uuid": "old-uuid"}}, "aliases": {alias: {}},
        }}
        default_holder = "ubergraph_nodes_20261103_abcd"
        if profile == "testing":
            inventory[default_holder] = {
                "settings": {"index": {"uuid": "default-uuid"}}, "aliases": {"ubergraph_nodes_staging": {}},
            }
        self.staging.side_effect = self.real_stage_indices
        self.target.client.info.return_value = response({"cluster_uuid": "ci-cluster"})

        def indices(**kwargs):
            selectors = set(kwargs["index"])
            return response({
                key: info for key, info in inventory.items()
                if key in selectors or selectors.intersection(info["aliases"])
            })

        def restore(**kwargs):
            self.assertFalse(kwargs["include_aliases"])
            inventory[name] = {"settings": {"index": {"uuid": "new-uuid"}}, "aliases": {}}
            return response({"accepted": True})

        def update_aliases(*, actions):
            self.target.client.cluster.health.assert_any_call(index=name, level="indices")
            self.target.client.cluster.health.assert_any_call(index=[name], level="indices")
            for action in actions:
                operation, details = next(iter(action.items()))
                aliases = inventory[details["index"]]["aliases"]
                if operation == "add":
                    aliases[details["alias"]] = {}
                else:
                    del aliases[details["alias"]]
            return response({"acknowledged": True, "errors": False})

        self.target.client.indices.get.side_effect = indices
        self.target.client.snapshot.restore.side_effect = restore
        self.target.client.indices.update_aliases.side_effect = update_aliases
        results = await self.install([name], alias_profile=profile)
        self.assertTrue(results[0]["staged"])
        self.assertEqual(inventory[name]["aliases"], {alias: {}})
        deletion_alias = "prod_testing_to_be_deleted" if profile == "testing" else "dingo_to_be_deleted"
        self.assertEqual(inventory[previous]["aliases"], {deletion_alias: {}})
        if profile == "testing":
            self.assertEqual(inventory[default_holder]["aliases"], {"ubergraph_nodes_staging": {}})
        self.target.client.indices.update_aliases.assert_called_once()
        self.load_builds.assert_not_called()

    async def test_unknown_custom_source_is_rejected_before_restore(self):
        self.staging.side_effect = self.real_stage_indices
        with self.assertRaisesRegex(ValueError, "supply sources"):
            hook.install_snapshots("ci_snapshots", ["custom_oneoff"])
        self.target.client.snapshot.restore.assert_not_called()

    async def test_unknown_profile_is_rejected_before_restore_when_staging(self):
        self.staging.side_effect = self.real_stage_indices
        with self.assertRaisesRegex(ValueError, "profile"):
            hook.install_snapshots("ci_snapshots", ["alliance_20261105_abcd"], alias_profile="unused-profile")
        self.target.client.snapshot.restore.assert_not_called()

    async def test_real_client_sends_alias_free_restore_and_checks_target_health(self):
        meta = ApiResponseMeta(
            status=200, http_version="1.1", duration=0,
            headers=HttpHeaders({"x-elastic-product": "Elasticsearch"}),
            node=NodeConfig("http", "unused.invalid", 9200),
        )
        posts = []

        def perform_request(method, target, **kwargs):
            if method == "GET":
                if target.startswith("/_snapshot/"):
                    return meta, {"snapshots": [snapshot()]}
                if target.startswith("/_cluster/health/"):
                    self.assertIn("level=indices", target)
                    return meta, self.health().body
                return meta, {}
            self.assertEqual(method, "POST")
            self.assertEqual(target, "/_snapshot/ci_repository/alliance_build/_restore")
            self.assertEqual(kwargs["body"], {
                "indices": "alliance_build", "include_aliases": False, "include_global_state": False,
            })
            posts.append(target)
            return meta, {"accepted": True}

        with Elasticsearch("http://unused.invalid:9200") as client:
            self.target.client = client
            with patch.object(client.transport, "perform_request", side_effect=perform_request):
                results = await self.install(["alliance_build"])
        self.assertEqual(results[0]["status"], "DONE")
        self.assertEqual(len(posts), 1)


if __name__ == "__main__":
    unittest.main()

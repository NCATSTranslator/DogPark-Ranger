import importlib.util
import json
import sys
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch


HOOK_PATH = (
    Path(__file__).resolve().parents[1]
    / "hooks"
    / "dogpark_release.py"
)
# Mirror deployment: the hooks package is imported from the Hub project root.
sys.path.insert(0, str(HOOK_PATH.parents[1]))
SPEC = importlib.util.spec_from_file_location("dogpark_release_hook", HOOK_PATH)
HOOK = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(HOOK)


_MISSING = object()


def build_document(
    config,
    started_at,
    *,
    build_id=None,
    target_name=None,
    status="success",
    index_name=None,
    environment="itrb-ci",
    archived=_MISSING,
    extra_indices=None,
):
    build_id = build_id or f"{config}_20261001_aaaaaaaa"
    target_name = target_name or build_id
    index_name = index_name or build_id
    registrations = {
        index_name: {
            "environment": environment,
            "count": 1,
        }
    }
    registrations.update(extra_indices or {})
    document = {
        "_id": build_id,
        "target_name": target_name,
        "build_config": {"_id": config, "name": config},
        "started_at": started_at,
        "jobs": [{"status": status}],
        "index": registrations,
    }
    if archived is not _MISSING:
        document["archived"] = archived
    return document


@contextmanager
def dataset_policy(release, special=()):
    release = tuple(release)
    special = tuple(special)
    with (
        patch.object(HOOK, "DOGPARK_RELEASE_DATASETS", release),
        patch.object(HOOK, "DOGPARK_NO_STAGING_TAG_DATASETS", special),
        patch.object(HOOK, "DOGPARK_MANAGED_DATASETS", release + special),
        patch.object(HOOK._dogpark_builds, "DOGPARK_RELEASE_DATASETS", release),
        patch.object(HOOK._dogpark_builds, "DOGPARK_NO_STAGING_TAG_DATASETS", special),
        patch.object(HOOK._dogpark_builds, "DOGPARK_MANAGED_DATASETS", release + special),
    ):
        yield


def empty_aliases():
    return {
        "prod": set(),
        "staging": set(),
        "deprecated": set(),
        "delete": set(),
    }


class SelectionTests(unittest.TestCase):
    def test_hook_executes_like_biothings_loader_without_file_global(self):
        hub_list = Mock(side_effect=AssertionError("Hub list command was called"))
        namespace = {"__name__": "dogpark_release_hook_exec", "list": hub_list}
        source = HOOK_PATH.read_text(encoding="utf-8")

        exec(compile(source, "<string>", "exec"), namespace, namespace)

        self.assertIn("plan_release_manifest", namespace)
        self.assertIn("freeze_release_manifest", namespace)
        self.assertIn("plan_prod_release", namespace)
        self.assertIn("release_prod_manifest", namespace)
        self.assertIs(namespace["release_prod_manifest"].confirm, True)
        self.assertTrue(
            namespace["_DOGPARK_RELEASE_RECORDS_DIR"].endswith(
                "/release-records"
            )
        )
        self.assertEqual(
            namespace["_dogpark_expected_post_staging_aliases"]({
                "observed_aliases": {"dingo_staging": ("old",)},
                "staging_remove_indices": ["old"],
                "staging_add_indices": ["new"],
            }),
            {"dingo_staging": ["new"]},
        )
        namespace["_dogpark_verify_manifest"] = lambda manifest: None
        with self.assertRaisesRegex(RuntimeError, "Manifest entries must be a list"):
            namespace["_dogpark_validate_manifest_contract"]({"entries": None})
        hub_list.assert_not_called()

    def test_loads_one_projected_src_build_read(self):
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
        )
        collection = Mock()
        collection.find.return_value = [document]

        with patch.object(
            HOOK,
            "_dogpark_get_src_build_collection",
            return_value=collection,
        ):
            result = HOOK._dogpark_load_build_documents()

        self.assertEqual([document], result)
        query, projection = collection.find.call_args.args
        self.assertEqual({}, query)
        self.assertEqual(1, projection["started_at"])
        self.assertEqual(1, projection["jobs"])
        self.assertEqual(1, projection["index"])

    def test_selects_actual_latest_timestamp_not_input_order_or_day(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-01T08:00:00-07:00",
                build_id="alliance_20261001_olderaaa",
            ),
            build_document(
                "alliance",
                datetime(2026, 10, 1, 17, 1, tzinfo=timezone.utc),
                build_id="alliance_20261001_neweraaa",
            ),
            build_document(
                "alliance",
                "2026-10-01T16:30:00Z",
                build_id="alliance_20261001_middleaa",
            ),
        ]

        with dataset_policy(["alliance"]):
            result = HOOK.dogpark_select_latest_builds(documents)

        self.assertEqual(
            ["alliance_20261001_neweraaa"],
            [item["build_id"] for item in result["selected"]],
        )

    def test_same_timestamp_is_ambiguous(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-01T17:00:00Z",
                build_id="alliance_20261001_aaaaaaaa",
            ),
            build_document(
                "alliance",
                "2026-10-01T17:00:00Z",
                build_id="alliance_20261001_bbbbbbbb",
            ),
        ]

        with dataset_policy(["alliance"]):
            result = HOOK.dogpark_select_latest_builds(documents)

        self.assertEqual([], result["selected"])
        self.assertEqual("ambiguous_newest_build", result["pending"][0]["reason"])

    def test_archived_field_presence_is_preserved_even_when_none(self):
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            archived=None,
        )

        with dataset_policy(["alliance"]):
            result = HOOK.dogpark_select_latest_builds([document])

        self.assertTrue(result["selected"][0]["archived"])

    def test_special_dataset_is_skipped_by_policy(self):
        document = build_document(
            "ubergraph_nodes",
            "2026-10-01T17:00:00Z",
        )

        with dataset_policy([], ["ubergraph_nodes"]):
            result = HOOK.dogpark_select_latest_builds([document])

        self.assertEqual([], result["selected"])
        self.assertEqual(
            [{"config": "ubergraph_nodes", "reason": "staging_tag_disabled"}],
            result["skipped"],
        )


class PlanningTests(unittest.TestCase):
    def plan(self, documents, aliases=None, existing=None, release=None):
        aliases = aliases or empty_aliases()
        if existing is None:
            existing = {
                name
                for document in documents
                for name in document.get("index", {})
            }
        release = release or sorted(
            {
                document["build_config"]["_id"]
                for document in documents
            }
        )
        with (
            dataset_policy(release),
            patch.object(HOOK, "_dogpark_validate_cluster", return_value="test-cluster"),
            patch.object(HOOK, "_dogpark_load_build_documents", return_value=documents),
            patch.object(HOOK, "_dogpark_get_alias_inventory", return_value=aliases),
            patch.object(
                HOOK,
                "_dogpark_index_exists",
                side_effect=lambda name, _uri: name in existing,
            ),
        ):
            return HOOK.plan_staging_tags(
                "http://es.invalid:9200",
                "test-cluster",
                "itrb-ci",
            )

    def test_newest_not_ready_does_not_fall_back_to_older_ready_build(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-01T17:00:00Z",
                build_id="alliance_20261001_neweraaa",
                status="in progress",
            ),
            build_document(
                "alliance",
                "2026-10-01T16:00:00Z",
                build_id="alliance_20261001_olderaaa",
            ),
        ]

        plan = self.plan(documents)

        self.assertEqual([], plan["eligible"])
        self.assertEqual("alliance_20261001_neweraaa", plan["selected"][0]["build_id"])
        self.assertEqual("build_not_ready", plan["pending"][0]["reason"])

    def test_stale_cancelled_post_index_after_successful_index_is_eligible(self):
        document = build_document(
            "semmeddb",
            "2026-09-29T17:00:00Z",
            build_id="semmeddb_20260929_ieowfv7q",
        )
        document["jobs"] = [
            {
                "step": "index",
                "status": "success",
                "logfile": "/logs/index_semmeddb_20260929_ieowfv7q.log",
            },
            {
                "step": "post-index",
                "status": "cancelled",
                "logfile": "/logs/index_semmeddb_20260929_ieowfv7q.log",
            },
        ]

        plan = self.plan([document])

        self.assertEqual([], plan["pending"])
        self.assertEqual(1, len(plan["eligible"]))
        self.assertEqual(
            "ready_to_stage_after_stale_post_index",
            plan["eligible"][0]["reason"],
        )
        self.assertEqual("cancelled", plan["eligible"][0]["status"])
        self.assertEqual(
            "cancelled_post_index_after_successful_index",
            plan["eligible"][0]["readiness_warning"],
        )

    def test_cancelled_main_index_remains_pending(self):
        document = build_document(
            "semmeddb",
            "2026-09-29T17:00:00Z",
        )
        document["jobs"] = [{"step": "index", "status": "cancelled"}]

        plan = self.plan([document])

        self.assertEqual([], plan["eligible"])
        self.assertEqual("build_not_ready", plan["pending"][0]["reason"])

    def test_failed_or_erroneous_post_index_remains_pending(self):
        for post_index_job in (
            {"step": "post-index", "status": "failed"},
            {
                "step": "post-index",
                "status": "cancelled",
                "err": "real post-index failure",
            },
        ):
            with self.subTest(post_index_job=post_index_job):
                document = build_document(
                    "semmeddb",
                    "2026-09-29T17:00:00Z",
                )
                document["jobs"] = [
                    {
                        "step": "index",
                        "status": "success",
                        "logfile": "/logs/index_semmeddb_20261001_aaaaaaaa.log",
                    },
                    post_index_job,
                ]

                plan = self.plan([document])

                self.assertEqual([], plan["eligible"])
                self.assertEqual(
                    "build_not_ready",
                    plan["pending"][0]["reason"],
                )

    def test_cancelled_post_index_must_immediately_follow_successful_index(self):
        document = build_document(
            "semmeddb",
            "2026-09-29T17:00:00Z",
        )
        document["jobs"] = [
            {
                "step": "index",
                "status": "success",
                "logfile": "/logs/index_semmeddb_20261001_aaaaaaaa.log",
            },
            {"step": "snapshot", "status": "success"},
            {
                "step": "post-index",
                "status": "cancelled",
                "logfile": "/logs/index_semmeddb_20261001_aaaaaaaa.log",
            },
        ]

        plan = self.plan([document])

        self.assertEqual([], plan["eligible"])
        self.assertEqual("build_not_ready", plan["pending"][0]["reason"])

    def test_cancelled_post_index_logfile_must_identify_resolved_index(self):
        document = build_document(
            "semmeddb",
            "2026-09-29T17:00:00Z",
            index_name="semmeddb-current-index",
        )
        document["jobs"] = [
            {
                "step": "index",
                "status": "success",
                "logfile": "/logs/index_semmeddb-old-index.log",
            },
            {
                "step": "post-index",
                "status": "cancelled",
                "logfile": "/logs/index_semmeddb-old-index.log",
            },
        ]

        plan = self.plan([document])

        self.assertEqual([], plan["eligible"])
        self.assertEqual("build_not_ready", plan["pending"][0]["reason"])

    def test_cancelled_post_index_requires_same_nonempty_logfile(self):
        for index_logfile, post_index_logfile in (
            (None, None),
            ("/logs/first-attempt.log", "/logs/second-attempt.log"),
        ):
            with self.subTest(
                index_logfile=index_logfile,
                post_index_logfile=post_index_logfile,
            ):
                document = build_document(
                    "semmeddb",
                    "2026-09-29T17:00:00Z",
                )
                document["jobs"] = [
                    {
                        "step": "index",
                        "status": "success",
                        "logfile": index_logfile,
                    },
                    {
                        "step": "post-index",
                        "status": "cancelled",
                        "logfile": post_index_logfile,
                    },
                ]

                plan = self.plan([document])

                self.assertEqual([], plan["eligible"])
                self.assertEqual(
                    "build_not_ready",
                    plan["pending"][0]["reason"],
                )

    def test_custom_registered_index_name_is_authoritative(self):
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            build_id="alliance_20261001_buildaaa",
            target_name="alliance_mongo_collection",
            index_name="custom-alliance-index-v2",
        )

        plan = self.plan([document])

        self.assertEqual("custom-alliance-index-v2", plan["eligible"][0]["index"])
        self.assertEqual(
            "alliance_mongo_collection",
            plan["eligible"][0]["target_name"],
        )

    def test_only_registration_for_requested_environment_is_considered(self):
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name="alliance-ci-index",
            extra_indices={
                "alliance-dev-index": {"environment": "dev", "count": 1},
            },
        )

        plan = self.plan(
            [document],
            existing={"alliance-ci-index", "alliance-dev-index"},
        )

        self.assertEqual("alliance-ci-index", plan["eligible"][0]["index"])

    def test_multiple_target_environment_indices_are_rejected(self):
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name="alliance-ci-one",
            extra_indices={
                "alliance-ci-two": {"environment": "itrb-ci", "count": 1},
            },
        )

        plan = self.plan(
            [document],
            existing={"alliance-ci-one", "alliance-ci-two"},
        )

        self.assertEqual([], plan["eligible"])
        self.assertEqual("ambiguous_target_indices", plan["rejected"][0]["reason"])

    def test_alias_states_and_history_based_holder_ownership(self):
        configs = ["eligible", "prod", "staging", "deprecated", "blocked"]
        latest = [
            build_document(
                config,
                "2026-10-01T17:00:00Z",
                build_id=f"{config}_20261001_newbuild",
                index_name=f"custom-{config}-new",
            )
            for config in configs
        ]
        old_blocked = build_document(
            "blocked",
            "2026-09-30T17:00:00Z",
            build_id="blocked_20260930_oldbuild",
            index_name="nonstandard-old-staging-name",
        )
        aliases = empty_aliases()
        aliases["prod"] = {"custom-prod-new"}
        aliases["staging"] = {
            "custom-staging-new",
            "nonstandard-old-staging-name",
        }
        aliases["deprecated"] = {"custom-deprecated-new"}

        plan = self.plan(latest + [old_blocked], aliases=aliases, release=configs)

        self.assertEqual(["custom-eligible-new"], [x["index"] for x in plan["eligible"]])
        self.assertEqual(["custom-prod-new"], [x["index"] for x in plan["already_prod"]])
        self.assertEqual(
            ["custom-staging-new"],
            [x["index"] for x in plan["already_staging"]],
        )
        rejected = {entry["config"]: entry for entry in plan["rejected"]}
        self.assertEqual("conflicting_release_state", rejected["deprecated"]["reason"])
        self.assertEqual("different_staging_holder", rejected["blocked"]["reason"])
        self.assertEqual(
            ["nonstandard-old-staging-name"],
            rejected["blocked"]["indices"],
        )

    def test_archived_newest_stays_pending_and_does_not_fall_back(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-01T17:00:00Z",
                build_id="alliance_20261001_neweraaa",
                archived=None,
            ),
            build_document(
                "alliance",
                "2026-09-30T17:00:00Z",
                build_id="alliance_20260930_olderaaa",
            ),
        ]

        plan = self.plan(documents)

        self.assertEqual([], plan["eligible"])
        self.assertEqual("newest_build_archived", plan["pending"][0]["reason"])

    def test_missing_concrete_index_is_pending(self):
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
        )

        plan = self.plan([document], existing=set())

        self.assertEqual([], plan["eligible"])
        self.assertEqual("index_not_found", plan["pending"][0]["reason"])


class ElasticsearchAndApplyTests(unittest.TestCase):
    def test_index_exists_requires_exact_concrete_index_key(self):
        with patch.object(
            HOOK,
            "_dogpark_request_json",
            return_value={"underlying-index": {"settings": {}}},
        ):
            self.assertFalse(HOOK._dogpark_index_exists("alias-name", "http://es"))

        with patch.object(
            HOOK,
            "_dogpark_request_json",
            return_value={"actual-index": {"settings": {}}},
        ):
            self.assertTrue(HOOK._dogpark_index_exists("actual-index", "http://es"))

    def test_assign_staging_tags_freezes_then_applies_exact_manifest(self):
        manifest = {
            "manifest_id": "manifest-123",
            "complete": True,
            "blocked": [],
            "counts": {"desired": 2},
            "desired_indices": ["a-index", "z-index"],
        }
        storage = {"manifest_id": "manifest-123"}
        frozen = {"manifest": manifest, "storage": storage}
        application = {
            "manifest": {"manifest_id": "manifest-123"},
            "applied": True,
            "tagged": ["a-index", "z-index"],
        }
        with (
            patch.object(
                HOOK,
                "freeze_release_manifest",
                return_value=frozen,
            ) as freeze,
            patch.object(
                HOOK,
                "assign_staging_manifest",
                return_value=application,
            ) as apply_manifest,
        ):
            result = HOOK.assign_staging_tags(
                "http://es.invalid:9200",
                "test-cluster",
                "itrb-ci",
            )

        freeze.assert_called_once_with(
            "http://es.invalid:9200",
            "test-cluster",
            "itrb-ci",
        )
        apply_manifest.assert_called_once_with(
            "manifest-123",
            "http://es.invalid:9200",
            "test-cluster",
        )
        self.assertIs(application, result)
        self.assertEqual({"desired": 2}, result["counts"])
        self.assertEqual(["a-index", "z-index"], result["desired_indices"])

    def test_bulk_alias_request_contains_only_add_actions(self):
        with patch.object(
            HOOK,
            "_dogpark_request_json",
            return_value={"acknowledged": True},
        ) as request:
            HOOK._dogpark_post_staging_aliases(
                ["a-index", "z-index"],
                "http://es",
            )

        request.assert_called_once_with(
            "http://es/_aliases",
            method="POST",
            payload={
                "actions": [
                    {
                        "add": {
                            "index": "a-index",
                            "alias": "dingo_staging",
                        }
                    },
                    {
                        "add": {
                            "index": "z-index",
                            "alias": "dingo_staging",
                        }
                    },
                ]
            },
        )

    def test_mutating_command_requests_hub_confirmation(self):
        self.assertIs(HOOK.assign_staging_tags.confirm, True)


class ExplicitStagingTagTests(unittest.TestCase):
    def invoke(
        self,
        index_name,
        *,
        documents=(),
        aliases=None,
        exists=True,
        release=("alliance",),
        special=(),
    ):
        aliases = aliases or empty_aliases()
        with (
            dataset_policy(release, special),
            patch.object(
                HOOK,
                "_dogpark_validate_cluster",
                return_value="test-cluster",
            ),
            patch.object(
                HOOK,
                "_dogpark_index_exists",
                return_value=exists,
            ),
            patch.object(
                HOOK,
                "_dogpark_load_build_documents",
                return_value=list(documents),
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_inventory",
                return_value=aliases,
            ),
            patch.object(
                HOOK,
                "_dogpark_post_staging_aliases",
                return_value={"acknowledged": True},
            ) as post,
            patch.object(HOOK, "plan_staging_tags") as inferred_plan,
        ):
            result = HOOK.assign_staging_tag(
                index_name,
                "http://es",
                "test-cluster",
            )
        return result, post, inferred_plan

    def test_assigns_one_atomic_index_and_bypasses_inferred_readiness(self):
        index_name = "alliance_20261001_manualaaa"
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name=index_name,
            status="in progress",
        )

        result, post, inferred_plan = self.invoke(
            index_name,
            documents=[document],
        )

        post.assert_called_once_with([index_name], "http://es")
        inferred_plan.assert_not_called()
        self.assertTrue(result["applied"])
        self.assertEqual([index_name], result["tagged"])
        self.assertEqual("single_index_override", result["mode"])
        self.assertEqual(
            "bypassed_by_explicit_operator_selection",
            result["readiness_checks"],
        )

    def test_unregistered_custom_concrete_index_can_be_explicitly_staged(self):
        result, post, _ = self.invoke(
            "operator-selected-custom-index",
            documents=[],
        )

        post.assert_called_once_with(
            ["operator-selected-custom-index"],
            "http://es",
        )
        self.assertIsNone(result["dataset"])

    def test_rejects_non_scalar_empty_or_multi_index_input(self):
        for index_name in (None, "", "  ", "alliance_*", "one,two", "name?"):
            with self.subTest(index_name=index_name):
                with self.assertRaises((TypeError, ValueError)):
                    HOOK.assign_staging_tag(index_name)

    def test_missing_concrete_index_is_rejected_before_alias_update(self):
        with self.assertRaisesRegex(RuntimeError, "was not found"):
            self.invoke("missing-index", exists=False)

    def test_existing_staging_or_prod_state_is_an_idempotent_noop(self):
        index_name = "alliance_20261001_manualaaa"
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name=index_name,
        )
        for state, reason in (
            ("staging", "already_staging"),
            ("prod", "already_prod"),
        ):
            with self.subTest(state=state):
                aliases = empty_aliases()
                aliases[state].add(index_name)

                result, post, _ = self.invoke(
                    index_name,
                    documents=[document],
                    aliases=aliases,
                )

                post.assert_not_called()
                self.assertFalse(result["applied"])
                self.assertEqual(reason, result["reason"])

    def test_deprecated_or_delete_state_is_rejected(self):
        index_name = "alliance_20261001_manualaaa"
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name=index_name,
        )
        for state in ("deprecated", "delete"):
            with self.subTest(state=state):
                aliases = empty_aliases()
                aliases[state].add(index_name)

                with self.assertRaisesRegex(RuntimeError, "conflicting release state"):
                    self.invoke(
                        index_name,
                        documents=[document],
                        aliases=aliases,
                    )

    def test_different_staging_holder_for_known_dataset_is_rejected(self):
        current = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name="alliance-current-index",
        )
        old = build_document(
            "alliance",
            "2026-09-01T17:00:00Z",
            index_name="alliance-old-index",
        )
        aliases = empty_aliases()
        aliases["staging"].add("alliance-old-index")

        with self.assertRaisesRegex(RuntimeError, "different staging holder"):
            self.invoke(
                "alliance-current-index",
                documents=[current, old],
                aliases=aliases,
            )

    def test_special_dataset_staging_policy_is_preserved(self):
        index_name = "ubergraph_nodes_20261001_manualaaa"
        document = build_document(
            "ubergraph_nodes",
            "2026-10-01T17:00:00Z",
            index_name=index_name,
        )

        with self.assertRaisesRegex(RuntimeError, "disabled by policy"):
            self.invoke(
                index_name,
                documents=[document],
                release=(),
                special=("ubergraph_nodes",),
            )

    def test_singular_mutating_command_requests_hub_confirmation(self):
        self.assertIn("assign_staging_tag", HOOK.__all__)
        self.assertIs(HOOK.assign_staging_tag.confirm, True)

    def test_single_index_override_does_not_publish_release_manifest(self):
        index_name = "alliance_20261001_manualaaa"
        document = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name=index_name,
        )
        with TemporaryDirectory() as directory, patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ), patch.object(HOOK, "freeze_release_manifest") as freeze, patch.object(
            HOOK,
            "assign_staging_manifest",
        ) as apply_manifest:
            result, post, _ = self.invoke(index_name, documents=[document])
            published_files = list(Path(directory).glob("*.json"))

        self.assertTrue(result["applied"])
        post.assert_called_once_with([index_name], "http://es")
        freeze.assert_not_called()
        apply_manifest.assert_not_called()
        self.assertEqual([], published_files)


class ReleaseManifestPlanningTests(unittest.TestCase):
    def test_complete_desired_set_uses_prod_fallback_and_special_alias_policy(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-02T17:00:00Z",
                build_id="alliance_20261002_newbuild",
                index_name="alliance-new-index",
            ),
            build_document(
                "alliance",
                "2026-09-01T17:00:00Z",
                build_id="alliance_20260901_oldbuild",
                index_name="alliance-prod-index",
            ),
            build_document(
                "bgee",
                "2026-10-02T17:00:00Z",
                build_id="bgee_20261002_newbuild",
                index_name="bgee-new-index",
                status="in progress",
            ),
            build_document(
                "bgee",
                "2026-09-01T17:00:00Z",
                build_id="bgee_20260901_oldbuild",
                index_name="bgee-prod-index",
            ),
            build_document(
                "ubergraph_nodes_info",
                "2026-10-02T17:00:00Z",
                index_name="ubergraph-nodes-info-new",
            ),
            build_document(
                "ubergraph_nodes",
                "2026-10-02T17:00:00Z",
                index_name="ubergraph-nodes-new",
            ),
        ]
        aliases = empty_aliases()
        aliases["prod"] = {"alliance-prod-index", "bgee-prod-index"}
        index_uuids = {
            name: f"uuid-{name}"
            for document in documents
            for name in document["index"]
        }

        with (
            dataset_policy(
                ("alliance", "bgee"),
                ("ubergraph_nodes_info", "ubergraph_nodes"),
            ),
            patch.object(
                HOOK,
                "_dogpark_cluster_identity",
                return_value={
                    "cluster_name": "test-cluster",
                    "cluster_uuid": "test-cluster-uuid",
                },
            ),
            patch.object(
                HOOK,
                "_dogpark_validate_cluster",
                return_value="test-cluster",
            ),
            patch.object(
                HOOK,
                "_dogpark_load_build_documents",
                return_value=documents,
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_inventory",
                return_value=aliases,
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_indices",
                return_value=set(),
            ),
            patch.object(
                HOOK,
                "_dogpark_index_exists",
                side_effect=lambda name, _uri: name in index_uuids,
            ),
            patch.object(
                HOOK,
                "_dogpark_index_uuid",
                side_effect=lambda name, _uri: index_uuids.get(name),
            ),
        ):
            manifest = HOOK.plan_release_manifest(
                "http://es.invalid:9200",
                "test-cluster",
                "itrb-ci",
            )

        entries = {entry["config"]: entry for entry in manifest["entries"]}
        self.assertEqual(
            {
                "alliance",
                "bgee",
                "ubergraph_nodes_info",
                "ubergraph_nodes",
            },
            set(entries),
        )
        self.assertTrue(manifest["complete"])
        self.assertEqual([], manifest["blocked"])
        self.assertEqual("alliance-new-index", entries["alliance"]["index"])
        self.assertEqual("newest_ready", entries["alliance"]["selection"])
        self.assertEqual("promote", entries["alliance"]["decision"])
        self.assertEqual("bgee-prod-index", entries["bgee"]["index"])
        self.assertEqual("current_prod_fallback", entries["bgee"]["selection"])
        self.assertEqual("keep_current_prod", entries["bgee"]["decision"])
        self.assertEqual("dingo_staging", entries["alliance"]["staging_alias"])
        self.assertIsNone(entries["ubergraph_nodes_info"]["staging_alias"])
        self.assertEqual(
            "ubergraph_nodes_mapping",
            entries["ubergraph_nodes_info"]["production_alias"],
        )
        self.assertIsNone(entries["ubergraph_nodes"]["staging_alias"])
        self.assertEqual(
            "ubergraph_nodes",
            entries["ubergraph_nodes"]["production_alias"],
        )
        self.assertEqual(
            {
                "alliance-new-index",
                "bgee-prod-index",
                "ubergraph-nodes-info-new",
                "ubergraph-nodes-new",
            },
            set(manifest["desired_indices"]),
        )
        self.assertEqual(
            ["alliance-new-index"],
            manifest["staging_required_indices"],
        )

    def test_stale_staging_holder_is_recorded_for_replacement_not_prod_fallback(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-02T17:00:00Z",
                build_id="alliance_20261002_newbuild",
                index_name="alliance-new-index",
            ),
            build_document(
                "alliance",
                "2026-09-01T17:00:00Z",
                build_id="alliance_20260901_prod",
                index_name="alliance-prod-index",
            ),
            build_document(
                "alliance",
                "2026-08-01T17:00:00Z",
                build_id="alliance_20260801_stale",
                index_name="alliance-stale-staging-index",
            ),
        ]
        aliases = empty_aliases()
        aliases["prod"] = {"alliance-prod-index"}
        aliases["staging"] = {"alliance-stale-staging-index"}
        index_uuids = {
            name: f"uuid-{name}"
            for document in documents
            for name in document["index"]
        }
        with (
            dataset_policy(("alliance",)),
            patch.object(
                HOOK,
                "_dogpark_cluster_identity",
                return_value={
                    "cluster_name": "test-cluster",
                    "cluster_uuid": "test-cluster-uuid",
                },
            ),
            patch.object(
                HOOK,
                "_dogpark_validate_cluster",
                return_value="test-cluster",
            ),
            patch.object(
                HOOK,
                "_dogpark_load_build_documents",
                return_value=documents,
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_inventory",
                return_value=aliases,
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_indices",
                return_value=set(),
            ),
            patch.object(
                HOOK,
                "_dogpark_index_exists",
                side_effect=lambda name, _uri: name in index_uuids,
            ),
            patch.object(
                HOOK,
                "_dogpark_index_uuid",
                side_effect=lambda name, _uri: index_uuids.get(name),
            ),
        ):
            manifest = HOOK.plan_release_manifest(
                "http://es.invalid:9200",
                "test-cluster",
                "itrb-ci",
            )

        entry = manifest["entries"][0]
        self.assertEqual("alliance-new-index", entry["index"])
        self.assertEqual("newest_ready", entry["selection"])
        self.assertEqual("promote", entry["decision"])
        self.assertEqual(["alliance-new-index"], manifest["staging_add_indices"])
        self.assertEqual(
            ["alliance-stale-staging-index"],
            manifest["staging_remove_indices"],
        )

    def test_conflicting_current_prod_is_not_accepted_as_fallback(self):
        documents = [
            build_document(
                "alliance",
                "2026-10-02T17:00:00Z",
                build_id="alliance_20261002_newbuild",
                index_name="alliance-new-index",
                status="in progress",
            ),
            build_document(
                "alliance",
                "2026-09-01T17:00:00Z",
                build_id="alliance_20260901_prod",
                index_name="alliance-prod-index",
            ),
        ]
        aliases = empty_aliases()
        aliases["prod"] = {"alliance-prod-index"}
        aliases["deprecated"] = {"alliance-prod-index"}

        with (
            dataset_policy(("alliance",)),
            patch.object(
                HOOK,
                "_dogpark_cluster_identity",
                return_value={
                    "cluster_name": "test-cluster",
                    "cluster_uuid": "test-cluster-uuid",
                },
            ),
            patch.object(
                HOOK,
                "_dogpark_validate_cluster",
                return_value="test-cluster",
            ),
            patch.object(
                HOOK,
                "_dogpark_load_build_documents",
                return_value=documents,
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_inventory",
                return_value=aliases,
            ),
            patch.object(HOOK, "_dogpark_get_alias_indices", return_value=set()),
            patch.object(HOOK, "_dogpark_index_exists", return_value=True),
            patch.object(
                HOOK,
                "_dogpark_index_uuid",
                return_value="uuid-alliance-prod-index",
            ),
        ):
            manifest = HOOK.plan_release_manifest(
                "http://es.invalid:9200",
                "test-cluster",
                "itrb-ci",
            )

        self.assertFalse(manifest["complete"])
        self.assertEqual([], manifest["desired_indices"])
        self.assertEqual(
            "current_prod_conflicting_release_state",
            manifest["blocked"][0]["reason"],
        )
        self.assertEqual(
            ["deprecated"],
            manifest["blocked"][0]["newest_candidate_issue"]["states"],
        )

    def test_wrong_dataset_holder_under_custom_alias_blocks_manifest(self):
        special = build_document(
            "ubergraph_nodes_info",
            "2026-10-06T17:00:00Z",
            index_name="ubergraph-nodes-info-new",
        )
        wrong_holder = build_document(
            "alliance",
            "2026-10-01T17:00:00Z",
            index_name="alliance-wrong-custom-holder",
        )

        def custom_alias_holders(alias, _uri):
            if alias == "ubergraph_nodes_mapping":
                return {"alliance-wrong-custom-holder"}
            return set()

        with (
            dataset_policy((), ("ubergraph_nodes_info",)),
            patch.object(
                HOOK,
                "_dogpark_cluster_identity",
                return_value={
                    "cluster_name": "test-cluster",
                    "cluster_uuid": "test-cluster-uuid",
                },
            ),
            patch.object(
                HOOK,
                "_dogpark_validate_cluster",
                return_value="test-cluster",
            ),
            patch.object(
                HOOK,
                "_dogpark_load_build_documents",
                return_value=[special, wrong_holder],
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_inventory",
                return_value=empty_aliases(),
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_indices",
                side_effect=custom_alias_holders,
            ),
            patch.object(HOOK, "_dogpark_index_exists", return_value=True),
            patch.object(
                HOOK,
                "_dogpark_index_uuid",
                return_value="uuid-ubergraph-nodes-info-new",
            ),
        ):
            manifest = HOOK.plan_release_manifest(
                "http://es.invalid:9200",
                "test-cluster",
                "itrb-ci",
            )

        self.assertFalse(manifest["complete"])
        self.assertEqual(1, len(manifest["blocked"]))
        self.assertEqual(
            "custom_prod_holder_ownership_conflict",
            manifest["blocked"][0]["reason"],
        )
        self.assertEqual(
            ["alliance-wrong-custom-holder"],
            manifest["blocked"][0]["newest_candidate_issue"]["holders"],
        )


class ManifestStorageTests(unittest.TestCase):
    def manifest(
        self,
        created_at,
        *,
        index_name="alliance-new-index",
        observed_staging=(),
    ):
        observed_staging = list(observed_staging)
        observed_aliases = {
            "dingo": [],
            "dingo_staging": observed_staging,
            "dingo_deprecated": [],
            "dingo_to_be_deleted": [],
        }
        precondition = {
            "cluster_name": "test-cluster",
            "cluster_uuid": "test-cluster-uuid",
            "observed_aliases": observed_aliases,
        }
        draft = {
            "schema": HOOK._DOGPARK_MANIFEST_SCHEMA,
            "created_at": created_at,
            "target": {
                "es_uri": "http://es.invalid:9200",
                "cluster_name": "test-cluster",
                "cluster_uuid": "test-cluster-uuid",
                "indexer_environment": "itrb-ci",
            },
            "policy": {
                "release_datasets": ["alliance"],
                "special_aliases": {},
            },
            "entries": [
                {
                    "config": "alliance",
                    "build_id": "alliance_20261002_newbuild",
                    "target_name": "alliance_20261002_newbuild",
                    "index": index_name,
                    "index_uuid": f"uuid-{index_name}",
                    "started_at": "2026-10-02T17:00:00Z",
                    "archived": False,
                    "hub_status": "success",
                    "registered_environment": "itrb-ci",
                    "selection": "newest_ready",
                    "decision": "promote",
                    "reason": "ready_to_stage",
                    "warnings": [],
                    "staging_alias": "dingo_staging",
                    "production_alias": "dingo",
                    "observed_prod_indices": [],
                    "observed_staging_indices": observed_staging,
                }
            ],
            "observed_aliases": observed_aliases,
            "precondition_sha256": HOOK._dogpark_sha256(
                HOOK._dogpark_canonical_json(precondition)
            ),
            "desired_indices": [index_name],
            "staging_required_indices": [index_name],
            "staging_add_indices": (
                [] if index_name in observed_staging else [index_name]
            ),
            "staging_remove_indices": [
                name for name in observed_staging if name != index_name
            ],
            "keep_indices": [],
            "promotion_indices": [index_name],
            "complete": True,
            "blocked": [],
            "counts": {
                "managed": 1,
                "desired": 1,
                "promote": 1,
                "keep_current_prod": 0,
                "blocked": 0,
                "staging_add": (
                    0 if index_name in observed_staging else 1
                ),
                "staging_remove": len(
                    [name for name in observed_staging if name != index_name]
                ),
            },
        }
        return HOOK._dogpark_finalize_manifest(draft)

    def freeze(self, directory, manifest):
        with (
            dataset_policy(("alliance",)),
            patch.object(HOOK, "_DOGPARK_MANIFESTS_DIR", directory),
            patch.object(
                HOOK,
                "plan_release_manifest",
                return_value=manifest,
            ),
            patch.object(
                HOOK,
                "_dogpark_prepare_release_manifest",
                return_value=(manifest, {}),
            ),
        ):
            return HOOK.freeze_release_manifest()

    def test_freeze_writes_unique_immutable_files_then_atomically_advances_latest(self):
        first = self.manifest("2026-10-02T17:00:00.000001Z")
        second = self.manifest(
            "2026-10-02T17:00:00.000002Z",
            index_name="alliance-newer-index",
        )

        with TemporaryDirectory() as directory, patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ), patch.object(
            HOOK,
            "_dogpark_prepare_release_manifest",
            side_effect=[(first, {}), (second, {})],
        ), patch.object(
            HOOK,
            "plan_release_manifest",
            side_effect=[first, second],
        ), patch.object(
            HOOK._dogpark_os,
            "replace",
            wraps=HOOK._dogpark_os.replace,
        ) as replace, dataset_policy(("alliance",)):
            first_result = HOOK.freeze_release_manifest()
            first_path = Path(first_result["storage"]["manifest_file"])
            if not first_path.is_absolute():
                first_path = Path(directory) / first_path
            first_bytes = first_path.read_bytes()

            second_result = HOOK.freeze_release_manifest()
            second_path = Path(second_result["storage"]["manifest_file"])
            if not second_path.is_absolute():
                second_path = Path(directory) / second_path

            self.assertNotEqual(
                first_result["storage"]["manifest_id"],
                second_result["storage"]["manifest_id"],
            )
            self.assertTrue(first_path.is_file())
            self.assertTrue(second_path.is_file())
            self.assertEqual(first_bytes, first_path.read_bytes())
            latest_path = Path(directory) / "latest-frozen.json"
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
            self.assertEqual("dogpark.release-pointer/v1", latest["schema"])
            self.assertEqual(second["manifest_id"], latest["manifest_id"])
            self.assertEqual(
                second_result["storage"]["manifest_sha256"],
                latest["manifest_sha256"],
            )
            self.assertTrue(
                any(
                    Path(call.args[1]) == latest_path
                    for call in replace.call_args_list
                )
            )
            self.assertEqual([], list(Path(directory).glob("*.tmp")))

    def test_failed_immutable_write_does_not_move_latest_pointer(self):
        manifest = self.manifest("2026-10-02T17:00:00.000001Z")
        with TemporaryDirectory() as directory, patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ), patch.object(
            HOOK,
            "_dogpark_prepare_release_manifest",
            return_value=(manifest, {}),
        ), patch.object(
            HOOK,
            "plan_release_manifest",
            return_value=manifest,
        ), dataset_policy(("alliance",)):
            HOOK.freeze_release_manifest()
            pointer_path = Path(directory) / "latest-frozen.json"
            original_pointer = pointer_path.read_bytes()
            with patch.object(
                HOOK,
                "_dogpark_write_immutable_json",
                side_effect=OSError("simulated disk failure"),
            ):
                with self.assertRaisesRegex(OSError, "disk failure"):
                    HOOK.freeze_release_manifest()
            self.assertEqual(original_pointer, pointer_path.read_bytes())

    def test_get_rejects_tampered_manifest_digest(self):
        manifest = self.manifest("2026-10-02T17:00:00.000001Z")
        with TemporaryDirectory() as directory, patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ), patch.object(
            HOOK,
            "_dogpark_prepare_release_manifest",
            return_value=(manifest, {}),
        ), patch.object(
            HOOK,
            "plan_release_manifest",
            return_value=manifest,
        ), dataset_policy(("alliance",)):
            frozen = HOOK.freeze_release_manifest()
            manifest_path = Path(directory) / frozen["storage"]["manifest_file"]
            tampered = json.loads(manifest_path.read_text(encoding="utf-8"))
            tampered["desired_indices"] = ["attacker-controlled-index"]
            manifest_path.write_text(
                json.dumps(tampered, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "integrity|checksum"):
                HOOK.get_release_manifest(manifest["manifest_id"])

    def test_get_rejects_rehashed_payload_that_no_longer_matches_manifest_id(self):
        manifest = self.manifest("2026-10-02T17:00:00.000001Z")
        with TemporaryDirectory() as directory, patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ), patch.object(
            HOOK,
            "_dogpark_prepare_release_manifest",
            return_value=(manifest, {}),
        ), patch.object(
            HOOK,
            "plan_release_manifest",
            return_value=manifest,
        ), dataset_policy(("alliance",)):
            frozen = HOOK.freeze_release_manifest()
            manifest_path = Path(directory) / frozen["storage"]["manifest_file"]
            tampered = json.loads(manifest_path.read_text(encoding="utf-8"))
            tampered["entries"][0]["index"] = "attacker-controlled-index"
            body = dict(tampered)
            body.pop("integrity")
            tampered["integrity"]["payload_sha256"] = HOOK._dogpark_sha256(
                HOOK._dogpark_canonical_json(body)
            )
            manifest_path.write_bytes(HOOK._dogpark_pretty_json(tampered))

            with self.assertRaisesRegex(RuntimeError, "manifest id|integrity"):
                HOOK.get_release_manifest(manifest["manifest_id"])

    def test_get_accepts_latest_frozen_but_rejects_path_like_ids(self):
        manifest = self.manifest("2026-10-02T17:00:00.000001Z")
        with TemporaryDirectory() as directory, patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ), patch.object(
            HOOK,
            "_dogpark_prepare_release_manifest",
            return_value=(manifest, {}),
        ), patch.object(
            HOOK,
            "plan_release_manifest",
            return_value=manifest,
        ), dataset_policy(("alliance",)):
            HOOK.freeze_release_manifest()
            self.assertEqual(manifest, HOOK.get_release_manifest())
            self.assertEqual(
                manifest,
                HOOK.get_release_manifest("latest-frozen"),
            )
            self.assertEqual(
                manifest,
                HOOK.get_release_manifest(manifest["manifest_id"]),
            )

            for unsafe_id in (
                None,
                "",
                "../outside",
                "/tmp/outside",
                "nested/manifest",
                "nested\\manifest",
                "manifest.json",
                "månifest",
            ):
                with self.subTest(unsafe_id=unsafe_id):
                    with self.assertRaises((TypeError, ValueError)):
                        HOOK.get_release_manifest(unsafe_id)

    def test_apply_uses_only_frozen_names_writes_receipt_and_retries_idempotently(self):
        manifest = self.manifest("2026-10-02T17:00:00.000001Z")
        aliases = {
            "dingo": set(),
            "dingo_staging": set(),
            "dingo_deprecated": set(),
            "dingo_to_be_deleted": set(),
        }

        def read_alias(alias, _es_uri):
            return set(aliases[alias])

        def apply_aliases(add_indices, remove_indices, _es_uri):
            aliases["dingo_staging"].difference_update(remove_indices)
            aliases["dingo_staging"].update(add_indices)
            return {"acknowledged": True}

        with TemporaryDirectory() as directory:
            frozen = self.freeze(directory, manifest)
            manifest_id = frozen["storage"]["manifest_id"]
            with (
                dataset_policy(("alliance",)),
                patch.object(HOOK, "_DOGPARK_MANIFESTS_DIR", directory),
                patch.object(
                    HOOK,
                    "_dogpark_cluster_identity",
                    return_value={
                        "cluster_name": "test-cluster",
                        "cluster_uuid": "test-cluster-uuid",
                    },
                ),
                patch.object(
                    HOOK,
                    "_dogpark_index_uuid",
                    return_value="uuid-alliance-new-index",
                ),
                patch.object(
                    HOOK,
                    "_dogpark_get_alias_indices",
                    side_effect=read_alias,
                ),
                patch.object(
                    HOOK,
                    "_dogpark_post_staging_reconciliation",
                    side_effect=apply_aliases,
                ) as post,
                patch.object(
                    HOOK,
                    "_dogpark_load_build_documents",
                    side_effect=AssertionError("must not rediscover builds"),
                ),
                patch.object(
                    HOOK,
                    "plan_release_manifest",
                    side_effect=AssertionError("must not re-plan"),
                ),
            ):
                first = HOOK.assign_staging_manifest(
                    manifest_id,
                    "http://es.invalid:9200",
                    "test-cluster",
                )
                second = HOOK.assign_staging_manifest(
                    manifest_id,
                    "http://es.invalid:9200",
                    "test-cluster",
                )
                latest_staged = HOOK.get_release_manifest("latest-staged")

            post.assert_called_once_with(
                ["alliance-new-index"],
                [],
                "http://es.invalid:9200",
            )
            self.assertTrue(first["applied"])
            self.assertEqual(["alliance-new-index"], first["tagged"])
            self.assertFalse(second["applied"])
            self.assertEqual([], second["tagged"])
            self.assertEqual("already_applied", second["receipt"]["outcome"])
            self.assertEqual(manifest, latest_staged)

            pointer_path = Path(directory) / "latest-staged.json"
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest_id, pointer["manifest_id"])
            self.assertEqual(
                second["receipt_storage"]["receipt_file"],
                pointer["receipt_file"],
            )
            self.assertEqual(
                second["receipt_storage"]["receipt_sha256"],
                pointer["receipt_sha256"],
            )
            receipt_path = Path(directory) / pointer["receipt_file"]
            self.assertTrue(receipt_path.is_file())
            self.assertEqual(
                second["receipt"],
                json.loads(receipt_path.read_text(encoding="utf-8")),
            )
            tampered_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            tampered_receipt["outcome"] = "tampered"
            receipt_path.write_text(
                json.dumps(tampered_receipt, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            with dataset_policy(("alliance",)), patch.object(
                HOOK,
                "_DOGPARK_MANIFESTS_DIR",
                directory,
            ):
                self.assertEqual(
                    manifest,
                    HOOK.get_release_manifest(manifest_id),
                )
                with self.assertRaisesRegex(RuntimeError, "receipt|checksum"):
                    HOOK.get_release_manifest("latest-staged")

    def test_alias_drift_aborts_before_mutation_or_receipt(self):
        manifest = self.manifest("2026-10-02T17:00:00.000001Z")
        aliases = {
            "dingo": {"unexpected-prod-index"},
            "dingo_staging": set(),
            "dingo_deprecated": set(),
            "dingo_to_be_deleted": set(),
        }
        with TemporaryDirectory() as directory:
            frozen = self.freeze(directory, manifest)
            with (
                dataset_policy(("alliance",)),
                patch.object(HOOK, "_DOGPARK_MANIFESTS_DIR", directory),
                patch.object(
                    HOOK,
                    "_dogpark_cluster_identity",
                    return_value={
                        "cluster_name": "test-cluster",
                        "cluster_uuid": "test-cluster-uuid",
                    },
                ),
                patch.object(
                    HOOK,
                    "_dogpark_index_uuid",
                    return_value="uuid-alliance-new-index",
                ),
                patch.object(
                    HOOK,
                    "_dogpark_get_alias_indices",
                    side_effect=lambda alias, _uri: set(aliases[alias]),
                ),
                patch.object(
                    HOOK,
                    "_dogpark_post_staging_reconciliation",
                ) as post,
            ):
                with self.assertRaisesRegex(RuntimeError, "drift"):
                    HOOK.assign_staging_manifest(
                        frozen["storage"]["manifest_id"],
                        "http://es.invalid:9200",
                        "test-cluster",
                    )

            post.assert_not_called()
            self.assertFalse((Path(directory) / "latest-staged.json").exists())
            self.assertEqual([], list(Path(directory).glob("receipts/**/*.json")))

    def test_stale_staging_holder_is_replaced_in_one_atomic_alias_request(self):
        stale_index = "alliance-stale-staging-index"
        new_index = "alliance-new-index"
        manifest = self.manifest(
            "2026-10-02T17:00:00.000001Z",
            index_name=new_index,
            observed_staging=(stale_index,),
        )
        aliases = {
            "dingo": set(),
            "dingo_staging": {stale_index},
            "dingo_deprecated": set(),
            "dingo_to_be_deleted": set(),
        }
        requests = []

        def request(url, *, method="GET", payload=None, allow_not_found=False):
            del allow_not_found
            requests.append((url, method, payload))
            for action in payload["actions"]:
                if "remove" in action:
                    aliases[action["remove"]["alias"]].discard(
                        action["remove"]["index"]
                    )
                if "add" in action:
                    aliases[action["add"]["alias"]].add(
                        action["add"]["index"]
                    )
            return {"acknowledged": True}

        with TemporaryDirectory() as directory:
            frozen = self.freeze(directory, manifest)
            with (
                dataset_policy(("alliance",)),
                patch.object(HOOK, "_DOGPARK_MANIFESTS_DIR", directory),
                patch.object(
                    HOOK,
                    "_dogpark_cluster_identity",
                    return_value={
                        "cluster_name": "test-cluster",
                        "cluster_uuid": "test-cluster-uuid",
                    },
                ),
                patch.object(
                    HOOK,
                    "_dogpark_index_uuid",
                    return_value=f"uuid-{new_index}",
                ),
                patch.object(
                    HOOK,
                    "_dogpark_get_alias_indices",
                    side_effect=lambda alias, _uri: set(aliases[alias]),
                ),
                patch.object(
                    HOOK,
                    "_dogpark_request_json",
                    side_effect=request,
                ),
            ):
                result = HOOK.assign_staging_manifest(
                    frozen["storage"]["manifest_id"],
                    "http://es.invalid:9200",
                    "test-cluster",
                )

        self.assertEqual(1, len(requests))
        url, method, payload = requests[0]
        self.assertEqual("http://es.invalid:9200/_aliases", url)
        self.assertEqual("POST", method)
        self.assertCountEqual(
            [
                {
                    "remove": {
                        "index": stale_index,
                        "alias": "dingo_staging",
                    }
                },
                {
                    "add": {
                        "index": new_index,
                        "alias": "dingo_staging",
                    }
                },
            ],
            payload["actions"],
        )
        self.assertEqual({new_index}, aliases["dingo_staging"])
        self.assertEqual([new_index], result["tagged"])
        self.assertEqual([stale_index], result["receipt"]["requested_removals"])
        self.assertEqual([stale_index], result["receipt"]["removed_this_run"])

    def test_manifest_contract_rejects_tampered_staging_removals(self):
        manifest = self.manifest(
            "2026-10-02T17:00:00.000001Z",
            observed_staging=("alliance-stale-staging-index",),
        )
        body = dict(manifest)
        body.pop("manifest_id")
        body.pop("integrity")
        body["staging_remove_indices"] = []
        tampered = HOOK._dogpark_finalize_manifest(body)

        with dataset_policy(("alliance",)):
            with self.assertRaisesRegex(RuntimeError, "staging_remove_indices"):
                HOOK._dogpark_validate_manifest_contract(tampered)

    def test_manifest_mutators_require_hub_confirmation(self):
        self.assertIs(HOOK.freeze_release_manifest.confirm, True)
        self.assertIs(HOOK.assign_staging_manifest.confirm, True)


class ProductionReleaseTests(unittest.TestCase):
    release_datasets = ("alliance", "bgee")
    special_datasets = ("ubergraph_nodes_info",)

    index_uuids = {
        "alliance-new-index": "uuid-alliance-new-index",
        "alliance-old-index": "uuid-alliance-old-index",
        "bgee-current-index": "uuid-bgee-current-index",
        "ubergraph-nodes-info-new": "uuid-ubergraph-nodes-info-new",
        "ubergraph-nodes-info-old": "uuid-ubergraph-nodes-info-old",
        "ubergraph-nodes-new": "uuid-ubergraph-nodes-new",
        "ubergraph-nodes-old": "uuid-ubergraph-nodes-old",
        "prior-deprecated-index": "uuid-prior-deprecated-index",
    }

    def manifest(
        self,
        created_at="2026-10-06T17:00:00.000001Z",
        *,
        desired_conflict_alias=None,
        current_prod_conflict_alias=None,
        observed_delete=(),
    ):
        observed_aliases = {
            "dingo": ["alliance-old-index", "bgee-current-index"],
            "dingo_staging": [],
            "dingo_deprecated": ["prior-deprecated-index"],
            "dingo_to_be_deleted": sorted(set(observed_delete)),
            "ubergraph_nodes_mapping": ["ubergraph-nodes-info-old"],
        }
        if desired_conflict_alias is not None:
            observed_aliases[desired_conflict_alias] = sorted(
                set(observed_aliases[desired_conflict_alias])
                | {"alliance-new-index"}
            )
        if current_prod_conflict_alias is not None:
            observed_aliases[current_prod_conflict_alias] = sorted(
                set(observed_aliases[current_prod_conflict_alias])
                | {"alliance-old-index"}
            )
        entries = [
            {
                "config": "alliance",
                "build_id": "alliance_20261006_newbuild",
                "target_name": "alliance_20261006_newbuild",
                "index": "alliance-new-index",
                "index_uuid": "uuid-alliance-new-index",
                "started_at": "2026-10-06T17:00:00Z",
                "archived": False,
                "hub_status": "success",
                "registered_environment": "itrb-ci",
                "selection": "newest_ready",
                "decision": "promote",
                "reason": "ready_to_stage",
                "warnings": [],
                "staging_alias": "dingo_staging",
                "production_alias": "dingo",
                "observed_prod_indices": ["alliance-old-index"],
                "observed_staging_indices": [],
            },
            {
                "config": "bgee",
                "build_id": "bgee_20261001_current",
                "target_name": "bgee_20261001_current",
                "index": "bgee-current-index",
                "index_uuid": "uuid-bgee-current-index",
                "started_at": "2026-10-01T17:00:00Z",
                "archived": False,
                "hub_status": "success",
                "registered_environment": "itrb-ci",
                "selection": "current_prod_fallback",
                "decision": "keep_current_prod",
                "reason": "fallback_to_current_prod",
                "warnings": [],
                "staging_alias": "dingo_staging",
                "production_alias": "dingo",
                "observed_prod_indices": ["bgee-current-index"],
                "observed_staging_indices": [],
            },
            {
                "config": "ubergraph_nodes_info",
                "build_id": "ubergraph_nodes_info_20261006_newbuild",
                "target_name": "ubergraph_nodes_info_20261006_newbuild",
                "index": "ubergraph-nodes-info-new",
                "index_uuid": "uuid-ubergraph-nodes-info-new",
                "started_at": "2026-10-06T17:00:00Z",
                "archived": False,
                "hub_status": "success",
                "registered_environment": "itrb-ci",
                "selection": "newest_ready",
                "decision": "promote",
                "reason": "ready_without_staging_alias",
                "warnings": [],
                "staging_alias": None,
                "production_alias": "ubergraph_nodes_mapping",
                "observed_prod_indices": ["ubergraph-nodes-info-old"],
                "observed_staging_indices": [],
            },
        ]
        precondition = {
            "cluster_name": "test-cluster",
            "cluster_uuid": "test-cluster-uuid",
            "observed_aliases": observed_aliases,
        }
        draft = {
            "schema": HOOK._DOGPARK_MANIFEST_SCHEMA,
            "created_at": created_at,
            "source": {
                "command": "plan_release_manifest",
                "discovery": "src_build",
                "staging_plan_generated_at": created_at,
            },
            "target": {
                "es_uri": "http://es.invalid:9200",
                "cluster_name": "test-cluster",
                "cluster_uuid": "test-cluster-uuid",
                "indexer_environment": "itrb-ci",
            },
            "policy": {
                "managed_datasets": [
                    "alliance",
                    "bgee",
                    "ubergraph_nodes_info",
                ],
                "default_aliases": {
                    "staging_alias": "dingo_staging",
                    "production_alias": "dingo",
                    "deprecated_alias": "dingo_deprecated",
                    "delete_alias": "dingo_to_be_deleted",
                },
                "special_aliases": {
                    "ubergraph_nodes_info": {
                        "staging_alias": None,
                        "production_alias": "ubergraph_nodes_mapping",
                    }
                },
            },
            "entries": entries,
            "observed_aliases": observed_aliases,
            "precondition_sha256": HOOK._dogpark_sha256(
                HOOK._dogpark_canonical_json(precondition)
            ),
            "desired_indices": [
                "alliance-new-index",
                "bgee-current-index",
                "ubergraph-nodes-info-new",
            ],
            "staging_required_indices": ["alliance-new-index"],
            "staging_add_indices": ["alliance-new-index"],
            "staging_remove_indices": [],
            "keep_indices": ["bgee-current-index"],
            "promotion_indices": [
                "alliance-new-index",
                "ubergraph-nodes-info-new",
            ],
            "blocked": [],
            "complete": True,
            "counts": {
                "managed": 3,
                "desired": 3,
                "promote": 2,
                "keep_current_prod": 1,
                "blocked": 0,
                "staging_add": 1,
                "staging_remove": 0,
            },
        }
        return HOOK._dogpark_finalize_manifest(draft)

    def no_op_manifest(self):
        manifest = self.manifest(
            observed_delete=("prior-deprecated-index",),
        )
        draft = json.loads(json.dumps(manifest))
        draft.pop("manifest_id")
        draft.pop("integrity")
        alliance, bgee, nodes_info = draft["entries"]
        alliance.update(
            {
                "build_id": "alliance_20261001_current",
                "target_name": "alliance_20261001_current",
                "index": "alliance-old-index",
                "index_uuid": "uuid-alliance-old-index",
                "started_at": "2026-10-01T17:00:00Z",
                "selection": "current_prod_fallback",
                "decision": "keep_current_prod",
                "reason": "fallback_to_current_prod",
            }
        )
        nodes_info.update(
            {
                "build_id": "ubergraph_nodes_info_20261001_current",
                "target_name": "ubergraph_nodes_info_20261001_current",
                "index": "ubergraph-nodes-info-old",
                "index_uuid": "uuid-ubergraph-nodes-info-old",
                "started_at": "2026-10-01T17:00:00Z",
                "selection": "current_prod_fallback",
                "decision": "keep_current_prod",
                "reason": "fallback_to_current_prod",
            }
        )
        draft["desired_indices"] = [
            alliance["index"],
            bgee["index"],
            nodes_info["index"],
        ]
        draft["staging_required_indices"] = []
        draft["staging_add_indices"] = []
        draft["staging_remove_indices"] = []
        draft["keep_indices"] = list(draft["desired_indices"])
        draft["promotion_indices"] = []
        draft["counts"].update(
            {
                "promote": 0,
                "keep_current_prod": 3,
                "staging_add": 0,
                "staging_remove": 0,
            }
        )
        return HOOK._dogpark_finalize_manifest(draft)

    def both_special_manifest(self):
        manifest = self.manifest()
        draft = json.loads(json.dumps(manifest))
        draft.pop("manifest_id")
        draft.pop("integrity")
        draft["entries"].append(
            {
                "config": "ubergraph_nodes",
                "build_id": "ubergraph_nodes_20261006_newbuild",
                "target_name": "ubergraph_nodes_20261006_newbuild",
                "index": "ubergraph-nodes-new",
                "index_uuid": "uuid-ubergraph-nodes-new",
                "started_at": "2026-10-06T17:00:00Z",
                "archived": False,
                "hub_status": "success",
                "registered_environment": "itrb-ci",
                "selection": "newest_ready",
                "decision": "promote",
                "reason": "ready_without_staging_alias",
                "warnings": [],
                "staging_alias": None,
                "production_alias": "ubergraph_nodes",
                "observed_prod_indices": ["ubergraph-nodes-old"],
                "observed_staging_indices": [],
            }
        )
        draft["policy"]["managed_datasets"].append("ubergraph_nodes")
        draft["policy"]["special_aliases"]["ubergraph_nodes"] = {
            "staging_alias": None,
            "production_alias": "ubergraph_nodes",
        }
        draft["observed_aliases"]["ubergraph_nodes"] = [
            "ubergraph-nodes-old"
        ]
        draft["desired_indices"].append("ubergraph-nodes-new")
        draft["promotion_indices"].append("ubergraph-nodes-new")
        draft["counts"]["managed"] += 1
        draft["counts"]["desired"] += 1
        draft["counts"]["promote"] += 1
        precondition = {
            "cluster_name": draft["target"]["cluster_name"],
            "cluster_uuid": draft["target"]["cluster_uuid"],
            "observed_aliases": draft["observed_aliases"],
        }
        draft["precondition_sha256"] = HOOK._dogpark_sha256(
            HOOK._dogpark_canonical_json(precondition)
        )
        return HOOK._dogpark_finalize_manifest(draft)

    def persist_staged(self, manifest):
        manifest_storage = HOOK._dogpark_persist_manifest(manifest)
        verified_aliases = HOOK._dogpark_expected_post_staging_aliases(manifest)
        receipt = HOOK._dogpark_finalize_receipt(
            {
                "schema": HOOK._DOGPARK_STAGING_RECEIPT_SCHEMA,
                "verified_at": "2026-10-06T17:01:00.000001Z",
                "manifest_id": manifest["manifest_id"],
                "manifest_payload_sha256": manifest["integrity"][
                    "payload_sha256"
                ],
                "target": {
                    "es_uri": manifest["target"]["es_uri"],
                    "cluster_name": manifest["target"]["cluster_name"],
                    "cluster_uuid": manifest["target"]["cluster_uuid"],
                },
                "outcome": (
                    "aliases_reconciled"
                    if manifest["staging_add_indices"]
                    or manifest["staging_remove_indices"]
                    else "no_aliases_required"
                ),
                "requested_additions": manifest["staging_add_indices"],
                "requested_removals": manifest["staging_remove_indices"],
                "tagged_this_run": manifest["staging_add_indices"],
                "removed_this_run": manifest["staging_remove_indices"],
                "required_staging_indices": manifest[
                    "staging_required_indices"
                ],
                "verified_aliases": verified_aliases,
            }
        )
        receipt_storage = HOOK._dogpark_persist_staging_receipt(
            manifest,
            manifest_storage,
            receipt,
        )
        return manifest_storage, receipt, receipt_storage

    def runtime_patches(self, aliases, *, index_uuids=None):
        identities = self.index_uuids if index_uuids is None else index_uuids
        return (
            patch.object(
                HOOK,
                "_dogpark_cluster_identity",
                return_value={
                    "cluster_name": "test-cluster",
                    "cluster_uuid": "test-cluster-uuid",
                },
            ),
            patch.object(
                HOOK,
                "_dogpark_index_uuid",
                side_effect=lambda name, _uri: identities.get(name),
            ),
            patch.object(
                HOOK,
                "_dogpark_get_alias_indices",
                side_effect=lambda alias, _uri: set(aliases.get(alias, set())),
            ),
            patch.object(
                HOOK,
                "_dogpark_load_build_documents",
                side_effect=AssertionError("production must not rediscover builds"),
            ),
            patch.object(
                HOOK,
                "plan_release_manifest",
                side_effect=AssertionError("production must not re-plan builds"),
            ),
        )

    def apply_payload(self, aliases, payload):
        for action in payload["actions"]:
            operation, detail = next(iter(action.items()))
            if operation == "add":
                aliases.setdefault(detail["alias"], set()).add(detail["index"])
            elif operation == "remove":
                aliases.setdefault(detail["alias"], set()).discard(
                    detail["index"]
                )

    def test_plan_reconciles_promote_keep_deprecate_and_special_aliases(self):
        manifest = self.manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with cluster, index_uuid, get_alias, no_builds, no_replan:
                plan = HOOK.plan_prod_release(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                )

        self.assertEqual("dogpark.production-plan/v1", plan["schema"])
        self.assertEqual("ready_to_apply", plan["state"])
        self.assertEqual(
            ["alliance-new-index", "ubergraph-nodes-info-new"],
            plan["promote_indices"],
        )
        self.assertEqual(["bgee-current-index"], plan["keep_indices"])
        self.assertEqual(
            ["alliance-old-index", "ubergraph-nodes-info-old"],
            plan["deprecate_indices"],
        )
        self.assertEqual(
            ["prior-deprecated-index"],
            plan["old_deprecated_to_delete"],
        )
        self.assertEqual(
            ["alliance-new-index"],
            plan["staging_clear_indices"],
        )
        self.assertEqual(
            ["alliance-new-index", "bgee-current-index"],
            plan["expected_after_aliases"]["dingo"],
        )
        self.assertEqual(
            ["ubergraph-nodes-info-new"],
            plan["expected_after_aliases"]["ubergraph_nodes_mapping"],
        )
        self.assertEqual([], plan["expected_after_aliases"]["dingo_staging"])
        self.assertEqual(
            {
                "alliance-old-index",
                "prior-deprecated-index",
                "ubergraph-nodes-info-old",
            },
            set(plan["expected_after_aliases"]["dingo_deprecated"]),
        )
        self.assertEqual(
            ["prior-deprecated-index"],
            plan["expected_after_aliases"]["dingo_to_be_deleted"],
        )
        self.assertEqual(
            {
                name: self.index_uuids[name]
                for name in (
                    "alliance-new-index",
                    "alliance-old-index",
                    "prior-deprecated-index",
                    "ubergraph-nodes-info-new",
                    "ubergraph-nodes-info-old",
                )
            },
            plan["affected_index_uuids"],
        )
        action_keys = [
            (operation, detail["index"], detail["alias"])
            for action in plan["actions"]
            for operation, detail in action.items()
        ]
        self.assertEqual(len(action_keys), len(set(action_keys)))
        self.assertFalse(
            any(
                ("remove" if operation == "add" else "add", index, alias)
                in action_keys
                for operation, index, alias in action_keys
            )
        )

    def test_plan_reconciles_both_special_production_aliases(self):
        manifest = self.both_special_manifest()
        both_special = ("ubergraph_nodes_info", "ubergraph_nodes")
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            both_special,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with cluster, index_uuid, get_alias, no_builds, no_replan:
                plan = HOOK.plan_prod_release(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                )

        self.assertEqual(
            ["ubergraph-nodes-info-new"],
            plan["expected_after_aliases"]["ubergraph_nodes_mapping"],
        )
        self.assertEqual(
            ["ubergraph-nodes-new"],
            plan["expected_after_aliases"]["ubergraph_nodes"],
        )
        self.assertIn("ubergraph-nodes-info-old", plan["deprecate_indices"])
        self.assertIn("ubergraph-nodes-old", plan["deprecate_indices"])
        self.assertIn(
            {
                "add": {
                    "index": "ubergraph-nodes-new",
                    "alias": "ubergraph_nodes",
                }
            },
            plan["actions"],
        )
        self.assertIn(
            {
                "remove": {
                    "index": "ubergraph-nodes-old",
                    "alias": "ubergraph_nodes",
                }
            },
            plan["actions"],
        )

    def test_desired_uuid_mismatch_aborts_before_mutation(self):
        manifest = self.manifest()
        wrong_uuids = dict(self.index_uuids)
        wrong_uuids["alliance-new-index"] = "replacement-index-uuid"
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases, index_uuids=wrong_uuids)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(HOOK, "_dogpark_request_json") as request,
            ):
                with self.assertRaisesRegex(RuntimeError, "identity|UUID"):
                    HOOK.release_prod_manifest(
                        es_uri="http://es.invalid:9200",
                        expected_cluster="test-cluster",
                    )

            request.assert_not_called()
            self.assertFalse((Path(directory) / "latest-prod.json").exists())

    def test_disabling_deprecated_aging_preserves_delete_alias(self):
        manifest = self.manifest(observed_delete=("prior-delete-index",))
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with cluster, index_uuid, get_alias, no_builds, no_replan:
                plan = HOOK.plan_prod_release(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                    mark_old_deprecated_for_deletion=False,
                )

        self.assertEqual([], plan["old_deprecated_to_delete"])
        self.assertEqual(
            ["prior-delete-index"],
            plan["expected_after_aliases"]["dingo_to_be_deleted"],
        )
        self.assertFalse(
            any(
                detail["alias"] == "dingo_to_be_deleted"
                for action in plan["actions"]
                for detail in action.values()
            )
        )

    def test_exact_noop_writes_no_aliases_required_receipt_without_post(self):
        manifest = self.no_op_manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(HOOK, "_dogpark_request_json") as request,
            ):
                result = HOOK.release_prod_manifest(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                )
                latest_prod = HOOK.get_release_manifest("latest-prod")

            request.assert_not_called()
            self.assertFalse(result["applied"])
            self.assertEqual("no_changes_required", result["plan"]["state"])
            self.assertEqual([], result["plan"]["actions"])
            self.assertEqual(
                "no_aliases_required",
                result["receipt"]["outcome"],
            )
            self.assertEqual({}, result["receipt"]["affected_index_uuids"])
            self.assertEqual(manifest, latest_prod)

    def test_current_prod_holder_in_deprecated_or_delete_state_is_rejected(self):
        for conflicting_alias in (
            "dingo_deprecated",
            "dingo_to_be_deleted",
        ):
            with self.subTest(conflicting_alias=conflicting_alias):
                manifest = self.manifest(
                    current_prod_conflict_alias=conflicting_alias,
                )
                with TemporaryDirectory() as directory, dataset_policy(
                    self.release_datasets,
                    self.special_datasets,
                ), patch.object(
                    HOOK,
                    "_DOGPARK_MANIFESTS_DIR",
                    directory,
                ):
                    self.persist_staged(manifest)
                    aliases = {
                        alias: set(indices)
                        for alias, indices in (
                            HOOK._dogpark_expected_post_staging_aliases(
                                manifest
                            ).items()
                        )
                    }
                    cluster, index_uuid, get_alias, no_builds, no_replan = (
                        self.runtime_patches(aliases)
                    )
                    with cluster, index_uuid, get_alias, no_builds, no_replan:
                        with self.assertRaisesRegex(
                            RuntimeError,
                            "production|deprecated|delete|conflict",
                        ):
                            HOOK.plan_prod_release(
                                es_uri="http://es.invalid:9200",
                                expected_cluster="test-cluster",
                            )

    def test_release_is_one_atomic_request_and_retry_is_idempotent(self):
        manifest = self.manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            requests = []

            def request(url, *, method="GET", payload=None, allow_not_found=False):
                del allow_not_found
                requests.append((url, method, payload))
                self.apply_payload(aliases, payload)
                return {"acknowledged": True}

            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(
                    HOOK,
                    "_dogpark_request_json",
                    side_effect=request,
                ),
            ):
                first = HOOK.release_prod_manifest(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                )
                second = HOOK.release_prod_manifest(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                )
                latest_prod = HOOK.get_release_manifest("latest-prod")

            self.assertEqual(1, len(requests))
            url, method, payload = requests[0]
            self.assertEqual("http://es.invalid:9200/_aliases", url)
            self.assertEqual("POST", method)
            self.assertEqual(first["plan"]["actions"], payload["actions"])
            self.assertEqual(8, len(payload["actions"]))
            self.assertTrue(first["applied"])
            self.assertEqual("aliases_reconciled", first["receipt"]["outcome"])
            self.assertEqual(
                first["plan"]["affected_index_uuids"],
                first["receipt"]["affected_index_uuids"],
            )
            self.assertFalse(second["applied"])
            self.assertEqual("already_applied", second["receipt"]["outcome"])
            self.assertEqual(manifest, latest_prod)

            pointer_path = Path(directory) / "latest-prod.json"
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["manifest_id"], pointer["manifest_id"])
            self.assertEqual(
                second["receipt_storage"]["receipt_id"],
                pointer["receipt_id"],
            )
            self.assertEqual(
                manifest["integrity"]["payload_sha256"],
                second["receipt"]["manifest_payload_sha256"],
            )
            self.assertEqual(
                second["plan"]["staging_receipt"],
                second["receipt"]["staging_receipt"],
            )
            self.assertEqual(
                second["receipt_storage"]["receipt_sha256"],
                pointer["receipt_sha256"],
            )
            self.assertEqual(
                second["receipt"]["staging_receipt"]["receipt_id"],
                pointer["staging_receipt_id"],
            )
            self.assertEqual(
                second["receipt"]["staging_receipt"]["receipt_sha256"],
                pointer["staging_receipt_sha256"],
            )
            receipt_path = Path(directory) / pointer["receipt_file"]
            self.assertTrue(receipt_path.is_file())
            original_pointer = pointer_path.read_bytes()
            tampered_pointer = dict(pointer)
            tampered_pointer["transition_sha256"] = "0" * 64
            pointer_path.write_text(
                json.dumps(tampered_pointer, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "pointer|transition"):
                HOOK.get_release_manifest("latest-prod")
            pointer_path.write_bytes(original_pointer)

            tampered = json.loads(receipt_path.read_text(encoding="utf-8"))
            tampered["outcome"] = "tampered"
            receipt_path.write_text(
                json.dumps(tampered, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "receipt|checksum|integrity"):
                HOOK.get_release_manifest("latest-prod")

    def test_retry_recovers_when_post_raises_after_aliases_were_applied(self):
        manifest = self.manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            requests = []

            def request(url, *, method="GET", payload=None, allow_not_found=False):
                del allow_not_found
                requests.append((url, method, payload))
                self.apply_payload(aliases, payload)
                raise OSError("simulated lost response after Elasticsearch applied")

            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(
                    HOOK,
                    "_dogpark_request_json",
                    side_effect=request,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "production|alias"):
                    HOOK.release_prod_manifest(
                        es_uri="http://es.invalid:9200",
                        expected_cluster="test-cluster",
                    )
                self.assertFalse((Path(directory) / "latest-prod.json").exists())

                recovered = HOOK.release_prod_manifest(
                    es_uri="http://es.invalid:9200",
                    expected_cluster="test-cluster",
                )

            self.assertEqual(1, len(requests))
            self.assertFalse(recovered["applied"])
            self.assertEqual("already_applied", recovered["receipt"]["outcome"])
            self.assertTrue((Path(directory) / "latest-prod.json").is_file())

    def test_failed_alias_readback_does_not_publish_prod_pointer(self):
        manifest = self.manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            existing_receipts = set(Path(directory).glob("receipts/**/*.json"))
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(
                    HOOK,
                    "_dogpark_request_json",
                    return_value={"acknowledged": True},
                ) as request,
            ):
                with self.assertRaisesRegex(RuntimeError, "readback|match"):
                    HOOK.release_prod_manifest(
                        es_uri="http://es.invalid:9200",
                        expected_cluster="test-cluster",
                    )

            request.assert_called_once()
            self.assertFalse((Path(directory) / "latest-prod.json").exists())
            self.assertEqual(
                existing_receipts,
                set(Path(directory).glob("receipts/**/*.json")),
            )

    def test_missing_required_staging_alias_aborts_before_mutation(self):
        manifest = self.manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            aliases["dingo_staging"].discard("alliance-new-index")
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(HOOK, "_dogpark_request_json") as request,
            ):
                with self.assertRaisesRegex(RuntimeError, "staging|drift"):
                    HOOK.release_prod_manifest(
                        es_uri="http://es.invalid:9200",
                        expected_cluster="test-cluster",
                    )

            request.assert_not_called()
            self.assertFalse((Path(directory) / "latest-prod.json").exists())

    def test_unrelated_alias_drift_aborts_without_receipt(self):
        manifest = self.manifest()
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(manifest)
            existing_receipts = set(Path(directory).glob("receipts/**/*.json"))
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            aliases["dingo"].add("unexpected-prod-index")
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with (
                cluster,
                index_uuid,
                get_alias,
                no_builds,
                no_replan,
                patch.object(HOOK, "_dogpark_request_json") as request,
            ):
                with self.assertRaisesRegex(RuntimeError, "drift"):
                    HOOK.release_prod_manifest(
                        es_uri="http://es.invalid:9200",
                        expected_cluster="test-cluster",
                    )

            request.assert_not_called()
            self.assertFalse((Path(directory) / "latest-prod.json").exists())
            self.assertEqual(
                existing_receipts,
                set(Path(directory).glob("receipts/**/*.json")),
            )

    def test_desired_index_in_deprecated_state_is_rejected(self):
        manifest = self.manifest(desired_conflict_alias="dingo_deprecated")
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            aliases = {
                alias: set(indices)
                for alias, indices in HOOK._dogpark_expected_post_staging_aliases(
                    manifest
                ).items()
            }
            cluster, index_uuid, get_alias, no_builds, no_replan = (
                self.runtime_patches(aliases)
            )
            with cluster, index_uuid, get_alias, no_builds, no_replan:
                with self.assertRaisesRegex(RuntimeError, "deprecated|conflict"):
                    self.persist_staged(manifest)
                    HOOK.plan_prod_release(
                        es_uri="http://es.invalid:9200",
                        expected_cluster="test-cluster",
                    )

    def test_exact_id_must_be_the_current_staged_manifest(self):
        staged = self.manifest()
        unstaged = self.manifest("2026-10-06T17:02:00.000001Z")
        with TemporaryDirectory() as directory, dataset_policy(
            self.release_datasets,
            self.special_datasets,
        ), patch.object(
            HOOK,
            "_DOGPARK_MANIFESTS_DIR",
            directory,
        ):
            self.persist_staged(staged)
            HOOK._dogpark_persist_manifest(unstaged)
            with self.assertRaisesRegex(RuntimeError, "latest-staged|staged"):
                HOOK.plan_prod_release(
                    unstaged["manifest_id"],
                    "http://es.invalid:9200",
                    "test-cluster",
                )

    def test_production_commands_are_exported_and_mutation_is_confirmed(self):
        self.assertIn("plan_prod_release", HOOK.__all__)
        self.assertIn("release_prod_manifest", HOOK.__all__)
        self.assertIs(HOOK.release_prod_manifest.confirm, True)


if __name__ == "__main__":
    unittest.main()

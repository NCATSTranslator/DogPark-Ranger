"""Stateful tests for alias-only staging, promotion, and restore preconditions."""

import copy
import unittest
from unittest.mock import Mock

from hooks.dogpark_hook_utils import aliases


def index(source, version):
    return f"{source}_20261105_{version}"


class FakeElasticsearch:
    def __init__(self, entries=None):
        self.cluster_uuid = "cluster-1"
        self.entries = {}
        self.posts = []
        self.gets = []
        self.response = {"acknowledged": True, "errors": False}
        self.raise_after_apply = False
        self.before_get = None
        self.health_overrides = {}
        self.cluster = Mock()
        self.cluster.health.side_effect = self.get_health
        self.indices = Mock()
        self.indices.get.side_effect = self.get_indices
        self.indices.update_aliases.side_effect = self.update_aliases
        for name, alias_names in (entries or {}).items():
            self.add(name, alias_names)

    def add(self, name, alias_names=()):
        self.entries[name] = {"uuid": f"uuid-{name}", "aliases": set(alias_names)}

    def info(self):
        return {"cluster_uuid": self.cluster_uuid}

    def get_health(self, *, index, level):
        return {
            "indices": {
                name: self.health_overrides.get(
                    name, {"status": "yellow", "number_of_shards": 2, "active_primary_shards": 2}
                )
                for name in index
                if name in self.entries
            }
        }

    def get_indices(self, **kwargs):
        self.gets.append(kwargs)
        if self.before_get:
            self.before_get(len(self.gets))
        requested = set(kwargs["index"])
        return {
            name: {"settings": {"index": {"uuid": info["uuid"]}}, "aliases": {alias: {} for alias in info["aliases"]}}
            for name, info in self.entries.items()
            if name in requested or requested & info["aliases"]
        }

    def update_aliases(self, *, actions):
        self.posts.append(copy.deepcopy(actions))
        for action in actions:
            operation, details = next(iter(action.items()))
            if operation == "remove":
                assert details["must_exist"] is True
                if details["alias"] not in self.entries[details["index"]]["aliases"]:
                    raise RuntimeError("Missing alias")
        if self.response.get("errors"):
            return self.response
        for action in actions:
            operation, details = next(iter(action.items()))
            names = self.entries[details["index"]]["aliases"]
            if operation == "add":
                names.add(details["alias"])
            else:
                names.remove(details["alias"])
        if self.raise_after_apply:
            raise TimeoutError("Response lost after application")
        return self.response


class AliasPolicyTests(unittest.TestCase):
    def test_complete_build_names_and_custom_source_overrides(self):
        self.assertEqual("drug_rep_hub", aliases.resolve_dataset(index("drug_rep_hub", "abc123")))
        self.assertEqual("alliance", aliases.resolve_dataset("alliance"))
        self.assertEqual("alliance", aliases.resolve_dataset("custom-index", "alliance"))
        for name in (
            "alliance_backup",
            "alliance_20261301_abc",
            "alliance_20261105_abc_extra",
            "alliance_20261105",
            "foreign_20261105_abc",
        ):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Cannot resolve datasource"):
                aliases.resolve_dataset(name)
        with self.assertRaisesRegex(ValueError, "conflicts"):
            aliases.resolve_dataset(index("alliance", "abc"), "bgee")

    def test_special_aliases_have_separate_staging_and_production(self):
        self.assertEqual(
            {"staging_alias": "dingo_staging", "production_alias": "dingo"}, aliases.alias_policy("alliance")
        )
        self.assertEqual(
            {"staging_alias": "ubergraph_nodes_staging", "production_alias": "ubergraph_nodes"},
            aliases.alias_policy("ubergraph_nodes"),
        )
        self.assertEqual(
            {"staging_alias": "ubergraph_nodes_mapping_staging", "production_alias": "ubergraph_nodes_mapping"},
            aliases.alias_policy("ubergraph_nodes_info"),
        )

    def test_exact_batch_validation(self):
        for names in (
            "alliance",
            None,
            {"alliance": "bgee"},
            ["*"],
            ["a,b"],
            ["alliance/abc"],
            [" alliance"],
            [""],
            ["alliance", "alliance"],
        ):
            with self.subTest(names=names), self.assertRaises((TypeError, ValueError)):
                aliases.validate_index_names(names)

    def test_concrete_name_cannot_reserve_its_future_production_alias(self):
        client = FakeElasticsearch({"ubergraph_nodes": []})
        with self.assertRaisesRegex(ValueError, "conflicts with a managed alias"):
            aliases.stage_indices(client, ["ubergraph_nodes"])
        self.assertFalse(client.posts)


class StagingTests(unittest.TestCase):
    def test_replaces_staging_and_deprecates_old_in_one_request(self):
        old, new, live = [index("alliance", suffix) for suffix in ("old", "new", "live")]
        client = FakeElasticsearch({old: ["dingo_staging"], new: [], live: ["dingo"]})
        result = aliases.stage_indices(client, [new])
        self.assertTrue(result["verified"])
        self.assertTrue(result["applied"])
        self.assertEqual(1, len(client.posts))
        self.assertEqual({"dingo_deprecated"}, client.entries[old]["aliases"])
        self.assertEqual({"dingo_staging"}, client.entries[new]["aliases"])
        self.assertEqual({"dingo"}, client.entries[live]["aliases"])
        self.assertEqual(3, len(client.posts[0]))
        self.assertNotIn("*", client.gets[0]["index"])

    def test_displaced_staging_that_is_live_is_not_deprecated(self):
        old, new = index("alliance", "old"), index("alliance", "new")
        client = FakeElasticsearch({old: ["dingo_staging", "dingo"], new: []})
        aliases.stage_indices(client, [new])
        self.assertEqual({"dingo"}, client.entries[old]["aliases"])

    def test_dry_run_before_restore_previews_missing_concrete_target(self):
        old, new = index("alliance", "old"), index("alliance", "new")
        client = FakeElasticsearch({old: ["dingo_staging"]})
        expected = aliases.stage_indices(client, [new], dry_run=True)["staging_state"]
        result = aliases.stage_indices(client, [new], expected_staging=expected, dry_run=True)
        self.assertEqual([new], result["missing_indices"])
        self.assertEqual(3, len(result["actions"]))
        self.assertFalse(result["verified"])
        self.assertFalse(client.posts)
        with self.assertRaisesRegex(RuntimeError, "missing"):
            aliases.stage_indices(client, [new], expected_staging=expected)
        self.assertFalse(client.posts)

    def test_late_restore_cannot_replace_newer_staging(self):
        old, new, concurrent = [index("alliance", suffix) for suffix in ("old", "new", "concurrent")]
        client = FakeElasticsearch({old: ["dingo_staging"]})
        expected = aliases.stage_indices(client, [new], dry_run=True)["staging_state"]
        client.entries[old]["aliases"].clear()
        client.add(concurrent, ["dingo_staging"])
        client.add(new)
        with self.assertRaisesRegex(RuntimeError, "Staging changed"):
            aliases.stage_indices(client, [new], expected_staging=expected)
        self.assertFalse(client.posts)

    def test_restore_precondition_includes_cluster_and_staged_index_uuid(self):
        old, new = index("alliance", "old"), index("alliance", "new")
        for change in ("cluster", "uuid"):
            client = FakeElasticsearch({old: ["dingo_staging"], new: []})
            expected = aliases.stage_indices(client, [new], dry_run=True)["staging_state"]
            if change == "cluster":
                client.cluster_uuid = "replacement-cluster"
            else:
                client.entries[old]["uuid"] = "replacement-index"
            with self.subTest(change=change), self.assertRaisesRegex(RuntimeError, "Staging changed"):
                aliases.stage_indices(client, [new], expected_staging=expected)
            self.assertFalse(client.posts)

    def test_unrelated_staging_change_does_not_invalidate_restore(self):
        old, new = index("alliance", "old"), index("alliance", "new")
        client = FakeElasticsearch({old: ["dingo_staging"]})
        expected = aliases.stage_indices(client, [new], dry_run=True)["staging_state"]
        other = index("bgee", "other")
        client.add(other, ["dingo_staging"])
        client.add(new)
        aliases.stage_indices(client, [new], expected_staging=expected)
        self.assertEqual({"dingo_staging"}, client.entries[other]["aliases"])

    def test_stage_retry_and_already_live_are_safe_noops(self):
        name = index("alliance", "new")
        for role in ("dingo_staging", "dingo"):
            client = FakeElasticsearch({name: [role]})
            result = aliases.stage_indices(client, [name])
            self.assertTrue(result["verified"])
            self.assertFalse(result["applied"])
            self.assertFalse(client.posts)

    def test_deprecated_delete_and_multiple_holders_are_rejected(self):
        name, old = index("alliance", "new"), index("alliance", "old")
        for role in ("dingo_deprecated", "dingo_to_be_deleted"):
            client = FakeElasticsearch({name: [role]})
            with self.assertRaisesRegex(RuntimeError, "deprecated/delete"):
                aliases.stage_indices(client, [name])
            self.assertFalse(client.posts)
        client = FakeElasticsearch({name: ["dingo_staging"], old: ["dingo_staging"]})
        with self.assertRaisesRegex(RuntimeError, "multiple staging"):
            aliases.stage_indices(client, [name])
        self.assertFalse(client.posts)


class PromotionTests(unittest.TestCase):
    def test_partial_release_preserves_unselected_live_and_staging(self):
        old, new = index("alliance", "old"), index("alliance", "new")
        bgee_old, bgee_new = index("bgee", "old"), index("bgee", "new")
        old_deprecated = index("ctd", "retired")
        client = FakeElasticsearch(
            {
                old: ["dingo"],
                new: ["dingo_staging"],
                bgee_old: ["dingo"],
                bgee_new: ["dingo_staging"],
                old_deprecated: ["dingo_deprecated"],
            }
        )
        result = aliases.release_indices(client, [new])
        self.assertTrue(result["verified"])
        self.assertEqual(1, len(client.posts))
        self.assertEqual({"dingo"}, client.entries[new]["aliases"])
        self.assertEqual({"dingo_deprecated"}, client.entries[old]["aliases"])
        self.assertEqual({"dingo"}, client.entries[bgee_old]["aliases"])
        self.assertEqual({"dingo_staging"}, client.entries[bgee_new]["aliases"])
        self.assertEqual({"dingo_deprecated"}, client.entries[old_deprecated]["aliases"])
        self.assertNotIn("dingo_to_be_deleted", str(result["actions"]))

    def test_default_release_includes_both_special_policies(self):
        names = [index(source, "new") for source in ("alliance", "ubergraph_nodes", "ubergraph_nodes_info")]
        client = FakeElasticsearch(
            {name: [aliases.alias_policy(aliases.resolve_dataset(name))["staging_alias"]] for name in names}
        )
        result = aliases.release_indices(client)
        self.assertEqual(sorted(names), result["indices"])
        self.assertEqual(1, len(client.posts))
        for name in names:
            self.assertEqual(
                {aliases.alias_policy(aliases.resolve_dataset(name))["production_alias"]},
                client.entries[name]["aliases"],
            )

    def test_explicit_live_retry_does_not_release_newer_staging(self):
        live, newer = index("alliance", "live"), index("alliance", "newer")
        client = FakeElasticsearch({live: ["dingo"], newer: ["dingo_staging"]})
        result = aliases.release_indices(client, [live])
        self.assertEqual([live], result["already_live"])
        self.assertFalse(client.posts)
        self.assertEqual({"dingo_staging"}, client.entries[newer]["aliases"])

    def test_replaced_selection_rejected_instead_of_promoting_newer(self):
        old, newer = index("alliance", "old"), index("alliance", "newer")
        client = FakeElasticsearch({old: ["dingo_deprecated"], newer: ["dingo_staging"]})
        with self.assertRaisesRegex(RuntimeError, "not the current staging"):
            aliases.release_indices(client, [old])
        self.assertFalse(client.posts)

    def test_custom_names_need_explicit_ownership_for_every_operation(self):
        client = FakeElasticsearch({"custom-build": []})
        mapping = {"custom-build": "alliance"}
        aliases.stage_indices(client, ["custom-build"], sources=mapping)
        with self.assertRaisesRegex(ValueError, "supply sources"):
            aliases.release_indices(client)
        aliases.release_indices(client, sources=mapping)
        self.assertEqual({"dingo"}, client.entries["custom-build"]["aliases"])

    def test_wrong_managed_alias_and_duplicate_dataset_batch_are_rejected(self):
        name, other = index("alliance", "new"), index("alliance", "other")
        client = FakeElasticsearch({name: ["ubergraph_nodes_staging"]})
        with self.assertRaisesRegex(RuntimeError, "another datasource policy"):
            aliases.release_indices(client)
        client = FakeElasticsearch({name: ["dingo_staging"], other: ["dingo_staging"]})
        with self.assertRaises((RuntimeError, ValueError)):
            aliases.release_indices(client)
        self.assertFalse(client.posts)

    def test_dry_run_and_empty_staging_do_not_mutate(self):
        name = index("alliance", "new")
        client = FakeElasticsearch({name: ["dingo_staging"]})
        result = aliases.release_indices(client, dry_run=True)
        self.assertTrue(result["actions"])
        self.assertFalse(client.posts)
        client = FakeElasticsearch({name: ["dingo"]})
        self.assertEqual([], aliases.release_indices(client)["indices"])
        self.assertFalse(client.posts)


class MutationVerificationTests(unittest.TestCase):
    def test_unrecovered_or_unassigned_primaries_cannot_stage_or_release(self):
        name = index("alliance", "new")
        for operation, role in ((aliases.stage_indices, None), (aliases.release_indices, "dingo_staging")):
            for health in (
                {"status": "red", "number_of_shards": 2, "active_primary_shards": 1},
                {"status": "yellow", "number_of_shards": 2, "active_primary_shards": 1},
                {"status": "green", "number_of_shards": 0, "active_primary_shards": 0},
            ):
                client = FakeElasticsearch({name: [role] if role else []})
                client.health_overrides[name] = health
                with self.subTest(operation=operation.__name__, health=health), self.assertRaisesRegex(
                    RuntimeError, "active primary shards"
                ):
                    operation(client, [name])
                self.assertFalse(client.posts)

    def test_errors_and_unacknowledged_updates_fail(self):
        name = index("alliance", "new")
        for response in ({"acknowledged": True, "errors": True}, {"acknowledged": False}):
            client = FakeElasticsearch({name: []})
            client.response = response
            with self.subTest(response=response), self.assertRaisesRegex(RuntimeError, "not fully acknowledged"):
                aliases.stage_indices(client, [name])

    def test_readback_recovers_from_response_lost_after_application(self):
        name = index("alliance", "new")
        client = FakeElasticsearch({name: ["dingo_staging"]})
        client.raise_after_apply = True
        result = aliases.release_indices(client, [name])
        self.assertTrue(result["verified"])
        self.assertTrue(result["recovered_after_error"])
        self.assertEqual({"dingo"}, client.entries[name]["aliases"])

    def test_changed_index_uuid_before_mutation_aborts(self):
        name = index("alliance", "new")
        client = FakeElasticsearch({name: []})

        def change_uuid(call):
            if call == 2:
                client.entries[name]["uuid"] = "recreated-index"

        client.before_get = change_uuid
        with self.assertRaisesRegex(RuntimeError, "identity drifted"):
            aliases.stage_indices(client, [name])
        self.assertFalse(client.posts)

    def test_post_state_drift_is_not_reported_as_verified(self):
        name = index("alliance", "new")
        client = FakeElasticsearch({name: ["dingo_staging"]})

        def change_aliases(call):
            if call == 3:
                client.entries[name]["aliases"].add("dingo_staging")

        client.before_get = change_aliases
        with self.assertRaisesRegex(RuntimeError, "expected state"):
            aliases.release_indices(client, [name])

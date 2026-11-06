"""Thin Hub command wrappers: configured clients, registration, and shared scope."""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

PATH = Path(__file__).resolve().parents[1] / "hooks" / "dogpark_release.py"
SPEC = importlib.util.spec_from_file_location("dogpark_release_hook", PATH)
HOOK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HOOK)


class HookTests(unittest.TestCase):
    def test_uses_full_native_indexer_args_and_closes_client(self):
        config = {
            "hosts": ["https://es.invalid:9243"],
            "basic_auth": ("user", "password"),
            "ca_certs": "/certs/ca.pem",
            "verify_certs": True,
            "request_timeout": 90,
        }
        client = Mock()
        with patch.dict(HOOK.__dict__, {"im": {"ci": {"args": config}}}), patch.object(
            HOOK, "_DogParkReleaseElasticsearch", return_value=client
        ) as factory, patch.object(
            HOOK._dogpark_aliases, "release_indices", return_value={"verified": True}
        ) as release:
            result = HOOK.release_prod("ci", ["alliance"], dry_run=True)
        factory.assert_called_once_with(**config)
        release.assert_called_once_with(client, ["alliance"], sources=None, dry_run=True)
        client.close.assert_called_once_with()
        self.assertEqual("ci", result["indexer_env"])

    def test_snapshot_manager_fallback_and_single_source_argument(self):
        client = Mock()
        manager = Mock(index_manager={"prod": {"args": {"hosts": "https://prod.invalid"}}})
        with patch.dict(HOOK.__dict__, {"im": None, "ssm": manager}), patch.object(
            HOOK, "_DogParkReleaseElasticsearch", return_value=client
        ), patch.object(HOOK._dogpark_aliases, "stage_indices", return_value={}) as stage:
            HOOK.assign_staging_tag("prod", "custom-index", source="alliance")
        stage.assert_called_once_with(client, ["custom-index"], sources={"custom-index": "alliance"}, dry_run=False)
        client.close.assert_called_once_with()

    def test_closes_client_on_preflight_failure(self):
        client = Mock()
        with patch.dict(HOOK.__dict__, {"im": {"ci": {"args": {"hosts": "https://ci.invalid"}}}}), patch.object(
            HOOK, "_DogParkReleaseElasticsearch", return_value=client
        ), patch.object(HOOK._dogpark_aliases, "stage_indices", side_effect=ValueError("blocked")):
            with self.assertRaisesRegex(ValueError, "blocked"):
                HOOK.assign_staging_tags("ci", ["alliance"])
        client.close.assert_called_once_with()

    def test_unknown_environment_fails_before_client_creation(self):
        with patch.dict(HOOK.__dict__, {"im": {}}), patch.object(HOOK, "_DogParkReleaseElasticsearch") as factory:
            with self.assertRaisesRegex(ValueError, "environment"):
                HOOK.release_prod("missing")
        factory.assert_not_called()

    def test_shared_hub_namespace_has_no_file_and_shadows_list(self):
        shadow = Mock(side_effect=AssertionError("Hub list command was called"))
        hub = Mock()
        hub.server = None
        hub.commands, hub.hidden = {}, {}
        namespace = {"__name__": "hub_shell", "list": shadow, "hub": hub}
        exec(compile(PATH.read_text(), "<hub-hook>", "exec"), namespace, namespace)
        self.assertEqual(["assign_staging_tag", "assign_staging_tags", "release_prod"], namespace["__all__"])
        for name in namespace["__all__"]:
            self.assertEqual("dry_run", namespace[name].confirm)
            self.assertIs(namespace[name], hub.commands[name])
            self.assertFalse(hub.hidden[name])
        self.assertNotIn("freeze_release_manifest", namespace)
        shadow.assert_not_called()

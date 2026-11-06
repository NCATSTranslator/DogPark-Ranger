import importlib
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from hooks.dogpark_hook_utils import aliases, builds


class ReloadTests(unittest.TestCase):
    def namespace(self):
        shadow_list = Mock(side_effect=AssertionError("Called Hub's list command"))
        namespace = {"__name__": "hub_shell", "ssm": Mock(), "list": shadow_list}
        shell = SimpleNamespace(extra_ns=namespace, commands={}, hidden={}, restart=Mock())
        namespace["hub"] = shell
        return namespace, shell

    def load(self, name, namespace):
        path = PROJECT_DIR / "hooks" / name
        exec(compile(path.read_text(), "<string>", "exec"), namespace, namespace)

    def test_hub_loader_without_file_and_with_shadowed_list(self):
        namespace, shell = self.namespace()
        env = SimpleNamespace(idxenv="builder", client=Mock(), repcfg=Mock())
        env.repcfg.format.return_value = SimpleNamespace(repo="builder_repository", data={"settings": {}})
        env.client.indices.get.return_value = SimpleNamespace(body={"alliance_build": {}})
        namespace["ssm"].register = {"auto": env}
        self.load("dogpark_snapshots.py", namespace)
        collection = Mock()
        collection.find.return_value = [{
            "_id": "alliance_build", "build_config": {"name": "alliance"},
            "index": {"alliance_build": {"environment": "builder"}},
        }]
        namespace["_snapshot_state"] = Mock(return_value="MISSING")
        with patch.object(namespace["_snapshot_builds"], "get_src_build_collection", return_value=collection):
            namespace["create_snapshots"]("auto", ["alliance_build"])
        namespace["ssm"].snapshot.assert_called_once_with("auto", "alliance_build")
        namespace["list"].assert_not_called()
        for name in ("create_snapshots", "get_snapshots_release", "install_snapshots"):
            self.assertIs(shell.commands[name], namespace[name])
            self.assertFalse(shell.hidden[name])

    def test_reload_refreshes_helpers_functions_and_registry_without_restarting(self):
        namespace, shell = self.namespace()
        self.load("dogpark_snapshots.py", namespace)
        self.load("dogpark_dev.py", namespace)
        old_create = namespace["create_snapshots"]
        old_helper = builds.get_available_builds
        manager = namespace["ssm"]
        try:
            with patch.object(importlib, "reload", wraps=importlib.reload) as reload_module:
                result = namespace["reload_snapshot_hooks"]()
            self.assertEqual(reload_module.call_args_list, [call(builds), call(aliases)])
            self.assertIsNot(namespace["create_snapshots"], old_create)
            self.assertIsNot(builds.get_available_builds, old_helper)
            self.assertIs(namespace["_snapshot_builds"], builds)
            self.assertIs(namespace["ssm"], manager)
            self.assertIs(namespace["hub"], shell)
            self.assertIs(shell.commands["create_snapshots"], namespace["create_snapshots"])
            self.assertIn("reload_snapshot_hooks", shell.commands)
            self.assertFalse(shell.hidden["reload_snapshot_hooks"])
            self.assertEqual(Path(result["reloaded"][-1]).name, "dogpark_snapshots.py")
            shell.restart.assert_not_called()
            manager.snapshot.assert_not_called()
        finally:
            # Keep other tests' imported references intact after exercising reload.
            builds.get_available_builds = old_helper

    def test_native_loader_owns_command_registration(self):
        namespace, shell = self.namespace()
        shell.server = SimpleNamespace(terminal=Mock())
        self.load("dogpark_snapshots.py", namespace)
        self.load("dogpark_dev.py", namespace)
        self.assertEqual(shell.commands, {})
        self.assertEqual(shell.hidden, {})
        self.assertEqual(namespace["__all__"], ["reload_snapshot_hooks"])

    def test_native_reload_uses_hub_hook_loader(self):
        namespace, shell = self.namespace()
        shell.server = SimpleNamespace(terminal=Mock(), process_hook_file=Mock())
        self.load("dogpark_dev.py", namespace)
        with patch.object(importlib, "reload") as reload_module:
            result = namespace["reload_snapshot_hooks"]()
        self.assertEqual(reload_module.call_args_list, [call(builds), call(aliases)])
        shell.server.process_hook_file.assert_called_once_with(
            str(PROJECT_DIR / "hooks" / "dogpark_snapshots.py")
        )
        self.assertEqual(Path(result["reloaded"][-1]).name, "dogpark_snapshots.py")
        self.assertNotIn("create_snapshots", namespace)
        shell.restart.assert_not_called()

    def test_syntax_error_keeps_current_hook_and_does_not_reload_helpers(self):
        namespace, shell = self.namespace()
        self.load("dogpark_snapshots.py", namespace)
        self.load("dogpark_dev.py", namespace)
        old_create = namespace["create_snapshots"]
        with patch.object(Path, "read_text", return_value="def broken(:"), patch.object(importlib, "reload") as reload_module:
            with self.assertRaises(SyntaxError):
                namespace["reload_snapshot_hooks"]()
        self.assertIs(namespace["create_snapshots"], old_create)
        reload_module.assert_not_called()
        shell.restart.assert_not_called()


if __name__ == "__main__":
    unittest.main()

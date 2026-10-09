"""Tests for hub.databuild.gandalf_builder (Option D, GANDALF_INTEGRATION.md).

biothings isn't importable in every environment that runs these tests (it
connects to MongoDB the moment ``biothings.hub`` is imported), so, as in
tests/test_kg_uploader.py, the small part of it that gandalf_builder imports is
faked in sys.modules. Everything about running a process is real: actual bash,
actual pipes, actual timeouts and process-group kills.
"""

import inspect
import os
import shutil
import sys
import tempfile
import textwrap
import time
import types
import unittest
from pathlib import Path
from unittest import mock


class FakeBuilderException(Exception):
    pass


class FakeLinkDataBuilder:
    """Stands in for biothings' LinkDataBuilder. GandalfBuilder only adds post_merge."""


fake_builder_module = types.ModuleType("biothings.hub.databuild.builder")
fake_builder_module.BuilderException = FakeBuilderException
fake_builder_module.LinkDataBuilder = FakeLinkDataBuilder

fake_hub_db_module = types.ModuleType("biothings.utils.hub_db")
fake_hub_db_module.get_src_dump = mock.Mock(name="get_src_dump")

fake_indexer_module = types.ModuleType("hub.dataindex.indexer")
fake_indexer_module.resolve_kg_build_backends = mock.Mock(name="resolve_kg_build_backends")

# a plain `import biothings.utils.hub_db` walks every parent package, so each one
# needs its own empty stand-in too, not just the leaf modules above
with mock.patch.dict(
    sys.modules,
    {
        "biothings": types.ModuleType("biothings"),
        "biothings.hub": types.ModuleType("biothings.hub"),
        "biothings.hub.databuild": types.ModuleType("biothings.hub.databuild"),
        "biothings.hub.databuild.builder": fake_builder_module,
        "biothings.utils": types.ModuleType("biothings.utils"),
        "biothings.utils.hub_db": fake_hub_db_module,
        "hub.dataindex.indexer": fake_indexer_module,
    },
):
    import hub.databuild as databuild_package
    from hub.databuild import gandalf_builder as gb

get_src_dump = fake_hub_db_module.get_src_dump


def backend(col, dbs="dogpark_src", host="localhost", port=27017):
    return types.SimpleNamespace(args={"host": host, "port": port}, dbs=dbs, col=col)


def edges(**kwargs):
    return backend("tier0_kg_edges", **kwargs)


def nodes(**kwargs):
    return backend("tier0_kg_nodes", **kwargs)


class DiscoveryTest(unittest.TestCase):
    def test_the_package_exposes_the_builder_for_biothings_class_discovery(self):
        # BuilderManager.find_builder_classes() only looks at names defined in the
        # hub.databuild package itself, using exactly this predicate
        found = [
            klass
            for _, klass in inspect.getmembers(
                databuild_package, lambda obj: isinstance(obj, type) and issubclass(obj, FakeLinkDataBuilder)
            )
        ]
        self.assertIn(gb.GandalfBuilder, found)

    def test_the_class_path_matches_what_the_build_config_must_say(self):
        self.assertEqual(
            f"{gb.GandalfBuilder.__module__}.{gb.GandalfBuilder.__name__}",
            "hub.databuild.gandalf_builder.GandalfBuilder",
        )

    def test_the_default_script_is_the_one_shipped_in_bin(self):
        self.assertEqual(gb.DEFAULT_SCRIPT.name, "build_gandalf.sh")
        self.assertTrue(gb.DEFAULT_SCRIPT.is_file(), gb.DEFAULT_SCRIPT)


class ScriptEnvTest(unittest.TestCase):
    def test_carries_the_resolved_coordinates(self):
        env = gb.script_env({}, edges(host="su11", port=27018), nodes(host="su11", port=27018))

        self.assertEqual(
            env,
            {
                "GANDALF_MONGO_URI": "mongodb://su11:27018",
                "GANDALF_MONGO_DB": "dogpark_src",
                "GANDALF_NODES_COLLECTION": "tier0_kg_nodes",
                "GANDALF_EDGES_COLLECTION": "tier0_kg_edges",
            },
        )

    def test_adds_the_release_only_when_known(self):
        self.assertEqual(gb.script_env({}, edges(), nodes(), "2026_06_21")["GANDALF_RELEASE"], "2026_06_21")
        self.assertNotIn("GANDALF_RELEASE", gb.script_env({}, edges(), nodes(), None))

    def test_maps_the_optional_settings(self):
        env = gb.script_env(
            {"root": "/data/g", "gandalf_bin": "/venv/bin/gandalf-build-mongo", "service": "g.service"},
            edges(),
            nodes(),
        )

        self.assertEqual(env["GANDALF_ROOT"], "/data/g")
        self.assertEqual(env["GANDALF_BUILD_MONGO_BIN"], "/venv/bin/gandalf-build-mongo")
        self.assertEqual(env["GANDALF_SERVICE"], "g.service")

    def test_a_service_left_out_keeps_the_scripts_default_but_none_turns_it_off(self):
        self.assertNotIn("GANDALF_SERVICE", gb.script_env({}, edges(), nodes()))
        # set-but-empty is how the script tells "skip the restart" from "use the default"
        self.assertEqual(gb.script_env({"service": None}, edges(), nodes())["GANDALF_SERVICE"], "")
        self.assertEqual(gb.script_env({"service": ""}, edges(), nodes())["GANDALF_SERVICE"], "")

    def test_maps_only_the_s3_settings_that_are_given(self):
        env = gb.script_env({"s3": {"bucket": "b", "prefix": "p", "region": None}}, edges(), nodes())

        self.assertEqual(env["GANDALF_S3_BUCKET"], "b")
        self.assertEqual(env["GANDALF_S3_PREFIX"], "p")
        self.assertNotIn("GANDALF_S3_REGION", env)
        self.assertNotIn("GANDALF_S3_PROFILE", env)

    def test_refuses_nodes_and_edges_in_different_databases(self):
        with self.assertRaises(gb.GandalfBuildError) as caught:
            gb.script_env({}, edges(dbs="dogpark_target"), nodes(dbs="dogpark_src"))

        self.assertIn("single database", str(caught.exception))

    def test_refuses_nodes_and_edges_on_different_servers(self):
        with self.assertRaises(gb.GandalfBuildError):
            gb.script_env({}, edges(host="a"), nodes(host="b"))


class SettingsAndReleaseTest(unittest.TestCase):
    def setUp(self):
        get_src_dump.reset_mock(return_value=True, side_effect=True)

    def settings_from(self, config):
        fake_biothings = types.ModuleType("biothings")
        if config is not None:
            fake_biothings.config = config
        with mock.patch.dict(sys.modules, {"biothings": fake_biothings}):
            return gb.gandalf_settings()

    def test_reads_gandalf_build_from_the_hub_config(self):
        self.assertEqual(self.settings_from(types.SimpleNamespace(GANDALF_BUILD={"root": "/x"})), {"root": "/x"})

    def test_is_empty_when_the_config_does_not_define_it(self):
        self.assertEqual(self.settings_from(types.SimpleNamespace()), {})
        self.assertEqual(self.settings_from(None), {})

    def test_returns_a_copy(self):
        config = types.SimpleNamespace(GANDALF_BUILD={"root": "/x"})

        self.settings_from(config)["root"] = "/changed"

        self.assertEqual(config.GANDALF_BUILD["root"], "/x")

    def test_release_is_what_the_dumper_recorded_under_download(self):
        # the real shape of a src_dump record, as seen in a hub that dumped tier0_kg
        get_src_dump.return_value.find_one.return_value = {
            "_id": "tier0_kg",
            "download": {"status": "success", "release": "2026_06_21"},
            "upload": {"jobs": {}},
        }

        self.assertEqual(gb.kgx_release_for("tier0_kg"), "2026_06_21")
        get_src_dump.return_value.find_one.assert_called_once_with({"_id": "tier0_kg"})

    def test_a_top_level_release_is_not_where_biothings_keeps_it(self):
        get_src_dump.return_value.find_one.return_value = {"_id": "tier0_kg", "release": "wrong-place"}

        self.assertIsNone(gb.kgx_release_for("tier0_kg"))

    def test_release_is_none_when_unknown_or_the_lookup_fails(self):
        for record in ({"_id": "tier0_kg"}, {"_id": "tier0_kg", "download": {}}, {"download": None}, None):
            with self.subTest(record=record):
                get_src_dump.return_value.find_one.return_value = record
                self.assertIsNone(gb.kgx_release_for("tier0_kg"))
        get_src_dump.side_effect = RuntimeError("no mongo")
        self.assertIsNone(gb.kgx_release_for("tier0_kg"))


class RunStreamingTest(unittest.TestCase):
    def run_bash(self, script, timeout=None, env=None):
        lines = []
        gb.run_streaming(["bash", "-c", script], env or dict(os.environ), lines.append, timeout=timeout)
        return lines

    def test_logs_stdout_and_stderr_lines_in_order(self):
        self.assertEqual(self.run_bash("echo one; echo two >&2; echo three"), ["one", "two", "three"])

    def test_logs_each_line_while_the_script_is_still_running(self):
        stamps = []

        def log(line):
            stamps.append((time.monotonic(), line))

        gb.run_streaming(["bash", "-c", "echo first; sleep 1.2; echo second"], dict(os.environ), log)

        self.assertEqual([line for _, line in stamps], ["first", "second"])
        self.assertGreater(stamps[1][0] - stamps[0][0], 0.8, "the first line was held back until the script ended")

    def test_a_failing_script_raises_with_its_status_and_last_output(self):
        with self.assertRaises(gb.GandalfBuildError) as caught:
            self.run_bash("echo before; echo 'it broke' >&2; exit 7")

        message = str(caught.exception)
        self.assertIn("status 7", message)
        self.assertIn("it broke", message)

    def test_the_error_keeps_only_the_tail_of_long_output(self):
        with self.assertRaises(gb.GandalfBuildError) as caught:
            self.run_bash("for i in $(seq 1 100); do echo L$i; done; exit 2")

        message = str(caught.exception)
        self.assertIn("L100", message)
        self.assertIn("L71", message)
        self.assertNotIn("L70\n", message + "\n")

    def test_a_timeout_terminates_the_script_and_says_so(self):
        lines = []
        started = time.monotonic()

        with self.assertRaises(gb.GandalfBuildError) as caught:
            gb.run_streaming(["bash", "-c", "echo started; sleep 30"], dict(os.environ), lines.append, timeout=1)

        self.assertIn("did not finish within 1s", str(caught.exception))
        self.assertEqual(lines, ["started"])
        self.assertLess(time.monotonic() - started, 8)

    def test_a_timeout_takes_down_everything_the_script_started(self):
        pidfile = Path(tempfile.mkdtemp(prefix="gandalf-pid-")) / "pid"
        self.addCleanup(shutil.rmtree, pidfile.parent, ignore_errors=True)

        with self.assertRaises(gb.GandalfBuildError):
            self.run_bash(f"sleep 60 & echo $! > {pidfile}; wait", timeout=1)

        child = int(pidfile.read_text())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(child, 0)
            except ProcessLookupError:
                return
            time.sleep(0.1)
        os.kill(child, 9)
        self.fail("the script's child process survived the timeout")

    def test_a_command_that_cannot_start_raises(self):
        with self.assertRaises(gb.GandalfBuildError) as caught:
            gb.run_streaming(["/no/such/command"], dict(os.environ), lambda line: None)

        self.assertIn("could not start", str(caught.exception))

    def test_nothing_can_wait_for_input(self):
        started = time.monotonic()

        lines = self.run_bash("read -t 5 answer; echo rc=$?")

        self.assertEqual(lines, ["rc=1"])  # end of input at once, not a 5 second wait
        self.assertLess(time.monotonic() - started, 4)

    def test_output_that_is_not_utf8_does_not_break_the_build(self):
        lines = self.run_bash(r"printf '\xff\xfe still ok\n'")

        self.assertEqual(len(lines), 1)
        self.assertIn("still ok", lines[0])

    def test_a_logging_failure_stops_the_script_instead_of_leaving_it_running(self):
        def log(line):
            raise RuntimeError("log broke")

        started = time.monotonic()

        with self.assertRaises(RuntimeError):
            gb.run_streaming(["bash", "-c", "echo a; sleep 30"], dict(os.environ), log)

        self.assertLess(time.monotonic() - started, 8)


class PostMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="gandalf-builder-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def make_script(self, body):
        script = self.tmp / "build_gandalf.sh"
        script.write_text("#!/usr/bin/env bash\n" + textwrap.dedent(body))
        return script

    def make_builder(self, settings, release="2026_06_21"):
        builder = gb.GandalfBuilder.__new__(gb.GandalfBuilder)
        builder.logger = mock.Mock(name="logger")
        builder.target_name = "tier0_kg_gandalf_20261009_abcd"
        builder.register_status = mock.Mock(name="register_status")
        builder.source_backend = types.SimpleNamespace(build=mock.Mock(name="src_build"))
        builder.source_backend.build.find_one.return_value = {"_id": builder.target_name}
        patches = [
            mock.patch.object(gb, "gandalf_settings", return_value=settings),
            mock.patch.object(
                gb, "resolve_kg_build_backends", return_value=(edges(), nodes(), "tier0_kg")
            ),
            mock.patch.object(gb, "kgx_release_for", return_value=release),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        return builder

    def script_output(self, builder):
        return [call.args[1] for call in builder.logger.info.call_args_list if call.args[0] == "[gandalf] %s"]

    def test_runs_the_script_with_this_builds_collections_and_streams_its_output(self):
        script = self.make_script("env | grep -E '^(GANDALF_|PYTHONUNBUFFERED)' | sort\n")
        builder = self.make_builder({"script": str(script), "s3": {"bucket": "my-bucket"}})

        result = builder.post_merge(["tier0_kg_edges"], 100000, job_manager=None)

        self.assertEqual(
            self.script_output(builder),
            [
                "GANDALF_EDGES_COLLECTION=tier0_kg_edges",
                "GANDALF_MONGO_DB=dogpark_src",
                "GANDALF_MONGO_URI=mongodb://localhost:27017",
                "GANDALF_NODES_COLLECTION=tier0_kg_nodes",
                "GANDALF_RELEASE=2026_06_21",
                "GANDALF_S3_BUCKET=my-bucket",
                "PYTHONUNBUFFERED=1",
            ],
        )
        self.assertEqual(
            result, {"gandalf_build": {"release": "2026_06_21", "nodes": "tier0_kg_nodes", "edges": "tier0_kg_edges"}}
        )
        builder.register_status.assert_not_called()
        builder.source_backend.build.find_one.assert_called_once_with({"_id": builder.target_name})

    def test_the_script_inherits_the_hubs_environment(self):
        script = self.make_script('echo "path_seen=${PATH:+yes}"\n')
        builder = self.make_builder({"script": str(script)})

        builder.post_merge([], 1, None)

        self.assertIn("path_seen=yes", self.script_output(builder))

    def test_warns_up_front_when_nothing_will_be_uploaded(self):
        script = self.make_script("true\n")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GANDALF_S3_BUCKET", None)
            builder = self.make_builder({"script": str(script)})
            builder.post_merge([], 1, None)

        builder.logger.warning.assert_called_once()
        self.assertIn("not uploaded", builder.logger.warning.call_args.args[0])

    def test_does_not_warn_when_a_bucket_is_configured(self):
        script = self.make_script("true\n")
        builder = self.make_builder({"script": str(script), "s3": {"bucket": "b"}})

        builder.post_merge([], 1, None)

        builder.logger.warning.assert_not_called()

    def test_a_failing_script_marks_the_build_failed_and_raises(self):
        script = self.make_script("echo working; echo 'step 4 failed' >&2; exit 9\n")
        builder = self.make_builder({"script": str(script)})

        with self.assertRaises(gb.GandalfBuildError):
            builder.post_merge([], 1, None)

        builder.register_status.assert_called_once()
        args, kwargs = builder.register_status.call_args
        self.assertEqual(args, ("failed",))
        self.assertIn("status 9", kwargs["job"]["err"])
        self.assertIn("step 4 failed", kwargs["job"]["err"])

    def test_a_missing_script_is_reported_clearly_and_marks_the_build_failed(self):
        builder = self.make_builder({"script": str(self.tmp / "nope.sh")})

        with self.assertRaises(gb.GandalfBuildError) as caught:
            builder.post_merge([], 1, None)

        self.assertIn("not found", str(caught.exception))
        builder.register_status.assert_called_once()

    def test_nodes_and_edges_in_different_databases_fail_before_anything_runs(self):
        script = self.make_script("echo should-not-run\n")
        builder = self.make_builder({"script": str(script)})
        gb.resolve_kg_build_backends.return_value = (edges(dbs="dogpark_target"), nodes(), "tier0_kg")

        with self.assertRaises(gb.GandalfBuildError):
            builder.post_merge([], 1, None)

        self.assertEqual(self.script_output(builder), [])
        builder.register_status.assert_called_once()

    def test_the_timeout_comes_from_the_settings(self):
        script = self.make_script("sleep 30\n")
        builder = self.make_builder({"script": str(script), "timeout": 1})

        with self.assertRaises(gb.GandalfBuildError) as caught:
            builder.post_merge([], 1, None)

        self.assertIn("did not finish within 1s", str(caught.exception))

    def test_runs_the_bundled_script_when_none_is_configured(self):
        builder = self.make_builder({})

        with mock.patch.object(gb, "run_streaming") as run_streaming:
            builder.post_merge([], 1, None)

        self.assertEqual(run_streaming.call_args.args[0], ["bash", str(gb.DEFAULT_SCRIPT)])
        self.assertEqual(run_streaming.call_args.kwargs["timeout"], gb.DEFAULT_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()

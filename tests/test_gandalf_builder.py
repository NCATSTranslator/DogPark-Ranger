"""Tests for hub.databuild.gandalf_builder.

biothings connects to MongoDB as soon as ``biothings.hub`` is imported, so, as in
tests/test_kg_uploader.py, the few biothings names that gandalf_builder imports are
replaced with fakes in sys.modules. Process handling is not faked: the tests use real
bash, pipes, timeouts and process-group kills.
"""

import inspect
import json
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


class FakeLastModifiedHTTPDumper:
    """Stands in for biothings' LastModifiedHTTPDumper, the base class of KgDumper."""


fake_builder_module = types.ModuleType("biothings.hub.databuild.builder")
fake_builder_module.BuilderException = FakeBuilderException
fake_builder_module.LinkDataBuilder = FakeLinkDataBuilder

fake_dumper_module = types.ModuleType("biothings.hub.dataload.dumper")
fake_dumper_module.LastModifiedHTTPDumper = FakeLastModifiedHTTPDumper

fake_hub_db_module = types.ModuleType("biothings.utils.hub_db")
fake_hub_db_module.get_src_dump = mock.Mock(name="get_src_dump")

fake_indexer_module = types.ModuleType("hub.dataindex.indexer")
fake_indexer_module.resolve_kg_build_backends = mock.Mock(name="resolve_kg_build_backends")

# importing biothings.utils.hub_db also imports each of its parent packages, so the
# parents need empty fakes too
with mock.patch.dict(
    sys.modules,
    {
        "biothings": types.ModuleType("biothings"),
        "biothings.hub": types.ModuleType("biothings.hub"),
        "biothings.hub.databuild": types.ModuleType("biothings.hub.databuild"),
        "biothings.hub.databuild.builder": fake_builder_module,
        "biothings.hub.dataload": types.ModuleType("biothings.hub.dataload"),
        "biothings.hub.dataload.dumper": fake_dumper_module,
        "biothings.utils": types.ModuleType("biothings.utils"),
        "biothings.utils.hub_db": fake_hub_db_module,
        "hub.dataindex.indexer": fake_indexer_module,
    },
):
    import hub.databuild as databuild_package
    from hub.databuild import gandalf_builder as gb

get_src_dump = fake_hub_db_module.get_src_dump

GRAPH_METADATA = {"name": "translator_kg", "version": "2026_06_21"}


def src_dump_record(graph=GRAPH_METADATA, dumped="2026_06_21", uploaded="2026_06_21"):
    """A src_dump record shaped like the real one: the dump, the upload and KgDumper's document."""
    record = {
        "_id": "tier0_kg",
        "download": {"release": dumped},
        "upload": {"jobs": {"tier0_kg_edges": {"release": uploaded}}},
    }
    if graph is not None:
        record["kgx_metadata"] = {"graph": graph}
    return record


def backend(col, dbs="dogpark_src", host="localhost", port=27017):
    return types.SimpleNamespace(args={"host": host, "port": port}, dbs=dbs, col=col)


def edges(**kwargs):
    return backend("tier0_kg_edges", **kwargs)


def nodes(**kwargs):
    return backend("tier0_kg_nodes", **kwargs)


class DiscoveryTest(unittest.TestCase):
    def test_the_package_exposes_the_builder_for_biothings_class_discovery(self):
        # BuilderManager.find_builder_classes() only looks at the names defined in the
        # hub.databuild package, with this predicate
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

    def test_adds_the_build_name_only_when_known(self):
        env = gb.script_env({}, edges(), nodes(), build_name="tier0_kg_gandalf_20261012_wffqr2d5")

        # the script names the graph folder and the archive after it
        self.assertEqual(env["GANDALF_BUILD_NAME"], "tier0_kg_gandalf_20261012_wffqr2d5")
        self.assertNotIn("GANDALF_BUILD_NAME", gb.script_env({}, edges(), nodes()))

    def test_adds_the_graph_metadata_path_only_when_given(self):
        env = gb.script_env({}, edges(), nodes(), graph_metadata="/tmp/gandalf-x/graph-metadata.json")

        self.assertEqual(env["GANDALF_GRAPH_METADATA"], "/tmp/gandalf-x/graph-metadata.json")
        self.assertNotIn("GANDALF_GRAPH_METADATA", gb.script_env({}, edges(), nodes()))

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
        # empty means skip the restart, unset means use the default
        self.assertEqual(gb.script_env({"service": None}, edges(), nodes())["GANDALF_SERVICE"], "")
        self.assertEqual(gb.script_env({"service": ""}, edges(), nodes())["GANDALF_SERVICE"], "")

    def test_maps_only_the_s3_settings_that_are_given(self):
        env = gb.script_env({"s3": {"bucket": "b", "prefix": "p", "region": None}}, edges(), nodes())

        self.assertEqual(env["GANDALF_S3_BUCKET"], "b")
        self.assertEqual(env["GANDALF_S3_PREFIX"], "p")
        self.assertNotIn("GANDALF_S3_REGION", env)
        self.assertNotIn("GANDALF_S3_PROFILE", env)

    def test_s3_none_skips_the_upload_on_purpose(self):
        env = gb.script_env({"s3": None}, edges(), nodes())

        # empty means no upload on purpose, unset means nobody configured it
        self.assertEqual(env["GANDALF_S3_BUCKET"], "")
        self.assertNotIn("GANDALF_S3_PREFIX", env)

    def test_a_missing_or_empty_bucket_stays_unset_so_the_script_refuses_to_start(self):
        for settings in ({}, {"s3": {}}, {"s3": {"bucket": ""}}, {"s3": {"bucket": None, "prefix": "gandalf"}}):
            with self.subTest(settings=settings):
                self.assertNotIn("GANDALF_S3_BUCKET", gb.script_env(settings, edges(), nodes()))

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
        # shape of a src_dump record from a hub that dumped tier0_kg
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


class ScriptContractTest(unittest.TestCase):
    def test_the_script_reads_every_variable_the_builder_sets(self):
        env = gb.script_env(
            {
                "root": "/data/g",
                "gandalf_bin": "/venv/bin/gandalf-build-mongo",
                "service": "g.service",
                "s3": {"bucket": "b", "prefix": "p", "region": "r", "profile": "x"},
            },
            edges(),
            nodes(),
            release="2026_06_21",
            build_name="tier0_kg_gandalf_20261012_abcd",
            graph_metadata="/tmp/graph-metadata.json",
        )

        script = gb.DEFAULT_SCRIPT.read_text()
        for name in env:
            with self.subTest(variable=name):
                # the whole name, so GANDALF_GRAPH_METADATA_FILE does not pass for GANDALF_GRAPH_METADATA
                self.assertRegex(script, rf"(?<![A-Z0-9_]){name}(?![A-Z0-9_])")


class GraphMetadataForTest(unittest.TestCase):
    def setUp(self):
        get_src_dump.reset_mock(return_value=True, side_effect=True)

    def stored(self, record):
        get_src_dump.return_value.find_one.return_value = record

    def test_returns_the_document_kgdumper_stored(self):
        self.stored(src_dump_record())

        self.assertEqual(gb.graph_metadata_for("tier0_kg", "tier0_kg_edges"), GRAPH_METADATA)
        get_src_dump.return_value.find_one.assert_called_once_with({"_id": "tier0_kg"})

    def test_a_source_without_the_document_is_refused_and_told_what_to_do(self):
        for record in (src_dump_record(graph=None), {"_id": "tier0_kg"}, None, src_dump_record(graph="not an object")):
            with self.subTest(record=record):
                self.stored(record)

                with self.assertRaises(gb.GandalfBuildError) as caught:
                    gb.graph_metadata_for("tier0_kg", "tier0_kg_edges")

                self.assertIn("No graph-metadata.json is stored for 'tier0_kg'", str(caught.exception))
                self.assertIn("dump check", str(caught.exception))

    def test_a_document_from_another_release_than_the_uploaded_data_is_refused(self):
        self.stored(src_dump_record(dumped="2026_07_20", uploaded="2026_06_21"))

        with self.assertRaises(gb.GandalfBuildError) as caught:
            gb.graph_metadata_for("tier0_kg", "tier0_kg_edges")

        self.assertIn("last dumped as release 2026_07_20", str(caught.exception))
        self.assertIn("uploaded from release 2026_06_21", str(caught.exception))

    def test_an_unknown_uploaded_release_is_not_a_mismatch(self):
        for uploaded in (None, ""):
            with self.subTest(uploaded=uploaded):
                self.stored(src_dump_record(uploaded=uploaded))

                self.assertEqual(gb.graph_metadata_for("tier0_kg", "tier0_kg_edges"), GRAPH_METADATA)
        self.stored(src_dump_record())
        self.assertEqual(gb.graph_metadata_for("tier0_kg", "a_collection_with_no_upload_job"), GRAPH_METADATA)

    def test_a_lookup_error_is_not_swallowed(self):
        # unlike kgx_release_for, which only feeds the log, the build needs this document
        get_src_dump.side_effect = RuntimeError("no mongo")

        with self.assertRaises(RuntimeError):
            gb.graph_metadata_for("tier0_kg", "tier0_kg_edges")


class GraphMetadataFileTest(unittest.TestCase):
    def test_holds_the_document_while_the_block_runs_and_is_gone_after(self):
        with gb.graph_metadata_file(GRAPH_METADATA) as path:
            self.assertEqual(path.name, "graph-metadata.json")
            self.assertEqual(json.loads(path.read_text()), GRAPH_METADATA)
            self.assertEqual(path.stat().st_mode & 0o777, 0o644)
            folder = path.parent

        self.assertFalse(folder.exists())

    def test_is_removed_when_the_block_fails(self):
        with self.assertRaises(RuntimeError):
            with gb.graph_metadata_file(GRAPH_METADATA) as path:
                folder = path.parent
                raise RuntimeError("the script failed")

        self.assertFalse(folder.exists())

    def test_uses_the_layout_of_the_file_kgx_publishes(self):
        with gb.graph_metadata_file({"name": "x", "tags": [1, 2]}) as path:
            text = path.read_text()

        self.assertEqual(text, '{\n  "name": "x",\n  "tags": [\n    1,\n    2\n  ]\n}')


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
            mock.patch.object(gb, "graph_metadata_for", return_value=GRAPH_METADATA),
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

        output = self.script_output(builder)
        # the temporary file's folder is different on every run, so only its name is checked
        metadata_path = next(line for line in output if line.startswith("GANDALF_GRAPH_METADATA="))
        self.assertEqual(Path(metadata_path.split("=", 1)[1]).name, "graph-metadata.json")
        self.assertEqual(
            [line for line in output if not line.startswith("GANDALF_GRAPH_METADATA=")],
            [
                "GANDALF_BUILD_NAME=tier0_kg_gandalf_20261009_abcd",
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

    def test_the_script_gets_the_stored_document_in_a_file_that_is_removed_afterwards(self):
        script = self.make_script('echo "path=${GANDALF_GRAPH_METADATA}"\ncat "${GANDALF_GRAPH_METADATA}"\n')
        builder = self.make_builder({"script": str(script)})

        builder.post_merge([], 1, None)

        output = self.script_output(builder)
        path = Path(output[0].split("=", 1)[1])
        self.assertEqual(json.loads("\n".join(output[1:])), GRAPH_METADATA)
        self.assertFalse(path.exists(), "the file must not outlive the script")
        gb.graph_metadata_for.assert_called_once_with("tier0_kg", "tier0_kg_edges")

    def test_the_file_is_removed_when_the_script_fails(self):
        script = self.make_script('echo "path=${GANDALF_GRAPH_METADATA}"\nexit 9\n')
        builder = self.make_builder({"script": str(script)})

        with self.assertRaises(gb.GandalfBuildError):
            builder.post_merge([], 1, None)

        path = Path(self.script_output(builder)[0].split("=", 1)[1])
        self.assertFalse(path.exists())

    def test_a_missing_document_fails_the_build_before_the_script_runs(self):
        script = self.make_script("echo should-not-run\n")
        builder = self.make_builder({"script": str(script)})
        gb.graph_metadata_for.side_effect = gb.GandalfBuildError("No graph-metadata.json is stored for 'tier0_kg'.")

        with self.assertRaises(gb.GandalfBuildError):
            builder.post_merge([], 1, None)

        self.assertEqual(self.script_output(builder), [])
        builder.register_status.assert_called_once()
        self.assertIn("No graph-metadata.json is stored", builder.register_status.call_args.kwargs["job"]["err"])

    def test_the_script_inherits_the_hubs_environment(self):
        script = self.make_script('echo "path_seen=${PATH:+yes}"\n')
        builder = self.make_builder({"script": str(script)})

        builder.post_merge([], 1, None)

        self.assertIn("path_seen=yes", self.script_output(builder))

    def test_a_missing_bucket_reaches_the_script_unset_and_a_switched_off_upload_reaches_it_empty(self):
        script = self.make_script('echo "bucket=${GANDALF_S3_BUCKET-<unset>}"\n')
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GANDALF_S3_BUCKET", None)
            missing = self.make_builder({"script": str(script)})
            missing.post_merge([], 1, None)
            switched_off = self.make_builder({"script": str(script), "s3": None})
            switched_off.post_merge([], 1, None)

        self.assertEqual(self.script_output(missing), ["bucket=<unset>"])
        self.assertEqual(self.script_output(switched_off), ["bucket="])
        # the script decides what a missing bucket means; the builder doesn't warn about it
        missing.logger.warning.assert_not_called()

    def test_a_script_that_refuses_to_start_fails_the_build_with_its_reasons(self):
        script = self.make_script(
            """\
            echo "ERROR: build_gandalf.sh is not set up to run. Nothing was built, restarted or uploaded." >&2
            echo "  - GANDALF_S3_BUCKET is not set: name the bucket to upload to" >&2
            exit 2
            """
        )
        builder = self.make_builder({"script": str(script)})

        with self.assertRaises(gb.GandalfBuildError) as caught:
            builder.post_merge([], 1, None)

        self.assertIn("status 2", str(caught.exception))
        self.assertIn("GANDALF_S3_BUCKET is not set", str(caught.exception))
        builder.register_status.assert_called_once()
        self.assertIn("GANDALF_S3_BUCKET is not set", builder.register_status.call_args.kwargs["job"]["err"])

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

"""Runs bin/build_gandalf.sh for real, with stand-ins for the commands that
would touch Mongo, systemd and AWS.

``gandalf-build-mongo``, ``sudo``, ``systemctl`` and ``aws`` are tiny scripts on
a restricted PATH that record how they were called, so every step of the real
script executes (``tar``, ``ln``, ``mkdir``, the step ordering, the failure
behaviour) without needing any of that infrastructure. PATH holds only the
stubs plus symlinks to the few basic tools the script uses, so a real ``sudo``,
``aws`` or ``systemctl`` on the machine running the tests can never leak in,
and "command missing" cases mean it. The script runs under ``/bin/bash`` when
there is one (3.2 on macOS), the oldest bash it has to support.
"""

import os
import shutil
import subprocess
import tarfile
import tempfile
import time
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "bin" / "build_gandalf.sh"
BASH = "/bin/bash" if os.path.exists("/bin/bash") else shutil.which("bash")
# what the script and the stubs themselves need, and nothing else
BASIC_TOOLS = ("bash", "env", "tar", "gzip", "gunzip", "ln", "mkdir", "date", "basename", "dirname", "sed", "cat", "rm")

STUBS = {
    "gandalf-build-mongo": """#!/usr/bin/env bash
echo "gandalf-build-mongo $*" >> "$STUB_LOG"
echo "TMPDIR=$TMPDIR" >> "$STUB_LOG"
out=""
while [[ $# -gt 0 ]]; do
    case "$1" in --output) out="$2"; shift 2;; *) shift;; esac
done
if [[ -n "${STUB_BUILD_FAIL:-}" ]]; then echo "stub build exploded" >&2; exit 3; fi
mkdir -p "$out" && echo graph > "$out/graph.bin"
echo "stub graph built"
""",
    "sudo": '#!/usr/bin/env bash\necho "sudo $*" >> "$STUB_LOG"\n',
    "systemctl": '#!/usr/bin/env bash\necho "systemctl $*" >> "$STUB_LOG"\n',
    "aws": '#!/usr/bin/env bash\necho "aws $*" >> "$STUB_LOG"\n',
}
DATE = "10_09_2026"


class BuildGandalfScriptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="build-gandalf-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.stubs = self.tmp / "stubs"
        self.stubs.mkdir()
        self.toolbox = self.tmp / "toolbox"
        self.toolbox.mkdir()
        for tool in BASIC_TOOLS:
            real = BASH if tool == "bash" else shutil.which(tool)
            if real:
                (self.toolbox / tool).symlink_to(real)
        self.root = self.tmp / "gandalf"
        self.log = self.tmp / "stub.log"
        for name, body in STUBS.items():
            self.add_stub(name, body)

    def add_stub(self, name, body):
        path = self.stubs / name
        path.write_text(body)
        path.chmod(0o755)
        return path

    def remove_stub(self, name):
        (self.stubs / name).unlink()

    def run_script(self, **overrides):
        """Run the script with a clean environment. A value of None unsets a variable."""
        env = {
            "PATH": f"{self.stubs}:{self.toolbox}",
            "HOME": str(self.tmp),
            "STUB_LOG": str(self.log),
            "GANDALF_ROOT": str(self.root),
            "GANDALF_BUILD_DATE": DATE,
            "GANDALF_MONGO_URI": "mongodb://db.example:27017",
            "GANDALF_MONGO_DB": "dogpark_src",
            "GANDALF_NODES_COLLECTION": "tier0_kg_nodes",
            "GANDALF_EDGES_COLLECTION": "tier0_kg_edges",
        }
        env.update(overrides)
        env = {key: value for key, value in env.items() if value is not None}
        return subprocess.run([BASH, str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120)

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    @property
    def archive(self):
        return self.root / "compacted" / f"gandalf_translatorkg_{DATE}.tar.gz"


class FullRunTest(BuildGandalfScriptTest):
    def test_runs_all_five_steps_in_order(self):
        result = self.run_script(
            GANDALF_S3_BUCKET="my-bucket",
            GANDALF_S3_PREFIX="gandalf",
            GANDALF_S3_REGION="us-west-2",
            GANDALF_S3_PROFILE="gandalf-profile",
            GANDALF_RELEASE="2026_06_21",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        steps = [line.split("]")[0] + "]" for line in result.stdout.splitlines() if line.startswith("[")]
        self.assertEqual(steps, ["[1/5]", "[2/5]", "[3/5]", "[4/5]", "[5/5]"])
        self.assertIn("Upstream KGX release: 2026_06_21", result.stdout)
        self.assertIn("Done. Build date: " + DATE, result.stdout)

        graph = self.root / "processed" / DATE / "graph_mmap"
        self.assertEqual((graph / "graph.bin").read_text().strip(), "graph")
        with tarfile.open(self.archive) as archive:
            self.assertIn("graph_mmap/graph.bin", archive.getnames())
        latest = self.root / "processed" / "latest"
        self.assertTrue(latest.is_symlink())
        self.assertEqual(latest.resolve(), graph.resolve())

    def test_passes_the_resolved_coordinates_to_gandalf_build_mongo(self):
        self.run_script()

        build_call = self.calls()[0]
        self.assertEqual(
            build_call,
            "gandalf-build-mongo --mongo-uri mongodb://db.example:27017 --db dogpark_src "
            f"--nodes-collection tier0_kg_nodes --edges-collection tier0_kg_edges "
            f"--output {self.root}/processed/{DATE}/graph_mmap/",
        )
        # the build gets its own temp dir, as in the previous hand-run script
        self.assertEqual(self.calls()[1], f"TMPDIR={self.root}/tmp")

    def test_restarts_the_service_without_ever_prompting_for_a_password(self):
        self.run_script()

        self.assertIn("sudo -n systemctl restart gandalf.service", self.calls())

    def test_uploads_the_archive_to_s3(self):
        self.run_script(
            GANDALF_S3_BUCKET="my-bucket",
            GANDALF_S3_PREFIX="gandalf",
            GANDALF_S3_REGION="us-west-2",
            GANDALF_S3_PROFILE="gandalf-profile",
        )

        self.assertIn(
            f"aws s3 cp {self.archive} s3://my-bucket/gandalf/gandalf_translatorkg_{DATE}.tar.gz "
            "--no-progress --region us-west-2 --profile gandalf-profile",
            self.calls(),
        )

    def test_s3_key_handles_a_trailing_slash_or_no_prefix(self):
        self.run_script(GANDALF_S3_BUCKET="b", GANDALF_S3_PREFIX="gandalf/")
        self.run_script(GANDALF_S3_BUCKET="b")

        uploads = [call for call in self.calls() if call.startswith("aws ")]
        self.assertIn(f"s3://b/gandalf/gandalf_translatorkg_{DATE}.tar.gz", uploads[0])
        self.assertIn(f"s3://b/gandalf_translatorkg_{DATE}.tar.gz", uploads[1])
        self.assertNotIn("--region", uploads[1])
        self.assertNotIn("--profile", uploads[1])

    def test_runs_again_and_repoints_latest(self):
        self.run_script(GANDALF_BUILD_DATE="10_08_2026")
        second = self.run_script(GANDALF_BUILD_DATE="10_09_2026")

        self.assertEqual(second.returncode, 0, second.stderr)
        latest = self.root / "processed" / "latest"
        self.assertEqual(latest.resolve(), (self.root / "processed" / "10_09_2026" / "graph_mmap").resolve())
        # -n matters: without it the new link would be created inside the old target
        self.assertFalse((self.root / "processed" / "10_08_2026" / "graph_mmap" / "graph_mmap").exists())

    def test_defaults_the_stamp_to_today(self):
        before = time.strftime("%m_%d_%Y")
        result = self.run_script(GANDALF_BUILD_DATE=None)
        after = time.strftime("%m_%d_%Y")

        self.assertEqual(result.returncode, 0, result.stderr)
        stamp = [line for line in result.stdout.splitlines() if line.startswith("Done.")][0].split(": ")[1]
        self.assertIn(stamp, {before, after})


class OptionalStepsTest(BuildGandalfScriptTest):
    def test_skips_the_upload_with_a_warning_when_no_bucket_is_set(self):
        self.remove_stub("aws")  # not needed, so not required either

        result = self.run_script()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("WARNING: GANDALF_S3_BUCKET is not set", result.stdout)
        self.assertFalse([call for call in self.calls() if call.startswith("aws ")])

    def test_an_explicitly_empty_service_skips_the_restart(self):
        self.remove_stub("sudo")
        self.remove_stub("systemctl")

        result = self.run_script(GANDALF_SERVICE="")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("skipping the service restart", result.stdout)
        self.assertFalse([call for call in self.calls() if call.startswith(("sudo", "systemctl"))])

    def test_restarts_a_custom_service(self):
        self.run_script(GANDALF_SERVICE="gandalf-test.service")

        self.assertIn("sudo -n systemctl restart gandalf-test.service", self.calls())

    def test_gandalf_build_mongo_can_be_given_as_a_full_path(self):
        elsewhere = self.tmp / "venv-bin"
        elsewhere.mkdir()
        (elsewhere / "gandalf-build-mongo").write_text((self.stubs / "gandalf-build-mongo").read_text())
        (elsewhere / "gandalf-build-mongo").chmod(0o755)
        self.remove_stub("gandalf-build-mongo")  # no longer on PATH

        result = self.run_script(GANDALF_BUILD_MONGO_BIN=str(elsewhere / "gandalf-build-mongo"))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.calls()[0].startswith("gandalf-build-mongo --mongo-uri"))


class FailureTest(BuildGandalfScriptTest):
    def test_each_required_variable_is_checked_before_anything_runs(self):
        for var in (
            "GANDALF_MONGO_URI",
            "GANDALF_MONGO_DB",
            "GANDALF_NODES_COLLECTION",
            "GANDALF_EDGES_COLLECTION",
        ):
            with self.subTest(var=var):
                result = self.run_script(**{var: None})

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(var, result.stderr)
                self.assertEqual(self.calls(), [])

    def test_a_missing_aws_cli_fails_before_the_long_build_starts(self):
        self.remove_stub("aws")

        result = self.run_script(GANDALF_S3_BUCKET="my-bucket")

        self.assertEqual(result.returncode, 1)
        self.assertIn("required command not found: aws", result.stderr)
        self.assertEqual(self.calls(), [], "the build must not have started")

    def test_a_missing_gandalf_build_mongo_says_how_to_fix_it(self):
        self.remove_stub("gandalf-build-mongo")

        result = self.run_script()

        self.assertEqual(result.returncode, 1)
        self.assertIn("required command not found: gandalf-build-mongo", result.stderr)
        self.assertIn("GANDALF_BUILD_MONGO_BIN", result.stderr)

    def test_a_missing_sudo_fails_up_front_when_a_restart_is_wanted(self):
        self.remove_stub("sudo")

        result = self.run_script()

        self.assertEqual(result.returncode, 1)
        self.assertIn("required command not found: sudo", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_a_failed_build_stops_before_compressing_restarting_or_uploading(self):
        result = self.run_script(STUB_BUILD_FAIL="1", GANDALF_S3_BUCKET="my-bucket")

        self.assertEqual(result.returncode, 3)
        self.assertIn("stub build exploded", result.stderr)
        self.assertIn("ERROR: build_gandalf.sh failed at line", result.stderr)
        self.assertFalse(self.archive.exists())
        self.assertFalse((self.root / "processed" / "latest").exists())
        self.assertFalse([call for call in self.calls() if call.startswith(("sudo", "aws"))])

    def test_a_failed_restart_stops_before_the_upload(self):
        self.add_stub("sudo", '#!/usr/bin/env bash\necho "sudo $*" >> "$STUB_LOG"\nexit 1\n')

        result = self.run_script(GANDALF_S3_BUCKET="my-bucket")

        self.assertEqual(result.returncode, 1)
        self.assertFalse([call for call in self.calls() if call.startswith("aws ")])


class SecretsTest(BuildGandalfScriptTest):
    def test_credentials_in_the_mongo_uri_are_not_printed(self):
        result = self.run_script(GANDALF_MONGO_URI="mongodb://hubuser:s3cret@db.example:27017")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("s3cret", result.stdout + result.stderr)
        self.assertIn("//***@db.example:27017", result.stdout)
        # ...but the build itself still gets the real URI
        self.assertIn("mongodb://hubuser:s3cret@db.example:27017", self.calls()[0])


if __name__ == "__main__":
    unittest.main()

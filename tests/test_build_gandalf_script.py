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
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "bin" / "build_gandalf.sh"
BASH = "/bin/bash" if os.path.exists("/bin/bash") else shutil.which("bash")
# what the script and the stubs themselves need, and nothing else
BASIC_TOOLS = (
    "bash", "env", "tar", "gzip", "gunzip", "ln", "mkdir", "date", "basename", "dirname", "sed", "cat", "rm", "id",
    "cp", "chmod", "git",
)

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
    # `sudo -n -l ...` is the preflight asking whether the restart is allowed, anything else is the restart itself
    "sudo": """#!/usr/bin/env bash
echo "sudo $*" >> "$STUB_LOG"
if [[ "$2" == "-l" ]]; then
    if [[ -n "${STUB_SUDO_DENIED:-}" ]]; then echo "Sorry, user may not run sudo for that command" >&2; exit 1; fi
    exit 0
fi
if [[ -n "${STUB_RESTART_FAIL:-}" ]]; then exit 1; fi
""",
    "systemctl": """#!/usr/bin/env bash
echo "systemctl $*" >> "$STUB_LOG"
if [[ "$1" == "cat" && -n "${STUB_NO_UNIT:-}" ]]; then echo "No files found for $2." >&2; exit 1; fi
""",
    "aws": """#!/usr/bin/env bash
echo "aws $*" >> "$STUB_LOG"
if [[ "$1" == "sts" ]]; then
    if [[ -n "${STUB_AWS_NO_CREDENTIALS:-}" ]]; then echo "Unable to locate credentials." >&2; exit 253; fi
    echo "arn:aws:iam::123456789012:user/dogpark"
fi
""",
}
DATE = "10_09_2026"
RELEASE = "2026_06_21"
VERSION = "v1.1.0"
ARCHIVE_NAME = f"gandalf_translatorkg_06_21_2026_{VERSION}.tar.gz"


@unittest.skipUnless(shutil.which("git"), "the script finds the Gandalf version with git")
class BuildGandalfScriptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="build-gandalf-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        # a Gandalf checkout at the lightweight tag v1.1.0 (like the real one), with the stubs where its
        # virtualenv keeps its commands, which is where the script looks for the version
        self.checkout = self.tmp / "gandalf-checkout"
        self.checkout.mkdir()
        self.git("init", "-q")
        self.git("commit", "-q", "--allow-empty", "-m", "release")
        self.git("tag", VERSION)
        self.stubs = self.checkout / ".venv" / "bin"
        self.stubs.mkdir(parents=True)
        self.toolbox = self.tmp / "toolbox"
        self.toolbox.mkdir()
        for tool in BASIC_TOOLS:
            real = BASH if tool == "bash" else shutil.which(tool)
            if real:
                (self.toolbox / tool).symlink_to(real)
        # the script parses graph-metadata.json with it when it can find one
        (self.toolbox / "python3").symlink_to(os.path.realpath(sys.executable))
        self.root = self.tmp / "gandalf"
        self.log = self.tmp / "stub.log"
        for name, body in STUBS.items():
            self.add_stub(name, body)

    def git(self, *args):
        """Run git in the fake Gandalf checkout, ignoring whatever git config the machine has."""
        settings = ["user.name=test", "user.email=test@example.org", "commit.gpgsign=false", "tag.gpgsign=false"]
        command = ["git", "-C", str(self.checkout)]
        for setting in settings:
            command += ["-c", setting]
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.tmp),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
        }
        subprocess.run([*command, *args], check=True, capture_output=True, env=env)

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
            "GANDALF_RELEASE": RELEASE,
            "GANDALF_MONGO_URI": "mongodb://db.example:27017",
            "GANDALF_MONGO_DB": "dogpark_src",
            "GANDALF_NODES_COLLECTION": "tier0_kg_nodes",
            "GANDALF_EDGES_COLLECTION": "tier0_kg_edges",
            # set but empty is how a run says "no upload on purpose"; unset is an error (see ConfigurationCheckTest)
            "GANDALF_S3_BUCKET": "",
        }
        env.update(overrides)
        env = {key: value for key, value in env.items() if value is not None}
        return subprocess.run([BASH, str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120)

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def build_calls(self):
        """The gandalf-build-mongo line and the TMPDIR line the stub logs right after it."""
        calls = self.calls()
        for index, call in enumerate(calls):
            if call.startswith("gandalf-build-mongo"):
                return calls[index : index + 2]
        return []

    @property
    def archive(self):
        """The archive of a run without a build name: the release date and the Gandalf version."""
        return self.root / "compacted" / ARCHIVE_NAME


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

        build_call, tmpdir_line = self.build_calls()
        self.assertEqual(
            build_call,
            "gandalf-build-mongo --mongo-uri mongodb://db.example:27017 --db dogpark_src "
            f"--nodes-collection tier0_kg_nodes --edges-collection tier0_kg_edges "
            f"--output {self.root}/processed/{DATE}/graph_mmap/",
        )
        # the build gets its own temp dir, as in the previous hand-run script
        self.assertEqual(tmpdir_line, f"TMPDIR={self.root}/tmp")

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
            f"aws s3 cp {self.archive} s3://my-bucket/gandalf/{ARCHIVE_NAME} "
            "--no-progress --region us-west-2 --profile gandalf-profile",
            self.calls(),
        )

    def test_s3_key_handles_a_trailing_slash_or_no_prefix(self):
        self.run_script(GANDALF_S3_BUCKET="my-bucket", GANDALF_S3_PREFIX="gandalf/")
        self.run_script(GANDALF_S3_BUCKET="my-bucket")

        uploads = [call for call in self.calls() if call.startswith("aws s3 cp")]
        self.assertIn(f"s3://my-bucket/gandalf/{ARCHIVE_NAME}", uploads[0])
        self.assertIn(f"s3://my-bucket/{ARCHIVE_NAME}", uploads[1])
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
    def test_an_explicitly_empty_bucket_skips_the_upload_on_purpose(self):
        self.remove_stub("aws")  # not needed, so not required either

        result = self.run_script(GANDALF_S3_BUCKET="")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("GANDALF_S3_BUCKET is empty, skipping the S3 upload", result.stdout)
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
        elsewhere = self.checkout / "other-venv" / "bin"  # still inside the checkout, where the version is read
        elsewhere.mkdir(parents=True)
        (elsewhere / "gandalf-build-mongo").write_text((self.stubs / "gandalf-build-mongo").read_text())
        (elsewhere / "gandalf-build-mongo").chmod(0o755)
        self.remove_stub("gandalf-build-mongo")  # no longer on PATH

        result = self.run_script(GANDALF_BUILD_MONGO_BIN=str(elsewhere / "gandalf-build-mongo"))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.build_calls()[0].startswith("gandalf-build-mongo --mongo-uri"))


class BuildNameTest(BuildGandalfScriptTest):
    """The hub's build name names the graph folder, and its id, the part after the last _, ends the archive name."""

    NAME = "tier0_kg_gandalf_20261012_wffqr2d5"
    ARCHIVE = f"gandalf_translatorkg_06_21_2026_{VERSION}_wffqr2d5.tar.gz"

    def test_the_build_name_names_the_graph_folder_and_its_id_ends_the_archive_and_the_s3_key(self):
        result = self.run_script(
            GANDALF_BUILD_NAME=self.NAME, GANDALF_S3_BUCKET="my-bucket", GANDALF_S3_PREFIX="gandalf"
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        graph = self.root / "processed" / self.NAME / "graph_mmap"
        self.assertEqual((graph / "graph.bin").read_text().strip(), "graph")
        archive = self.root / "compacted" / self.ARCHIVE
        with tarfile.open(archive) as tar:
            # whoever unpacks it still finds a graph_mmap/ folder, as before
            self.assertIn("graph_mmap/graph.bin", tar.getnames())
        self.assertEqual((self.root / "processed" / "latest").resolve(), graph.resolve())
        self.assertIn(f"aws s3 cp {archive} s3://my-bucket/gandalf/{self.ARCHIVE} --no-progress", self.calls())
        # the build date names neither the folder nor the archive
        self.assertFalse((self.root / "processed" / DATE).exists())
        self.assertFalse(self.archive.exists())

    def test_two_builds_on_the_same_day_keep_to_their_own_folders_and_archives(self):
        names = ("tier0_kg_gandalf_20261012_aaaaaaaa", "tier0_kg_gandalf_20261012_bbbbbbbb")

        results = [self.run_script(GANDALF_BUILD_NAME=name) for name in names]

        self.assertEqual([result.returncode for result in results], [0, 0], results[-1].stdout + results[-1].stderr)
        for name in names:
            self.assertTrue((self.root / "processed" / name / "graph_mmap" / "graph.bin").is_file(), name)
            archive = f"gandalf_translatorkg_06_21_2026_{VERSION}_{name.rsplit('_', 1)[1]}.tar.gz"
            self.assertTrue((self.root / "compacted" / archive).is_file(), archive)
        # latest follows the build that ran last
        latest = self.root / "processed" / "latest"
        self.assertEqual(latest.resolve(), (self.root / "processed" / names[1] / "graph_mmap").resolve())

    def test_a_build_name_without_an_underscore_is_its_own_id(self):
        result = self.run_script(GANDALF_BUILD_NAME="nightly")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        archive = self.root / "compacted" / f"gandalf_translatorkg_06_21_2026_{VERSION}_nightly.tar.gz"
        self.assertTrue(archive.is_file())

    def test_a_build_name_ending_in_an_underscore_still_gets_an_id(self):
        result = self.run_script(GANDALF_BUILD_NAME="nightly_")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        archive = self.root / "compacted" / f"gandalf_translatorkg_06_21_2026_{VERSION}_nightly_.tar.gz"
        self.assertTrue(archive.is_file())

    def test_without_a_build_name_the_date_based_names_stay(self):
        for unset in (None, ""):
            with self.subTest(build_name=unset):
                # each case starts from nothing, or it would pass on the files the case before it left behind
                shutil.rmtree(self.root, ignore_errors=True)

                result = self.run_script(GANDALF_BUILD_NAME=unset)

                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertTrue((self.root / "processed" / DATE / "graph_mmap" / "graph.bin").is_file())
                self.assertEqual(sorted(path.name for path in (self.root / "processed").iterdir()), [DATE, "latest"])
                self.assertEqual([path.name for path in (self.root / "compacted").iterdir()], [self.archive.name])

    def test_the_plan_names_the_build_and_its_files(self):
        result = self.run_script(GANDALF_BUILD_NAME=self.NAME)

        self.assertIn(f"Gandalf build {self.NAME}: dogpark_src.", result.stdout)
        self.assertIn(f"Output:   {self.root}/processed/{self.NAME}/graph_mmap", result.stdout)
        self.assertIn(f"Archive:  {self.root}/compacted/{self.ARCHIVE}", result.stdout)


class ArchiveNameTest(BuildGandalfScriptTest):
    """The archive is dated with the KGX release and carries the Gandalf version, the git tag of its checkout."""

    def run_and_find_archive(self, **overrides):
        result = self.run_script(**overrides)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result, sorted(path.name for path in (self.root / "compacted").iterdir())

    def test_the_release_dates_it_and_the_tag_is_the_version(self):
        result, archives = self.run_and_find_archive()

        self.assertEqual(archives, ["gandalf_translatorkg_06_21_2026_v1.1.0.tar.gz"])
        self.assertIn("Upstream KGX release: 2026_06_21", result.stdout)
        self.assertIn("Gandalf version: v1.1.0", result.stdout)

    def test_the_build_date_does_not_date_the_archive(self):
        _, archives = self.run_and_find_archive(GANDALF_BUILD_DATE="01_02_2027")

        self.assertEqual(archives, [ARCHIVE_NAME])

    def test_a_release_written_with_dashes_gives_the_same_name(self):
        _, archives = self.run_and_find_archive(GANDALF_RELEASE="2026-06-21")

        self.assertEqual(archives, [ARCHIVE_NAME])

    def test_an_annotated_tag_works_like_the_lightweight_one(self):
        self.git("tag", "-d", VERSION)
        self.git("tag", "-a", VERSION, "-m", "release")

        _, archives = self.run_and_find_archive()

        self.assertEqual(archives, [ARCHIVE_NAME])

    def test_a_checkout_ahead_of_the_tag_says_how_far(self):
        self.git("commit", "-q", "--allow-empty", "-m", "after the release")

        _, archives = self.run_and_find_archive()

        self.assertRegex(archives[0], rf"^gandalf_translatorkg_06_21_2026_{VERSION}-1-g[0-9a-f]+\.tar\.gz$")

    def test_local_changes_to_the_checkout_show_as_dirty(self):
        (self.checkout / "gandalf.py").write_text("print('released')\n")
        self.git("add", "gandalf.py")
        self.git("commit", "-q", "-m", "code")
        self.git("tag", "-f", VERSION)
        (self.checkout / "gandalf.py").write_text("print('edited on the server')\n")

        _, archives = self.run_and_find_archive()

        self.assertEqual(archives, [f"gandalf_translatorkg_06_21_2026_{VERSION}-dirty.tar.gz"])


class GraphMetadataTest(BuildGandalfScriptTest):
    """A graph-metadata.json, when given, is copied into the graph folder, so the archive carries it too."""

    DOCUMENT = '{\n  "name": "translator_kg",\n  "version": "2026_06_21"\n}'

    def setUp(self):
        super().setUp()
        self.metadata = self.tmp / "graph-metadata.json"
        self.metadata.write_text(self.DOCUMENT)

    def test_the_file_goes_into_the_graph_folder_and_the_archive(self):
        result = self.run_script(GANDALF_GRAPH_METADATA=str(self.metadata))

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        graph = self.root / "processed" / DATE / "graph_mmap"
        self.assertEqual((graph / "graph-metadata.json").read_text(), self.DOCUMENT)
        with tarfile.open(self.archive) as archive:
            self.assertEqual(archive.extractfile("graph_mmap/graph-metadata.json").read().decode(), self.DOCUMENT)

    def test_it_is_added_after_the_build_and_before_the_archive_is_made(self):
        result = self.run_script(GANDALF_GRAPH_METADATA=str(self.metadata))

        out = result.stdout
        self.assertLess(out.index("[1/5]"), out.index("Adding graph-metadata.json"))
        self.assertLess(out.index("Adding graph-metadata.json"), out.index("[2/5]"))

    def test_without_it_the_graph_folder_has_no_metadata_file(self):
        result = self.run_script()

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.root / "processed" / DATE / "graph_mmap" / "graph-metadata.json").exists())
        with tarfile.open(self.archive) as archive:
            self.assertNotIn("graph_mmap/graph-metadata.json", archive.getnames())

    def test_the_copy_is_readable_by_everyone_whatever_the_given_file_allows(self):
        self.metadata.chmod(0o600)

        self.run_script(GANDALF_GRAPH_METADATA=str(self.metadata))

        copy = self.root / "processed" / DATE / "graph_mmap" / "graph-metadata.json"
        self.assertEqual(copy.stat().st_mode & 0o777, 0o644)

    def test_the_plan_names_the_file_or_says_there_is_none(self):
        with_file = self.run_script(GANDALF_GRAPH_METADATA=str(self.metadata))
        without = self.run_script()

        self.assertIn(
            f"Metadata: {self.metadata} -> {self.root}/processed/{DATE}/graph_mmap/graph-metadata.json",
            with_file.stdout,
        )
        self.assertIn("Metadata: none (GANDALF_GRAPH_METADATA is not set)", without.stdout)

    def test_the_json_check_is_skipped_not_failed_on_a_host_without_python3(self):
        (self.toolbox / "python3").unlink()
        self.metadata.write_text("not json")

        result = self.run_script(GANDALF_GRAPH_METADATA=str(self.metadata))

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class ConfigurationCheckTest(BuildGandalfScriptTest):
    """The script refuses to start, having touched nothing, when it can't possibly finish."""

    def assert_refused(self, result, *needles):
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("Nothing was built, restarted or uploaded", result.stderr)
        for needle in needles:
            self.assertIn(needle, result.stderr)
        self.assertEqual(self.build_calls(), [], "the build must not have started")
        self.assertFalse(self.root.exists(), "nothing may be created before every check has passed")
        self.assertNotIn("[1/5]", result.stdout)

    def test_a_bucket_that_was_never_set_is_an_error_not_a_silent_skip(self):
        result = self.run_script(GANDALF_S3_BUCKET=None)

        self.assert_refused(result, "GANDALF_S3_BUCKET is not set", "empty string to skip the upload on purpose")
        self.assertEqual(self.calls(), [], "not even the environment checks run once the settings are wrong")

    def test_every_problem_is_reported_in_one_go(self):
        self.remove_stub("gandalf-build-mongo")

        result = self.run_script(
            GANDALF_MONGO_URI=None,
            GANDALF_MONGO_DB=None,
            GANDALF_NODES_COLLECTION=None,
            GANDALF_EDGES_COLLECTION=None,
            GANDALF_S3_BUCKET=None,
        )

        self.assert_refused(
            result,
            "GANDALF_MONGO_URI is not set",
            "GANDALF_MONGO_DB is not set",
            "GANDALF_NODES_COLLECTION is not set",
            "GANDALF_EDGES_COLLECTION is not set",
            "GANDALF_S3_BUCKET is not set",
            "required command not found: gandalf-build-mongo",
        )
        listed = [line for line in result.stderr.splitlines() if line.startswith("  - ")]
        self.assertEqual(len(listed), 6, result.stderr)

    def test_the_message_says_where_the_hub_settings_live(self):
        result = self.run_script(GANDALF_S3_BUCKET=None)

        self.assertIn("GANDALF_BUILD in config.py", result.stderr)
        self.assertIn('"s3": None', result.stderr)

    def test_values_that_cannot_work_are_refused_with_the_reason(self):
        empty = self.tmp / "empty.json"
        empty.write_text("")
        broken = self.tmp / "broken.json"
        broken.write_text('{"name": ')
        cases = {
            "mongo uri without a scheme": ({"GANDALF_MONGO_URI": "localhost:27017"}, "must start with mongodb://"),
            "bucket given as an s3 url": ({"GANDALF_S3_BUCKET": "s3://my-bucket"}, "is not a bucket name"),
            "bucket with capitals": ({"GANDALF_S3_BUCKET": "My_Bucket"}, "is not a bucket name"),
            "bucket with a path": ({"GANDALF_S3_BUCKET": "my-bucket/gandalf"}, "is not a bucket name"),
            "prefix with a leading slash": ({"GANDALF_S3_BUCKET": "my-bucket", "GANDALF_S3_PREFIX": "/gandalf"}, "GANDALF_S3_PREFIX"),
            "prefix given as an s3 url": ({"GANDALF_S3_BUCKET": "my-bucket", "GANDALF_S3_PREFIX": "s3://my-bucket/x"}, "GANDALF_S3_PREFIX"),
            "prefix with a space": ({"GANDALF_S3_BUCKET": "my-bucket", "GANDALF_S3_PREFIX": "my gandalf"}, "GANDALF_S3_PREFIX"),
            "region that is not one": ({"GANDALF_S3_BUCKET": "my-bucket", "GANDALF_S3_REGION": "US West 2"}, "is not a region name"),
            "date in the wrong format": ({"GANDALF_BUILD_DATE": "2026-10-09"}, "MM_DD_YYYY"),
            "relative root": ({"GANDALF_ROOT": "relative/dir"}, "must be an absolute path"),
            "build name that climbs out of its folder": ({"GANDALF_BUILD_NAME": "../escape"}, "GANDALF_BUILD_NAME"),
            "build name with a slash": ({"GANDALF_BUILD_NAME": "a/b"}, "GANDALF_BUILD_NAME"),
            "build name with a space": ({"GANDALF_BUILD_NAME": "has space"}, "GANDALF_BUILD_NAME"),
            "build name starting with a dash": ({"GANDALF_BUILD_NAME": "-rf"}, "GANDALF_BUILD_NAME"),
            "build name that is too long": ({"GANDALF_BUILD_NAME": "x" * 201}, "GANDALF_BUILD_NAME"),
            "graph metadata file that does not exist": (
                {"GANDALF_GRAPH_METADATA": str(self.tmp / "nope.json")},
                "not a readable, non-empty file",
            ),
            "graph metadata file that is empty": (
                {"GANDALF_GRAPH_METADATA": str(empty)},
                "not a readable, non-empty file",
            ),
            "graph metadata that is a folder": (
                {"GANDALF_GRAPH_METADATA": str(self.tmp)},
                "not a readable, non-empty file",
            ),
            "graph metadata file that is not JSON": ({"GANDALF_GRAPH_METADATA": str(broken)}, "is not valid JSON"),
            "release that is not set": ({"GANDALF_RELEASE": None}, "GANDALF_RELEASE is not set"),
            "release that is not a date": ({"GANDALF_RELEASE": "latest"}, "is not a date like 2026_06_21"),
            "release with a month that does not exist": (
                {"GANDALF_RELEASE": "2026_13_01"},
                "is not a date like 2026_06_21",
            ),
            "release with a day that does not exist": (
                {"GANDALF_RELEASE": "2026_06_32"},
                "is not a date like 2026_06_21",
            ),
        }
        for name, (overrides, expected) in cases.items():
            with self.subTest(case=name):
                result = self.run_script(**overrides)

                self.assert_refused(result, expected)
                # exactly the problem the case is about, so it can't pass for an unrelated reason
                listed = [line for line in result.stderr.splitlines() if line.startswith("  - ")]
                self.assertEqual(len(listed), 1, result.stderr)

    def test_a_bad_mongo_uri_is_refused_without_printing_its_credentials(self):
        result = self.run_script(GANDALF_MONGO_URI="hubuser:s3cret@db.example:27017")

        self.assert_refused(result, "must start with mongodb://")
        self.assertNotIn("s3cret", result.stdout + result.stderr)

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "root can write anywhere")
    def test_a_root_folder_nobody_can_write_to_is_caught_up_front(self):
        locked = self.tmp / "locked"
        locked.mkdir()
        locked.chmod(0o555)
        self.addCleanup(locked.chmod, 0o755)

        result = self.run_script(GANDALF_ROOT=str(locked / "gandalf"))

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("can't be created or written", result.stderr)
        self.assertEqual(self.build_calls(), [])
        self.assertFalse((locked / "gandalf").exists())

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "root can read anything")
    def test_a_graph_metadata_file_nobody_can_read_is_caught_up_front(self):
        locked = self.tmp / "locked.json"
        locked.write_text("{}")
        locked.chmod(0o000)
        self.addCleanup(locked.chmod, 0o600)

        result = self.run_script(GANDALF_GRAPH_METADATA=str(locked))

        self.assert_refused(result, "not a readable, non-empty file")

    def assert_refused_for_the_version(self, result, *needles):
        self.assert_refused(result, *needles)
        listed = [line for line in result.stderr.splitlines() if line.startswith("  - ")]
        self.assertEqual(len(listed), 1, result.stderr)

    def test_a_host_without_git_is_refused_because_the_archive_needs_the_version(self):
        (self.toolbox / "git").unlink()

        result = self.run_script()

        self.assert_refused_for_the_version(result, "required command not found: git")

    def test_gandalf_outside_a_git_checkout_is_refused(self):
        outside = self.tmp / "outside" / "bin"
        outside.mkdir(parents=True)
        shutil.copy(self.stubs / "gandalf-build-mongo", outside / "gandalf-build-mongo")

        result = self.run_script(GANDALF_BUILD_MONGO_BIN=str(outside / "gandalf-build-mongo"))

        self.assert_refused_for_the_version(
            result, "can't tell which Gandalf version", "git describe --tags", "inside a Gandalf git checkout"
        )

    def test_a_checkout_without_tags_is_refused(self):
        self.git("tag", "-d", VERSION)

        result = self.run_script()

        self.assert_refused_for_the_version(result, "can't tell which Gandalf version", "with its tags fetched")

    def test_a_tag_that_cannot_be_part_of_a_file_name_is_refused(self):
        self.git("tag", "-d", VERSION)
        self.git("tag", "release/1.1")

        result = self.run_script()

        self.assert_refused_for_the_version(result, "Gandalf version 'release/1.1'", "can't be part of a file name")

    def test_a_systemd_unit_that_does_not_exist_is_caught_before_the_build(self):
        result = self.run_script(STUB_NO_UNIT="1")

        self.assert_refused(result, "systemd has no unit called gandalf.service")

    def test_a_sudo_rule_that_does_not_cover_the_restart_is_caught_before_the_build(self):
        result = self.run_script(STUB_SUDO_DENIED="1")

        self.assert_refused(result, "sudo won't run 'systemctl restart gandalf.service'", "without a password")

    def test_aws_credentials_that_do_not_work_are_caught_before_the_build(self):
        result = self.run_script(GANDALF_S3_BUCKET="my-bucket", STUB_AWS_NO_CREDENTIALS="1")

        self.assert_refused(result, "AWS rejected the credentials", "Unable to locate credentials")
        self.assertFalse([call for call in self.calls() if call.startswith("aws s3")])

    def test_the_aws_check_asks_as_the_same_account_the_upload_will_use(self):
        self.run_script(GANDALF_S3_BUCKET="my-bucket", GANDALF_S3_REGION="us-west-2", GANDALF_S3_PROFILE="gandalf-profile")

        asked = [call for call in self.calls() if call.startswith("aws sts get-caller-identity")]
        self.assertEqual(len(asked), 1)
        self.assertIn("--region us-west-2 --profile gandalf-profile", asked[0])

    def test_the_aws_check_can_be_skipped_for_a_host_that_cannot_reach_sts(self):
        result = self.run_script(GANDALF_S3_BUCKET="my-bucket", STUB_AWS_NO_CREDENTIALS="1", GANDALF_SKIP_AWS_CHECK="1")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse([call for call in self.calls() if call.startswith("aws sts")])
        self.assertTrue([call for call in self.calls() if call.startswith("aws s3 cp")])

    def test_the_environment_is_checked_before_the_build_starts(self):
        self.run_script(GANDALF_S3_BUCKET="my-bucket")

        calls = self.calls()
        build = next(index for index, call in enumerate(calls) if call.startswith("gandalf-build-mongo"))
        before_the_build = calls[:build]
        self.assertIn("systemctl cat gandalf.service", before_the_build)
        self.assertIn("sudo -n -l systemctl restart gandalf.service", before_the_build)
        self.assertTrue([call for call in before_the_build if call.startswith("aws sts get-caller-identity")])
        # checking is not doing: the restart and the upload come after the build
        self.assertNotIn("sudo -n systemctl restart gandalf.service", before_the_build)
        self.assertFalse([call for call in before_the_build if call.startswith("aws s3")])

    def test_switched_off_steps_need_none_of_their_tools(self):
        for tool in ("aws", "sudo", "systemctl"):
            self.remove_stub(tool)

        result = self.run_script(GANDALF_S3_BUCKET="", GANDALF_SERVICE="")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual([call for call in self.calls() if not call.startswith(("gandalf-build-mongo", "TMPDIR"))], [])

    def test_the_plan_is_printed_before_the_build_starts(self):
        result = self.run_script(GANDALF_S3_BUCKET="my-bucket", GANDALF_S3_PREFIX="gandalf")

        out = result.stdout
        self.assertIn(f"Output:   {self.root}/processed/{DATE}/graph_mmap", out)
        self.assertIn("Restart:  gandalf.service", out)
        self.assertIn(
            f"Upload:   s3://my-bucket/gandalf/{ARCHIVE_NAME} (as arn:aws:iam::123456789012:user/dogpark)",
            out,
        )
        self.assertLess(out.index("Upload:"), out.index("[1/5]"))

    def test_the_plan_says_which_steps_are_switched_off(self):
        result = self.run_script(GANDALF_SERVICE="")  # the bucket is empty in the base environment

        self.assertIn("Restart:  skipped (GANDALF_SERVICE is empty)", result.stdout)
        self.assertIn("Upload:   skipped (GANDALF_S3_BUCKET is empty)", result.stdout)


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

        self.assertEqual(result.returncode, 2)
        self.assertIn("required command not found: aws", result.stderr)
        self.assertEqual(self.calls(), [], "the build must not have started")

    def test_a_missing_gandalf_build_mongo_says_how_to_fix_it(self):
        self.remove_stub("gandalf-build-mongo")

        result = self.run_script()

        self.assertEqual(result.returncode, 2)
        self.assertIn("required command not found: gandalf-build-mongo", result.stderr)
        self.assertIn("GANDALF_BUILD_MONGO_BIN", result.stderr)

    def test_a_missing_sudo_fails_up_front_when_a_restart_is_wanted(self):
        self.remove_stub("sudo")

        result = self.run_script()

        self.assertEqual(result.returncode, 2)
        self.assertIn("required command not found: sudo", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_a_failed_build_stops_before_compressing_restarting_or_uploading(self):
        result = self.run_script(STUB_BUILD_FAIL="1", GANDALF_S3_BUCKET="my-bucket")

        self.assertEqual(result.returncode, 3)
        self.assertIn("stub build exploded", result.stderr)
        self.assertIn("ERROR: build_gandalf.sh failed at line", result.stderr)
        self.assertFalse(self.archive.exists())
        self.assertFalse((self.root / "processed" / "latest").exists())
        self.assertFalse([call for call in self.calls() if call.startswith(("sudo -n systemctl", "aws s3"))])

    def test_a_failed_restart_stops_before_the_upload(self):
        result = self.run_script(STUB_RESTART_FAIL="1", GANDALF_S3_BUCKET="my-bucket")

        self.assertEqual(result.returncode, 1)
        self.assertFalse([call for call in self.calls() if call.startswith("aws s3")])


class SecretsTest(BuildGandalfScriptTest):
    def test_credentials_in_the_mongo_uri_are_not_printed(self):
        result = self.run_script(GANDALF_MONGO_URI="mongodb://hubuser:s3cret@db.example:27017")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("s3cret", result.stdout + result.stderr)
        self.assertIn("//***@db.example:27017", result.stdout)
        # ...but the build itself still gets the real URI
        self.assertIn("mongodb://hubuser:s3cret@db.example:27017", self.build_calls()[0])


if __name__ == "__main__":
    unittest.main()

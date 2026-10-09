"""Gandalf builder: a link build that runs ``bin/build_gandalf.sh`` after the merge.

Option D from ``GANDALF_INTEGRATION.md``, for a hub that runs on the same host
as Gandalf. ``GandalfBuilder`` is biothings' ``LinkDataBuilder`` (no data is
copied, the build points at the uploaded ``tier0_kg_edges`` collection), so the
build record, status, logs and ``_meta`` all behave like any other build. The
one addition is the ``post_merge`` hook, which runs the bash script that builds
the Gandalf graph, compacts it, updates the ``latest`` symlink, restarts the
Gandalf service and uploads the archive to S3. The script's output is streamed
line by line into this build's log, so it shows up in the hub while it runs.

Setup
-----
1. Create a build config that uses this class, once, from the hub console::

       create_build_conf(
           "tier0_kg_gandalf",          # any name
           "tier0_kg",                  # doc_type, free text
           ["tier0_kg_edges"],          # exactly one source: the edges collection
           builder_class="hub.databuild.gandalf_builder.GandalfBuilder",
       )

2. Optionally tune it in ``config.py`` (every key is optional)::

       GANDALF_BUILD = {
           "root": "/data/dogpark/gandalf",
           "gandalf_bin": "/opt/home/dogpark/gandalf/.venv/bin/gandalf-build-mongo",
           "service": "gandalf.service",   # None or "" skips the restart
           "s3": {"bucket": "my-bucket", "prefix": "gandalf",
                  "region": "us-west-2", "profile": None},
           "timeout": 6 * 3600,            # seconds before the script is killed
           "script": "/path/to/another/build_gandalf.sh",
       }

3. Run it like any other build: ``merge("tier0_kg_gandalf")``. It refuses to
   start until ``tier0_kg_edges`` has a successful upload, so it can never run
   against missing data.

The Mongo collections come from ``resolve_kg_build_backends()``, the same
function ``KGXIndexer`` uses, so the Gandalf graph is built from exactly the
collections the ES index of the same data is built from.
"""

import os
import signal
import subprocess
import threading
import time
from collections import deque
from pathlib import Path

from biothings.hub.databuild.builder import BuilderException, LinkDataBuilder
from biothings.utils.hub_db import get_src_dump

from hub.dataindex.indexer import resolve_kg_build_backends

DEFAULT_SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "build_gandalf.sh"
# A full build took tens of minutes on the real graph; this is only a backstop
# so a hung script can't hold a job-manager thread forever.
DEFAULT_TIMEOUT_SECONDS = 6 * 60 * 60
_KILL_GRACE_SECONDS = 10
_TAIL_LINES = 30


class GandalfBuildError(BuilderException):
    """The Gandalf build script could not be started, failed, or timed out."""


def gandalf_settings():
    """The ``GANDALF_BUILD`` dict from ``config.py``, or ``{}`` when it isn't defined."""
    import biothings

    settings = getattr(getattr(biothings, "config", None), "GANDALF_BUILD", None)
    return dict(settings or {})


def _mongo_uri(client_args):
    """``mongodb://host:port`` from the ``{"host", "port"}`` args biothings backends carry."""
    return f"mongodb://{client_args.get('host', 'localhost')}:{client_args.get('port', 27017)}"


def kgx_release_for(data_name):
    """The KGX release ``KgDumper`` recorded for this source, or ``None``.

    biothings keeps it at ``src_dump[<source>]["download"]["release"]`` (that is
    what ``BaseDumper.current_release`` reads), not at the top level of the
    record. Informational only (it is printed in the build log), so any lookup
    problem returns ``None`` instead of failing a build.
    """
    try:
        record = get_src_dump().find_one({"_id": data_name}) or {}
    except Exception:
        return None
    release = (record.get("download") or {}).get("release")
    return str(release) if release else None


def script_env(settings, edge_backend, node_backend, release=None):
    """The ``GANDALF_*`` variables ``build_gandalf.sh`` reads (see its header)."""
    if (edge_backend.args, edge_backend.dbs) != (node_backend.args, node_backend.dbs):
        raise GandalfBuildError(
            "gandalf-build-mongo reads nodes and edges from a single database, but this build keeps "
            f"edges in {edge_backend.dbs!r} and nodes in {node_backend.dbs!r}. Use a link build, "
            "which leaves both in the source database."
        )
    env = {
        "GANDALF_MONGO_URI": _mongo_uri(edge_backend.args),
        "GANDALF_MONGO_DB": edge_backend.dbs,
        "GANDALF_NODES_COLLECTION": node_backend.col,
        "GANDALF_EDGES_COLLECTION": edge_backend.col,
    }
    if release:
        env["GANDALF_RELEASE"] = str(release)
    if settings.get("root"):
        env["GANDALF_ROOT"] = str(settings["root"])
    if settings.get("gandalf_bin"):
        env["GANDALF_BUILD_MONGO_BIN"] = str(settings["gandalf_bin"])
    if "service" in settings:
        # the script reads a variable that is set but empty as "skip the restart"
        env["GANDALF_SERVICE"] = settings["service"] or ""
    s3 = settings.get("s3") or {}
    for key, var in (
        ("bucket", "GANDALF_S3_BUCKET"),
        ("prefix", "GANDALF_S3_PREFIX"),
        ("region", "GANDALF_S3_REGION"),
        ("profile", "GANDALF_S3_PROFILE"),
    ):
        if s3.get(key):
            env[var] = str(s3[key])
    return env


def _signal_group(proc, sig):
    """Signal the whole process group (the script and everything it started)."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, sig)  # start_new_session=True makes the pgid equal the pid
    except (ProcessLookupError, PermissionError):
        pass


def run_streaming(cmd, env, log, timeout=None, cwd=None):
    """Run ``cmd`` and hand each output line to ``log`` as it arrives.

    stdout and stderr are merged so lines keep their real order. Raises
    ``GandalfBuildError`` when the command can't start, exits non-zero, or runs
    longer than ``timeout`` seconds (its whole process group is terminated).
    """
    name = Path(cmd[0]).name
    try:
        proc = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            stdin=subprocess.DEVNULL,  # nothing can wait for input nobody will type
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=True,
        )
    except OSError as exc:
        raise GandalfBuildError(f"could not start {cmd[0]!r}: {exc}") from exc

    timed_out = threading.Event()
    tail = deque(maxlen=_TAIL_LINES)

    def terminate():
        timed_out.set()
        _signal_group(proc, signal.SIGTERM)
        deadline = time.monotonic() + _KILL_GRACE_SECONDS
        while proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.2)
        _signal_group(proc, signal.SIGKILL)

    timer = None
    if timeout:
        timer = threading.Timer(timeout, terminate)
        timer.daemon = True
        timer.start()
    try:
        for line in proc.stdout:
            line = line.rstrip("\r\n")
            tail.append(line)
            log(line)
        returncode = proc.wait()
    finally:
        if timer:
            timer.cancel()
        # only still running here if logging raised while the script was alive
        _signal_group(proc, signal.SIGKILL)
        proc.wait()
        proc.stdout.close()

    if timed_out.is_set():
        raise GandalfBuildError(f"{name} did not finish within {timeout}s and was terminated")
    if returncode != 0:
        raise GandalfBuildError(f"{name} exited with status {returncode}. Last output:\n" + "\n".join(tail))


class GandalfBuilder(LinkDataBuilder):
    """Link build that builds, compacts, deploys and uploads the Gandalf graph.

    Behaves like ``LinkDataBuilder`` (a link to the one uploaded source, no data
    copy), then runs ``bin/build_gandalf.sh`` as the post-merge step with the
    Mongo collections of this build, streaming its output into the build log.
    """

    def post_merge(self, source_names, batch_size, job_manager):
        try:
            return self._run_gandalf_build()
        except Exception as exc:
            # biothings logs a post-merge failure and re-raises it, but leaves the
            # build's job marked "building". Record the failure so the build list
            # tells the truth.
            self.register_status("failed", job={"err": repr(exc)})
            raise

    def _run_gandalf_build(self):
        settings = gandalf_settings()
        script = Path(settings.get("script") or DEFAULT_SCRIPT).expanduser()
        if not script.is_file():
            raise GandalfBuildError(f"Gandalf build script not found: {script}")

        build_doc = self.source_backend.build.find_one({"_id": self.target_name})
        edge_backend, node_backend, data_name = resolve_kg_build_backends(build_doc)
        release = kgx_release_for(data_name)
        env = {**os.environ, "PYTHONUNBUFFERED": "1", **script_env(settings, edge_backend, node_backend, release)}

        if not env.get("GANDALF_S3_BUCKET"):
            self.logger.warning(
                "No S3 bucket configured (GANDALF_BUILD['s3']['bucket']): the graph will be built but not uploaded"
            )
        self.logger.info(
            "Running %s: nodes=%s.%s edges=%s.%s KGX release=%s",
            script,
            node_backend.dbs,
            node_backend.col,
            edge_backend.dbs,
            edge_backend.col,
            release or "unknown",
        )
        run_streaming(
            ["bash", str(script)],
            env,
            lambda line: self.logger.info("[gandalf] %s", line),
            timeout=settings.get("timeout", DEFAULT_TIMEOUT_SECONDS),
        )
        self.logger.info("Gandalf build script finished")
        return {"gandalf_build": {"release": release, "nodes": node_backend.col, "edges": edge_backend.col}}

#!/usr/bin/env python3

"""Create snapshots, collect a release, and restore it on a selected cluster."""

from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING

from biothings.utils.es import ESIndexer

from hooks.dogpark_hook_utils.builds import (
    dogpark_select_latest_builds,
    get_available_builds,
    get_src_build_collection,
    load_build_documents,
)

__all__ = [
    "create_snapshots",
    "get_snapshots_release",
    "install_snapshots",
]


if TYPE_CHECKING:
    from biothings.hub.dataindex.snapshooter import SnapshotEnv, SnapshotManager
    from biothings.utils.hub import HubShell

    ssm: SnapshotManager
    hub: HubShell

logger = logging.getLogger("dogpark.snapshots")


def _validate_index_names(index_names):
    if isinstance(index_names, (str, bytes, dict)) or index_names is None:
        raise TypeError("index_names must be a list of index names")
    names = []
    for name in index_names:
        if not isinstance(name, str) or not name or name.startswith("_") or any(
            char in name for char in "*?,"
        ):
            raise ValueError("index_names must contain exact index names")
        if name in names:
            raise ValueError(f"Duplicate index in batch: {name}")
        names.append(name)
    return names


def _snapshot_state(client, repository, name):
    # Import lazily: importing biothings.hub can initialize the Hub configuration.
    from biothings.hub.dataindex.snapshot_task import Snapshot

    return Snapshot(client, repository, name).state()


def create_snapshots(snapshot_env: str, index_names: list[str] | None = None, *, dry_run: bool = False):
    """Snapshot explicit indices or the latest ready builds; dry_run only previews."""

    snapshot_environment: SnapshotEnv = ssm.register[snapshot_env]
    indexer_env = snapshot_environment.idxenv

    # setting default targets
    if index_names is None:
        index_names = get_available_builds(indexer_env)
    index_names = _validate_index_names(index_names)

    if not index_names:
        logger.info("No indices selected for snapshot creation")
        return {"snapshot_env": snapshot_env, "dry_run": True, "indices": []} if dry_run else []

    client = snapshot_environment.client

    # check if underlying index exists on the cluster
    response = client.indices.get(
        index=index_names,
        features=["settings", "aliases"],
        ignore_unavailable=True,
        allow_no_indices=True,
    )

    existing = response.body
    if set(existing) - set(index_names):
        raise ValueError("Pass concrete index names, not aliases")
    documents = get_src_build_collection().find(
        {"_id": {"$in": sorted(existing)}},
        {"_id": 1, "build_config": 1, "index": 1, "_meta": 1},
    ) if existing else []
    builds = {document["_id"]: document for document in documents}
    preview = []
    for name in index_names:
        row = {"index_name": name}
        preview.append(row)
        if name not in existing:
            row.update(action="skip", reason="index missing on source cluster")
            continue
        build = builds.get(name)
        if not build or build.get("index", {}).get(name, {}).get("environment") != indexer_env:
            row.update(action="blocked", reason=f"no Hub index registration under {indexer_env}")
            continue
        config = snapshot_environment.repcfg.format(build)
        row["repository"] = config.repo
        row["aliases"] = sorted(existing[name].get("aliases", {}))
        if str(config.data.get("settings", {}).get("readonly", False)).lower() == "true":
            row.update(action="blocked", reason="repository is read-only")
            continue
        state = _snapshot_state(client, config.repo, name)
        row["snapshot_state"] = state
        if state == "IN_PROGRESS":
            row.update(action="blocked", reason="snapshot is already in progress")
        else:
            row["action"] = "create" if state in {"MISSING", "N/A"} else "replace"

    if dry_run:
        return {"snapshot_env": snapshot_env, "indexer_env": indexer_env, "dry_run": True, "indices": preview}
    blocked = [row for row in preview if row["action"] == "blocked"]
    if blocked:
        raise ValueError(f"Cannot create snapshots: {blocked}")
    for row in preview:
        if row["action"] == "skip":
            logger.warning("Skipping %s: %s", row["index_name"], row["reason"])
    return [
        ssm.snapshot(snapshot_env, row["index_name"])
        for row in preview if row["action"] in {"create", "replace"}
    ]


def _complete_snapshot(client, repository: str, snapshot_name: str, index_name: str):
    """Read one complete snapshot; missing or unfinished snapshots return None."""
    snapshots = client.snapshot.get(
        repository=repository,
        snapshot=snapshot_name,
        ignore_unavailable=True,
    ).body["snapshots"]
    if not snapshots:
        return None
    if len(snapshots) != 1 or snapshots[0]["snapshot"] != snapshot_name:
        raise ValueError(f"Expected exactly one snapshot named {snapshot_name!r}")

    snapshot = snapshots[0]
    if snapshot["state"] != "SUCCESS" or snapshot["shards"]["failed"]:
        return None
    if snapshot["indices"] != [index_name]:
        raise ValueError(f"Snapshot {snapshot_name!r} does not contain exactly {index_name!r}")
    return snapshot


def get_snapshots_release(snapshot_env: str, index_names: list[str] | None = None) -> list[dict]:
    """Return completed snapshots for the selected builder indices.

    Defaults to the latest ready builds, like create_snapshots. Missing or
    unfinished snapshots are skipped. This is an optional inspection step;
    installation takes index names and looks up the snapshots itself.
    """
    snapshot_environment: SnapshotEnv = ssm.register[snapshot_env]
    if index_names is None:
        index_names = get_available_builds(snapshot_environment.idxenv)
    if not index_names:
        return []
    if isinstance(index_names, str):
        raise TypeError("index_names must be a list of index names")

    documents = get_src_build_collection().find(
        {"_id": {"$in": index_names}},
        {"_id": 1, "build_config": 1},
    )
    builds = {document["_id"]: document for document in documents}
    missing = set(index_names) - builds.keys()
    if missing:
        raise ValueError(f"No build metadata for indices: {sorted(missing)}")

    release = []
    for name in sorted(set(index_names)):
        build = builds[name]
        repository = snapshot_environment.repcfg.format(build).repo
        snapshot = _complete_snapshot(snapshot_environment.client, repository, name, name)
        if snapshot is None:
            logger.warning("No completed snapshot for %s in %s", name, repository)
            continue
        release.append({
            "build_id": name,
            "build_config": build["build_config"],
            "index_name": name,
            "repository": repository,
            "snapshot": snapshot["snapshot"],
            "snapshot_uuid": snapshot["uuid"],
        })

    logger.info("Found %s completed snapshots in %s", len(release), snapshot_env)
    return release


def install_snapshots(snapshot_env: str, index_names: list[str] | None = None, *, dry_run: bool = False):
    """Restore the completed snapshots named after these indices on the target.

    Omit index_names to discover snapshots for the newest DogPark builds in the
    target repository, skipping missing or incomplete snapshots without falling
    back to older builds. Explicit lists require every snapshot to be complete;
    an empty list does nothing. Discovery needs no source indexer environment.

    Returns Hub tasks that finish after primary-shard recovery. Target
    repository settings must resolve without build metadata and already expose
    the source snapshots. Explicit lists need no Hub build lookup.
    Existing indices are rejected. BioThings restores the snapshot's settings and
    aliases, excluding global cluster state. dry_run only returns the preflight.
    """
    snapshot_environment: SnapshotEnv = ssm.register[snapshot_env]
    auto_detect = index_names is None
    if auto_detect:
        selection = dogpark_select_latest_builds(load_build_documents())
        index_names = [candidate["build_id"] for candidate in selection["selected"]]
        if selection["pending"]:
            logger.warning("Builds omitted from snapshot discovery: %s", selection["pending"])
    index_names = _validate_index_names(index_names)
    if not index_names:
        return {"snapshot_env": snapshot_env, "dry_run": True, "indices": []} if dry_run else []
    client = snapshot_environment.client

    # Check the entire batch before submitting any restores.
    repository = snapshot_environment.repcfg.format().repo
    preview = []
    for name in index_names:
        row = {"index_name": name, "repository": repository, "action": "restore"}
        preview.append(row)
        try:
            snapshot = _complete_snapshot(client, repository, name, name)
        except ValueError as exc:
            row.update(action="blocked", reason=str(exc))
        else:
            if snapshot is None:
                row.update(action="skip" if auto_detect else "blocked", reason="snapshot is missing or incomplete")
                if auto_detect:
                    logger.warning("Skipping %s: %s", name, row["reason"])
            else:
                row["snapshot_uuid"] = snapshot["uuid"]

    # Skipped discoveries are not restore targets. Never query ES with an empty
    # list, which can resolve to every index rather than none.
    targets = [row["index_name"] for row in preview if row["action"] != "skip"]
    existing = client.indices.get(
        index=targets,
        features="aliases",
        expand_wildcards="all",
        ignore_unavailable=True,
        allow_no_indices=True,
    ).body if targets else {}
    conflicts = set(existing)
    for info in existing.values():
        conflicts.update(info.get("aliases", {}))
    for row in preview:
        if row["index_name"] in conflicts:
            row.update(action="blocked", reason="destination index or alias already exists")
    if dry_run:
        return {
            "snapshot_env": snapshot_env, "indexer_env": snapshot_environment.idxenv,
            "dry_run": True, "restore_aliases": True, "indices": preview,
        }
    blocked = [row for row in preview if row["action"] == "blocked"]
    if blocked or existing:
        raise ValueError(f"Cannot install snapshots: {blocked or sorted(existing)}")
    if not targets:
        return []

    indexer_args = ssm.index_manager[snapshot_environment.idxenv]["args"]

    async def work(name, step, func):
        pinfo = snapshot_environment.pinfo.get_pinfo(step, name)
        job = await snapshot_environment.job_manager.defer_to_thread(pinfo, func)
        return await job

    async def restore(name):
        logger.info("Restoring %s from %s on %s", name, repository, snapshot_env)
        indexer = await work(name, "restore", partial(
            ESIndexer, index=name, es_host=indexer_args["hosts"], check_index=False,
            timeout=indexer_args.get("request_timeout", indexer_args.get("timeout", 300)),
            max_retries=0, retry_on_timeout=False,
        ))
        try:
            result = await work(name, "restore", partial(
                indexer.restore, repository, name, index_name=name, purge=False,
            ))
            if not result.get("accepted"):
                raise RuntimeError(f"Restore was not accepted for {name}: {result}")
            failures = 0
            while True:
                try:
                    status = await work(name, "recovery", partial(indexer.get_restore_status, name))
                except Exception as exc:
                    # Retry monitoring only; never resubmit an uncertain restore request.
                    failures += 1
                    if failures >= 3:
                        raise
                    logger.warning("Recovery check failed for %s; retrying: %s", name, exc)
                    await asyncio.sleep(snapshot_environment.wtime)
                    continue
                failures = 0
                logger.info("Recovery for %s: %s", name, status)
                if status["status"] == "DONE":
                    return {"index_name": name, "repository": repository, "snapshot": name, **status}
                if status["status"] not in {"INIT", "IN_PROGRESS"}:
                    raise RuntimeError(f"Restore failed for {name}: {status}")
                await asyncio.sleep(snapshot_environment.wtime)
        finally:
            # ESIndexer owns this client but does not expose a close() method.
            indexer._es.close()

    return [
        snapshot_environment.job_manager.loop.create_task(restore(name))
        for name in targets
    ]


# BioThings 1.3 registers __all__; older Hubs need explicit registration.
if "hub" in globals() and not hasattr(getattr(hub, "server", None), "terminal"):
    for name in __all__:
        hub.commands[name] = globals()[name]
        hub.hidden[name] = False

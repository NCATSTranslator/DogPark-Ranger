#!/usr/bin/env python3

"""Create snapshots, collect a release, and restore it on a selected cluster."""

from __future__ import annotations

import asyncio
import logging
import math
from functools import partial
from typing import TYPE_CHECKING

from hooks.dogpark_hook_utils import aliases as _snapshot_aliases
from hooks.dogpark_hook_utils import builds as _snapshot_builds

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
        index_names = _snapshot_builds.get_available_builds(indexer_env)
    index_names = _snapshot_aliases.validate_index_names(index_names)

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
    documents = _snapshot_builds.get_src_build_collection().find(
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
    if not _snapshot_complete(snapshot):
        return None
    if snapshot["indices"] != [index_name]:
        raise ValueError(f"Snapshot {snapshot_name!r} does not contain exactly {index_name!r}")
    return snapshot


def _snapshot_complete(snapshot):
    return snapshot["state"] == "SUCCESS" and not snapshot["shards"]["failed"]


def get_snapshots_release(snapshot_env: str, index_names: list[str] | None = None) -> list[dict]:
    """Inspect completed snapshots in the environment's repository, without build lookup.

    Omit index_names to list all completed snapshots containing exactly one
    index with the same name as the snapshot, including older builds. An
    explicit list filters by those exact names; an empty list does nothing.
    Missing, incomplete, or incompatible snapshots are omitted. Repository
    settings must resolve without build metadata, as for installation.

    This is read-only inspection. Installation takes index names directly
    and retains its own newest-build discovery when no list is supplied.
    """
    snapshot_environment: SnapshotEnv = ssm.register[snapshot_env]
    if index_names is not None:
        index_names = _snapshot_aliases.validate_index_names(index_names)
        if not index_names:
            return []

    repository = snapshot_environment.repcfg.format().repo
    snapshots = snapshot_environment.client.snapshot.get(
        repository=repository,
        snapshot="_all" if index_names is None else index_names,
        ignore_unavailable=True,
    ).body["snapshots"]
    if index_names is not None:
        missing = set(index_names) - {snapshot["snapshot"] for snapshot in snapshots}
        for name in sorted(missing):
            logger.warning("No snapshot for %s in %s", name, repository)

    release = []
    for snapshot in sorted(snapshots, key=lambda item: item["snapshot"]):
        name = snapshot["snapshot"]
        if not _snapshot_complete(snapshot):
            logger.warning("No completed snapshot for %s in %s", name, repository)
            continue
        if snapshot["indices"] != [name]:
            logger.warning("Skipping snapshot %s in %s: expected exactly the index %s", name, repository, name)
            continue
        release.append({
            "index_name": name,
            "repository": repository,
            "snapshot": name,
            "snapshot_uuid": snapshot["uuid"],
        })

    logger.info("Found %s completed snapshots in %s", len(release), snapshot_env)
    return release


def install_snapshots(
    snapshot_env: str,
    index_names: list[str] | None = None,
    *,
    dry_run: bool = False,
    stage: bool = True,
    sources: dict[str, str] | None = None,
    alias_profile: str = "default",
    recovery_timeout: float = 3600,
):
    """Restore the completed snapshots named after these indices on the target.

    Omit index_names to discover snapshots for the newest DogPark builds in the
    target repository, skipping missing or incomplete snapshots without falling
    back to older builds. Explicit lists require every snapshot to be complete;
    an empty list does nothing. Discovery needs no source indexer environment.

    Auto-stage the batch after every restore has recovered all primary shards.
    Set stage=False to install without changing target aliases. Custom names
    require sources={index_name: datasource} when staging is enabled.
    alias_profile selects the target alias set; "testing" isolates test aliases.
    It has no effect when stage=False.

    Returns one Hub batch task containing per-index results and the staging
    result. It finishes after all recoveries and staging. A failed restore leaves
    staging unchanged. recovery_timeout limits recovery monitoring per index.
    Target repository settings must resolve without build metadata and expose
    the source snapshots. Explicit lists need no Hub build lookup.
    Existing indices are rejected. Restore preserves snapshot settings but
    excludes source aliases and global cluster state. dry_run returns the
    restore and staging preflight without submitting jobs.
    """
    if not isinstance(dry_run, bool) or not isinstance(stage, bool):
        raise TypeError("dry_run and stage must be booleans")
    if (
        isinstance(recovery_timeout, bool)
        or not isinstance(recovery_timeout, (int, float))
        or not math.isfinite(recovery_timeout)
        or recovery_timeout <= 0
    ):
        raise ValueError("recovery_timeout must be a positive number of seconds")
    snapshot_environment: SnapshotEnv = ssm.register[snapshot_env]
    auto_detect = index_names is None
    if auto_detect:
        selection = _snapshot_builds.dogpark_select_latest_builds(_snapshot_builds.load_build_documents())
        index_names = [candidate["build_id"] for candidate in selection["selected"]]
        if selection["pending"]:
            logger.warning("Builds omitted from snapshot discovery: %s", selection["pending"])
    index_names = _snapshot_aliases.validate_index_names(index_names)
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
                row["primary_shards"] = snapshot["shards"]["total"]
                if not isinstance(row["primary_shards"], int) or row["primary_shards"] <= 0:
                    row.update(action="blocked", reason="snapshot has no primary shards")

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

    ready = [row["index_name"] for row in preview if row["action"] == "restore"]
    expected_staging = None
    staging_preview = None
    if stage and ready:
        staging_preview = _snapshot_aliases.stage_indices(
            client, ready, sources=sources, alias_profile=alias_profile, dry_run=True,
        )
        expected_staging = staging_preview["staging_state"]
    if dry_run:
        return {
            "snapshot_env": snapshot_env, "indexer_env": snapshot_environment.idxenv,
            "dry_run": True, "restore_aliases": False, "stage": stage, "indices": preview,
            "alias_profile": alias_profile, "expected_staging": expected_staging, "staging": staging_preview,
        }
    blocked = [row for row in preview if row["action"] == "blocked"]
    if blocked or existing:
        raise ValueError(f"Cannot install snapshots: {blocked or sorted(existing)}")
    if not targets:
        return []

    async def work(name, step, func):
        pinfo = snapshot_environment.pinfo.get_pinfo(step, name)
        job = await snapshot_environment.job_manager.defer_to_thread(pinfo, func)
        return await job

    async def restore(row):
        name = row["index_name"]
        logger.info("Restoring %s from %s on %s", name, repository, snapshot_env)

        def submit_restore():
            # Check identity again in the same worker immediately before submission.
            current = _complete_snapshot(client, repository, name, name)
            if current is None or current["uuid"] != row["snapshot_uuid"]:
                raise ValueError(f"Snapshot changed after preflight for {name}")
            return client.options(max_retries=0, retry_on_timeout=False).snapshot.restore(
                repository=repository, snapshot=name, indices=name,
                include_aliases=False, include_global_state=False,
            ).body

        result = await work(name, "restore", submit_restore)
        if not result.get("accepted"):
            raise RuntimeError(f"Restore was not accepted for {name}: {result}")
        loop = snapshot_environment.job_manager.loop
        deadline = loop.time() + recovery_timeout
        failures = 0
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(f"Primary-shard recovery timed out for {name}")
            try:
                health = await asyncio.wait_for(work(name, "recovery", partial(
                    client.cluster.health, index=name, level="indices",
                )), timeout=remaining)
                info = health.body.get("indices", {}).get(name, {})
            except asyncio.TimeoutError:
                raise TimeoutError(f"Primary-shard recovery timed out for {name}") from None
            except Exception as exc:
                # Retry monitoring only; never resubmit an uncertain restore request.
                failures += 1
                if failures >= 3:
                    raise
                logger.warning("Recovery check failed for %s; retrying: %s", name, exc)
            else:
                failures = 0
                logger.info("Primary-shard recovery for %s: %s", name, info)
                if (
                    info.get("number_of_shards") == row["primary_shards"]
                    and info.get("active_primary_shards") == row["primary_shards"]
                    and info.get("status") in {"yellow", "green"}
                ):
                    return {
                        "index_name": name, "repository": repository, "snapshot": name,
                        "snapshot_uuid": row["snapshot_uuid"], "status": "DONE", "progress": "100%",
                    }
            await asyncio.sleep(min(snapshot_environment.wtime, max(0, deadline - loop.time())))

    async def install_batch():
        results = await asyncio.gather(
            *(restore(row) for row in preview if row["action"] == "restore"),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        staging_result = None
        if stage:
            staging_result = await work("batch", "staging", partial(
                _snapshot_aliases.stage_indices, client, targets, sources=sources,
                alias_profile=alias_profile, expected_staging=expected_staging,
            ))
        return {
            "indices": [{**result, "staged": stage} for result in results],
            "staged": stage, "staging": staging_result,
        }

    return [snapshot_environment.job_manager.loop.create_task(install_batch())]


# BioThings 1.3 registers __all__; older Hubs need explicit registration.
if "hub" in globals() and not hasattr(getattr(hub, "server", None), "terminal"):
    for name in __all__:
        hub.commands[name] = globals()[name]
        hub.hidden[name] = False

"""Reusable build discovery and index-registration helpers for DogPark hooks.

No release policy, HTTP transport or snapshot operations live here. Hub access
is limited to get_src_build_collection() and load_build_documents(); other helpers
operate on supplied metadata. resolve_build_index() can use a caller-supplied probe.
"""

from __future__ import annotations

import datetime

__all__ = [
    "get_available_builds",
    "dogpark_select_latest_builds",
    "get_src_build_collection",
    "load_build_documents",
    "select_latest_builds",
    "build_config_name",
    "parse_started_at",
    "public_candidate",
    "with_reason",
    "build_status",
    "has_stale_cancelled_post_index",
    "check_build_readiness",
    "target_index_candidates",
    "resolve_build_index",
    "DOGPARK_RELEASE_DATASETS",
    "DOGPARK_NO_STAGING_TAG_DATASETS",
    "DOGPARK_MANAGED_DATASETS",
]


DOGPARK_RELEASE_DATASETS = (
    "alliance",
    "bgee",
    "bindingdb",
    "chembl",
    "cohd",
    "ctd",
    "ctkp",
    "dakp",
    "dgidb",
    "diseases",
    "drug_rep_hub",
    "drugcentral",
    "gene2phenotype",
    "geneticskp",
    "go_cam",
    "goa",
    "gtopdb",
    "hpoa",
    "icees",
    "intact",
    "panther",
    "pathbank",
    "semmeddb",
    "sider",
    "signor",
    "tmkp",
    "ttd",
    "ubergraph",
)

# These are built and released with custom prod aliases, but release-staging
# currently gives them no staging alias.
DOGPARK_NO_STAGING_TAG_DATASETS = (
    "ubergraph_nodes_info",
    "ubergraph_nodes",
)


DOGPARK_MANAGED_DATASETS = (
        DOGPARK_RELEASE_DATASETS + DOGPARK_NO_STAGING_TAG_DATASETS
)

def get_src_build_collection():
    """Return the active Hub build collection; importing this module needs no Hub."""
    from biothings.utils.hub_db import get_src_build

    return get_src_build()


def load_build_documents(collection=None) -> list[dict]:
    """Read projected build metadata from the Hub or a supplied collection."""
    if collection is None:
        collection = get_src_build_collection()
    projection = {
        "_id": 1,
        "target_name": 1,
        "build_config._id": 1,
        "build_config.name": 1,
        "started_at": 1,
        "archived": 1,
        "jobs": 1,
        "index": 1,
    }
    documents = list(collection.find({}, projection))
    if not all(isinstance(document, dict) for document in documents):
        raise TypeError("src_build returned a non-object build document")
    return documents


def parse_started_at(value) -> tuple[float, str] | None:
    parsed = None
    if isinstance(value, datetime.datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            parsed = datetime.datetime.fromisoformat(
                text[:-1] + "+00:00" if text.endswith("Z") else text
            )
        except ValueError:
            return None

    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    normalized = parsed.astimezone(datetime.timezone.utc)
    return normalized.timestamp(), normalized.isoformat().replace("+00:00", "Z")


def _candidate_from_item(
    config_name: str,
    item,
) -> tuple[dict | None, str | None]:
    if not isinstance(item, dict):
        return None, "build item is not an object"

    build_id = item.get("_id")
    if not isinstance(build_id, str) or not build_id.strip():
        return None, "build item has no string _id"
    build_id = build_id.strip()

    target_name = item.get("target_name") or build_id
    if not isinstance(target_name, str) or not target_name.strip():
        return None, f"build {build_id!r} has no target_name"
    target_name = target_name.strip()

    started_at = parse_started_at(item.get("started_at"))
    if started_at is None:
        return None, f"build {build_id!r} has no valid started_at"
    timestamp, normalized_started_at = started_at

    return (
        {
            "config": config_name,
            "build_id": build_id,
            "target_name": target_name,
            "started_at": normalized_started_at,
            # BioThings treats presence of this field as archived, regardless
            # of the stored marker value.
            "archived": "archived" in item,
            "_timestamp": timestamp,
            "_build_document": item,
        },
        None,
    )


def public_candidate(candidate: dict) -> dict:
    return {key: value for key, value in candidate.items() if not key.startswith("_")}


def with_reason(candidate: dict, reason: str, **details) -> dict:
    result = public_candidate(candidate)
    result["reason"] = reason
    result.update(details)
    return result


def select_latest_builds(
    raw_builds: list[dict],
    datasets,
    *,
    normalize_config_names: bool = False,
) -> dict:
    """Select the newest build per dataset before checking readiness.

    Return ``selected`` candidates and ``pending`` issues. Each candidate
    carries its original document in ``_build_document``. Invalid history
    or tied newest timestamps are reported, never silently skipped.
    ``normalize_config_names`` preserves the release staging selector
    behavior of trimming configuration names; exact matching is default.
    No datasource policy, database access or Elasticsearch calls occur here.
    """
    datasets = tuple(datasets)
    groups = {}
    for build_document in raw_builds:
        if not isinstance(build_document, dict):
            continue
        build_config = build_document.get("build_config")
        if not isinstance(build_config, dict):
            continue
        config_name = build_config.get("_id") or build_config.get("name")
        if normalize_config_names and isinstance(config_name, str):
            config_name = config_name.strip()
        if isinstance(config_name, str) and config_name in datasets:
            groups.setdefault(config_name, []).append(build_document)

    selected = []
    pending = []
    for config_name in datasets:
        items = groups.get(config_name)
        if items is None:
            pending.append({"config": config_name, "reason": "missing_build_config"})
            continue
        if not items:
            pending.append({"config": config_name, "reason": "no_builds"})
            continue

        ordered_items = []
        ordering_errors = []
        for item in items:
            started_at = parse_started_at(item.get("started_at"))
            if started_at is None:
                ordering_errors.append(
                    f"build {item.get('_id')!r} has no valid started_at"
                )
                continue
            timestamp, normalized_started_at = started_at
            ordered_items.append((timestamp, normalized_started_at, item))

        if ordering_errors:
            pending.append(
                {
                    "config": config_name,
                    "reason": "malformed_build_history",
                    "details": ordering_errors,
                }
            )
            continue
        if not ordered_items:
            pending.append({"config": config_name, "reason": "no_valid_builds"})
            continue

        latest_timestamp = max(item[0] for item in ordered_items)
        latest_items = [item for item in ordered_items if item[0] == latest_timestamp]
        if len(latest_items) != 1:
            pending.append(
                {
                    "config": config_name,
                    "reason": "ambiguous_newest_build",
                    "build_ids": sorted(
                        item[2].get("_id")
                        for item in latest_items
                        if isinstance(
                            item[2].get("_id"),
                            str,
                        )
                    ),
                    "started_at": latest_items[0][1],
                }
            )
            continue

        candidate, error = _candidate_from_item(
            config_name,
            latest_items[0][2],
        )
        if error:
            pending.append(
                {
                    "config": config_name,
                    "reason": "malformed_newest_build",
                    "details": [error],
                }
            )
            continue
        selected.append(candidate)

    return {"selected": selected, "pending": pending}


def build_config_name(build_document: dict) -> str | None:
    build_config = build_document.get("build_config")
    if not isinstance(build_config, dict):
        return None
    config_name = build_config.get("_id") or build_config.get("name")
    if not isinstance(config_name, str) or not config_name.strip():
        return None
    return config_name.strip()


def build_status(build_document: dict) -> str:
    """Return the final recorded job status, not proof of index completion."""
    jobs = build_document.get("jobs")
    if isinstance(jobs, list) and jobs and isinstance(jobs[-1], dict):
        status = jobs[-1].get("status")
        if isinstance(status, str) and status:
            return status
    return "unknown"


def has_stale_cancelled_post_index(
    build_document: dict,
    index_name: str,
) -> bool:
    """Recognize the BioThings startup-pruned no-op post-index edge case.

    A legacy BioThings deployment can leave the final ``post-index`` job marked
    ``cancelled`` when the hub restarts after the concrete index was completed.
    DogPark's indexer does not implement post-index work, so accept only the
    narrow shape where that cancelled record immediately follows a successful
    main index. Any recorded error, different step ordering, or different
    status remains non-ready.
    """
    jobs = build_document.get("jobs")
    if (
        not isinstance(jobs, list)
        or len(jobs) < 2
        or not isinstance(jobs[-2], dict)
        or not isinstance(jobs[-1], dict)
    ):
        return False

    index_job = jobs[-2]
    post_index_job = jobs[-1]
    index_logfile = index_job.get("logfile")
    expected_logfile_name = f"index_{index_name}.log"
    return (
        index_job.get("step") == "index"
        and index_job.get("status") == "success"
        and isinstance(index_logfile, str)
        and bool(index_logfile)
        and index_logfile.rsplit("/", 1)[-1] == expected_logfile_name
        and post_index_job.get("step") == "post-index"
        and post_index_job.get("status") == "cancelled"
        and post_index_job.get("logfile") == index_logfile
        and "err" not in post_index_job
        and "error" not in post_index_job
        and "pid" not in post_index_job
    )


def target_index_candidates(
    build_document: dict,
    expected_indexer_env: str,
    *,
    allow_legacy_environment: bool = True,
) -> tuple[list[str], str | None]:
    """Resolve registered names without consulting Elasticsearch.

    Prefer exact-environment registrations. Optionally accept registrations
    with no environment only when no exact match exists (legacy behavior).
    ``target_name`` and the build ID are never used as index-name fallbacks.
    """
    registrations = build_document.get("index")
    if not isinstance(registrations, dict) or not registrations:
        return [], "index_not_registered"

    matching_environment = []
    unknown_environment = []
    for index_name, registration in registrations.items():
        if not isinstance(index_name, str) or not index_name:
            continue
        environment = (
            registration.get("environment") if isinstance(registration, dict) else None
        )
        if environment == expected_indexer_env:
            matching_environment.append(index_name)
        elif environment is None:
            # Older registrations did not always record the environment. They
            # remain eligible for an exact existence check on the target.
            unknown_environment.append(index_name)

    candidates = matching_environment or (
        unknown_environment if allow_legacy_environment else []
    )
    if not candidates:
        return [], "index_not_registered_for_environment"
    return sorted(set(candidates)), None


def check_build_readiness(
    build_document: dict,
    index_name: str,
    *,
    allow_stale_cancelled_post_index: bool = True,
) -> dict:
    """Apply the existing release hook's job-status readiness rule.

    This accepts a final successful job or the narrow legacy cancelled-post-index
    case. It does not independently prove successful indexing of this index.
    Callers remain responsible for index registration/existence, archive policy
    and any stricter completion checks. No serving aliases are consulted here.
    """
    status = build_status(build_document)
    stale = allow_stale_cancelled_post_index and has_stale_cancelled_post_index(
        build_document, index_name
    )
    return {
        "ready": status == "success" or stale,
        "status": status,
        "warning": "cancelled_post_index_after_successful_index" if stale else None,
    }


def resolve_build_index(
    build_document: dict,
    indexer_env: str,
    *,
    index_exists=None,
    allow_legacy_environment: bool = True,
) -> tuple[str | None, dict | None]:
    """Resolve one index registration, optionally checking the target cluster.

    ``index_exists(name)`` must test one concrete index on the intended cluster.
    Supply an ES-client or Cerebro-backed probe as appropriate; this module has
    no transport dependency. Without a probe, only metadata is checked and all
    matching registrations count as candidates. Return ``(name, None)`` or
    ``(None, issue)``. Probe errors propagate rather than becoming missing indices.
    """
    names, error = target_index_candidates(
        build_document,
        indexer_env,
        allow_legacy_environment=allow_legacy_environment,
    )
    if error:
        return None, {"reason": error, "indexer_environment": indexer_env}
    existing = (
        names
        if index_exists is None
        else [name for name in names if index_exists(name)]
    )
    if not existing:
        return None, {
            "reason": "index_not_found",
            "registered_indices": names,
            "indexer_environment": indexer_env,
        }
    if len(existing) > 1:
        return None, {
            "reason": "ambiguous_target_indices",
            "indices": existing,
            "indexer_environment": indexer_env,
        }
    return existing[0], None

def dogpark_select_latest_builds(raw_builds: list[dict]) -> dict:
    selection = select_latest_builds(
        raw_builds,
        DOGPARK_RELEASE_DATASETS,
        normalize_config_names=True,
    )
    config_names = {
        build_config_name(document)
        for document in raw_builds
        if isinstance(document, dict)
    } - {None}
    return {
        **selection,
        "skipped": [
            {"config": name, "reason": "staging_tag_disabled"}
            for name in DOGPARK_NO_STAGING_TAG_DATASETS
            if name in config_names
        ],
        "ignored_configs": sorted(
            config_names - set(DOGPARK_MANAGED_DATASETS)
        ),
    }


def _is_build_ready(build: dict, indexer_env:str) -> bool:
    """Check if a build is done indexing."""
    index_name = build["_id"]

    # filter out builds that are not registered for target cluster
    registration = build.get("index", {}).get(index_name, {})
    if registration.get("environment") != indexer_env:
        return False

    for job in reversed(build.get("jobs", [])):
        if job.get("step") in {"pre-index", "index", "post-index"}:
            return (
                    job.get("step") == "post-index"
                    and job.get("status") == "success"
            )

    return False


def get_available_builds(indexer_env: str) -> list[str]:
    """Return the latest ready builds for DogPark-managed datasets."""
    raw_builds = load_build_documents()
    selection = dogpark_select_latest_builds(raw_builds)
    return list(
        map(
            lambda candidate: candidate["build_id"],
            filter(
                lambda candidate: _is_build_ready(candidate["_build_document"], indexer_env),
                selection["selected"]
            )
        )
    )
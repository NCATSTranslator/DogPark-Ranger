"""Build discovery for snapshot creation and installation.

Hub access is limited to get_src_build_collection() and load_build_documents().
Other helpers select snapshot candidates from supplied metadata; alias operations
are independent of Hub build history.
"""

from __future__ import annotations

import datetime

__all__ = [
    "get_available_builds",
    "dogpark_select_latest_builds",
    "get_src_build_collection",
    "load_build_documents",
    "select_latest_builds",
    "parse_started_at",
    "DOGPARK_RELEASE_DATASETS",
    "DOGPARK_SPECIAL_DATASETS",
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

# Auxiliary datasets use their own staging and production aliases.
DOGPARK_SPECIAL_DATASETS = (
    "ubergraph_nodes_info",
    "ubergraph_nodes",
)


DOGPARK_MANAGED_DATASETS = DOGPARK_RELEASE_DATASETS + DOGPARK_SPECIAL_DATASETS


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
    ``normalize_config_names`` trims configuration names when requested;
    exact matching is default.
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


def dogpark_select_latest_builds(raw_builds: list[dict]) -> dict:
    """Select the newest build of each managed dataset, including auxiliaries."""
    return select_latest_builds(
        raw_builds,
        DOGPARK_MANAGED_DATASETS,
        normalize_config_names=True,
    )

def _is_build_ready(build: dict, indexer_env: str) -> bool:
    """Check if a build is done indexing."""
    if "archived" in build:
        return False
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
    return [
        candidate["build_id"]
        for candidate in selection["selected"]
        if _is_build_ready(candidate["_build_document"], indexer_env)
    ]

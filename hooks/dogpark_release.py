#!/usr/bin/env python3
"""BioThings hub commands for manifest-backed DogPark releases.

The hook treats scheduled builds as a continuously changing desired state. It
selects the newest build for each configured release datasource directly from
the hub build collection, retains current production when a newest build is not
ready, freezes the complete desired set as an immutable JSON manifest, and then
reconciles ``dingo_staging`` from that exact frozen selection.  A verified
staging receipt anchors a deterministic, receipt-backed production promotion.

Deploy the ``hooks`` package, including its ``dogpark_hook_utils`` subpackage,
with the Hub project root on Python's import path.
"""

from __future__ import annotations

import builtins  # AutoHub can shadow list in the shared hook namespace.
from contextlib import contextmanager as _dogpark_contextmanager
import datetime as _dogpark_datetime
import fcntl as _dogpark_fcntl
import hashlib as _dogpark_hashlib
import json as _dogpark_json
import os as _dogpark_os
import tempfile as _dogpark_tempfile
from urllib.error import HTTPError as _dogpark_HTTPError
from urllib.error import URLError as _dogpark_URLError
from urllib.parse import quote as _dogpark_quote
from urllib.request import Request as _dogpark_Request
from urllib.request import urlopen as _dogpark_urlopen

from hooks.dogpark_hook_utils import builds as _dogpark_builds
from hooks.dogpark_hook_utils.builds import (
    get_src_build_collection as _dogpark_get_src_build_collection,
    parse_started_at as _dogpark_parse_started_at,
    public_candidate as _dogpark_public_candidate,
    with_reason as _dogpark_with_reason,
    select_latest_builds as _dogpark_select_latest_for_configs,
    build_config_name as _dogpark_build_config_name,
    build_status as _dogpark_build_status,
    dogpark_select_latest_builds,
    DOGPARK_RELEASE_DATASETS,
    DOGPARK_NO_STAGING_TAG_DATASETS,
    DOGPARK_MANAGED_DATASETS
)

__all__ = [
    "plan_staging_tags",
    "plan_release_manifest",
    "freeze_release_manifest",
    "get_release_manifest",
    "assign_staging_tags",
    "assign_staging_manifest",
    "assign_staging_tag",
    "plan_prod_release",
    "release_prod_manifest",
]

_DOGPARK_ES_URI = "http://tier1-dogpark.ci.transltr.io:9200"
_DOGPARK_EXPECTED_CLUSTER = "es-tier1-cluster"
_DOGPARK_EXPECTED_INDEXER_ENV = "itrb-ci"
_DOGPARK_REQUEST_TIMEOUT_SECONDS = 30

_DOGPARK_STAGING_ALIAS = "dingo_staging"
_DOGPARK_PROD_ALIAS = "dingo"
_DOGPARK_DEPRECATED_ALIAS = "dingo_deprecated"
_DOGPARK_DELETE_ALIAS = "dingo_to_be_deleted"

_DOGPARK_MANIFEST_SCHEMA = "dogpark.release-manifest/v1"
_DOGPARK_STAGING_RECEIPT_SCHEMA = "dogpark.staging-receipt/v1"
_DOGPARK_PROD_RECEIPT_SCHEMA = "dogpark.production-receipt/v1"
_DOGPARK_RELEASE_RECORDS_DIR = _dogpark_os.path.abspath(
    _dogpark_os.environ.get(
        "DOGPARK_RELEASE_RECORDS_DIR",
        _dogpark_os.path.join(_dogpark_os.getcwd(), "release-records"),
    )
)
_DOGPARK_MANIFESTS_DIR = _dogpark_os.path.join(
    _DOGPARK_RELEASE_RECORDS_DIR,
    "manifests",
    "live",
)


_DOGPARK_SPECIAL_ALIAS_POLICY = {
    "ubergraph_nodes_info": {
        "staging_alias": None,
        "production_alias": "ubergraph_nodes_mapping",
    },
    "ubergraph_nodes": {
        "staging_alias": None,
        "production_alias": "ubergraph_nodes",
    },
}




def _dogpark_load_build_documents() -> list[dict]:
    return _dogpark_builds.load_build_documents(_dogpark_get_src_build_collection())


def _dogpark_resolve_target_index(
    candidate: dict,
    es_uri: str,
    expected_indexer_env: str,
) -> tuple[str | None, dict | None]:
    index_name, issue = _dogpark_builds.resolve_build_index(
        candidate["_build_document"],
        expected_indexer_env,
        index_exists=lambda name: _dogpark_index_exists(name, es_uri),
    )
    if issue is not None:
        return None, _dogpark_with_reason(candidate, **issue)
    return index_name, None


def _dogpark_utc_now() -> str:
    return (
        _dogpark_datetime.datetime.now(_dogpark_datetime.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _dogpark_read_response_json(response, url: str):
    body = response.read().decode("utf-8")
    if not body:
        return {}
    try:
        return _dogpark_json.loads(body)
    except _dogpark_json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON returned by {url}: {body[:200]}"
        ) from exc


def _dogpark_request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
    allow_not_found: bool = False,
):
    data = None if payload is None else _dogpark_json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    request = _dogpark_Request(url, data=data, headers=headers, method=method)
    try:
        with _dogpark_urlopen(request, timeout=_DOGPARK_REQUEST_TIMEOUT_SECONDS) as response:
            return _dogpark_read_response_json(response, url)
    except _dogpark_HTTPError as exc:
        if allow_not_found and exc.code == 404:
            return None
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Elasticsearch request failed (HTTP {exc.code}) for {url}: {error_body}"
        ) from exc
    except _dogpark_URLError as exc:
        raise RuntimeError(
            f"Failed to reach Elasticsearch at {url}: {exc}"
        ) from exc


def _dogpark_validate_cluster(es_uri: str, expected_cluster: str) -> str:
    return _dogpark_cluster_identity(es_uri, expected_cluster)["cluster_name"]


def _dogpark_cluster_identity(es_uri: str, expected_cluster: str) -> dict:
    payload = _dogpark_request_json(f"{es_uri.rstrip('/')}/")
    actual_cluster = (
        payload.get("cluster_name")
        if isinstance(payload, dict)
        else None
    )
    if actual_cluster != expected_cluster:
        raise RuntimeError(
            "Elasticsearch cluster mismatch: "
            f"expected {expected_cluster!r}, got {actual_cluster!r}"
        )
    cluster_uuid = payload.get("cluster_uuid")
    if (
        not isinstance(cluster_uuid, str)
        or not cluster_uuid
    ):
        raise RuntimeError(
            "Elasticsearch root response has no usable cluster_uuid"
        )
    return {
        "cluster_name": actual_cluster,
        "cluster_uuid": cluster_uuid,
    }


def _dogpark_get_alias_indices(alias: str, es_uri: str) -> set[str]:
    url = f"{es_uri.rstrip('/')}/_alias/{_dogpark_quote(alias, safe='')}"
    payload = _dogpark_request_json(url, allow_not_found=True)
    if payload is None:
        return set()
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"Elasticsearch alias response for {alias!r} is not an object"
        )
    return {
        name
        for name in payload
        if isinstance(name, str) and name
    }


def _dogpark_get_alias_inventory(es_uri: str) -> dict[str, set[str]]:
    return {
        "prod": _dogpark_get_alias_indices(_DOGPARK_PROD_ALIAS, es_uri),
        "staging": _dogpark_get_alias_indices(_DOGPARK_STAGING_ALIAS, es_uri),
        "deprecated": _dogpark_get_alias_indices(_DOGPARK_DEPRECATED_ALIAS, es_uri),
        "delete": _dogpark_get_alias_indices(_DOGPARK_DELETE_ALIAS, es_uri),
    }


def _dogpark_build_index_ownership(build_documents: list[dict]) -> dict[str, str]:
    """Map every known build/registered index name to its build config."""
    ownership = {}
    for build_document in build_documents:
        build_config = build_document.get("build_config")
        if not isinstance(
            build_config,
            dict,
        ):
            continue
        config_name = build_config.get("_id") or build_config.get("name")
        if config_name not in DOGPARK_MANAGED_DATASETS:
            continue

        names = []
        for key in ("_id", "target_name"):
            value = build_document.get(key)
            if isinstance(value, str) and value:
                names.append(value)
        registrations = build_document.get("index")
        if isinstance(registrations, dict):
            names.extend(
                name
                for name in registrations
                if isinstance(name, str) and name
            )

        for name in names:
            previous = ownership.get(name)
            if previous is not None and previous != config_name:
                raise RuntimeError(
                    f"Index/build name {name!r} belongs to both "
                    f"{previous!r} and {config_name!r}"
                )
            ownership[name] = config_name
    return ownership


def _dogpark_dataset_from_index(
    index_name: str,
    known_ownership: dict[str, str],
) -> str | None:
    known = known_ownership.get(index_name)
    if known is not None:
        return known
    if index_name in DOGPARK_MANAGED_DATASETS:
        return index_name
    parts = index_name.split("_")
    if len(parts) <= 2:
        return None
    candidate = "_".join(parts[:-2])
    return candidate if candidate in DOGPARK_MANAGED_DATASETS else None


def _dogpark_indices_by_dataset(
    index_names: set[str],
    known_ownership: dict[str, str],
) -> dict[str, list[str]]:
    grouped = {}
    for index_name in index_names:
        dataset = _dogpark_dataset_from_index(index_name, known_ownership)
        if dataset is None:
            continue
        grouped.setdefault(dataset, []).append(index_name)
    return {
        dataset: sorted(names)
        for dataset, names in grouped.items()
    }


def _dogpark_index_exists(index_name: str, es_uri: str) -> bool:
    # GET returns a mapping keyed by concrete index names. Requiring the exact
    # key prevents an alias with this name from masquerading as the index.
    url = (
        f"{es_uri.rstrip('/')}/{_dogpark_quote(index_name, safe='')}"
        "?filter_path=*.settings.index.uuid"
    )
    payload = _dogpark_request_json(url, allow_not_found=True)
    return (
        isinstance(payload, dict)
        and index_name in payload
    )


def _dogpark_index_uuid(index_name: str, es_uri: str) -> str | None:
    url = (
        f"{es_uri.rstrip('/')}/{_dogpark_quote(index_name, safe='')}"
        "?filter_path=*.settings.index.uuid"
    )
    payload = _dogpark_request_json(url, allow_not_found=True)
    if (
        not isinstance(payload, dict)
        or index_name not in payload
    ):
        return None
    index_payload = payload.get(index_name)
    settings = (
        index_payload.get("settings")
        if isinstance(index_payload, dict)
        else None
    )
    index_settings = (
        settings.get("index")
        if isinstance(settings, dict)
        else None
    )
    index_uuid = (
        index_settings.get("uuid")
        if isinstance(index_settings, dict)
        else None
    )
    return (
        index_uuid
        if isinstance(index_uuid, str)
        and index_uuid
        else None
    )


def _dogpark_build_alias_payload(index_names: list[str]) -> dict:
    return {
        "actions": [
            {"add": {"index": index_name, "alias": _DOGPARK_STAGING_ALIAS}}
            for index_name in index_names
        ]
    }


def _dogpark_post_staging_aliases(index_names: list[str], es_uri: str) -> dict:
    response = _dogpark_request_json(
        f"{es_uri.rstrip('/')}/_aliases",
        method="POST",
        payload=_dogpark_build_alias_payload(index_names),
    )
    if (
        not isinstance(response, dict)
        or not response.get("acknowledged", False)
    ):
        raise RuntimeError(
            f"Elasticsearch alias update was not acknowledged: {response!r}"
        )
    return response


def _dogpark_build_staging_reconcile_payload(
    add_indices: list[str],
    remove_indices: list[str],
) -> dict:
    return {
        "actions": [
            {
                "remove": {
                    "index": index_name,
                    "alias": _DOGPARK_STAGING_ALIAS,
                }
            }
            for index_name in remove_indices
        ]
        + [
            {
                "add": {
                    "index": index_name,
                    "alias": _DOGPARK_STAGING_ALIAS,
                }
            }
            for index_name in add_indices
        ]
    }


def _dogpark_post_staging_reconciliation(
    add_indices: list[str],
    remove_indices: list[str],
    es_uri: str,
) -> dict:
    response = _dogpark_request_json(
        f"{es_uri.rstrip('/')}/_aliases",
        method="POST",
        payload=_dogpark_build_staging_reconcile_payload(
            add_indices,
            remove_indices,
        ),
    )
    if (
        not isinstance(response, dict)
        or not response.get("acknowledged", False)
    ):
        raise RuntimeError(
            f"Elasticsearch alias update was not acknowledged: {response!r}"
        )
    return response


def _dogpark_plan_staging_tags_with_context(
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    expected_indexer_env: str = _DOGPARK_EXPECTED_INDEXER_ENV,
) -> tuple[dict, dict]:
    actual_cluster = _dogpark_validate_cluster(es_uri, expected_cluster)
    build_documents = _dogpark_load_build_documents()
    selection = dogpark_select_latest_builds(build_documents)
    known_ownership = _dogpark_build_index_ownership(build_documents)
    aliases = _dogpark_get_alias_inventory(es_uri)
    prod_by_dataset = _dogpark_indices_by_dataset(
        aliases["prod"],
        known_ownership,
    )
    staging_by_dataset = _dogpark_indices_by_dataset(
        aliases["staging"],
        known_ownership,
    )

    eligible = []
    already_prod = []
    already_staging = []
    pending = builtins.list(selection["pending"])
    rejected = []

    for candidate in selection["selected"]:
        config_name = candidate["config"]
        index_name, resolution_error = _dogpark_resolve_target_index(
            candidate,
            es_uri,
            expected_indexer_env,
        )
        if resolution_error:
            if resolution_error["reason"] == "ambiguous_target_indices":
                rejected.append(resolution_error)
            else:
                pending.append(resolution_error)
            continue

        resolved_candidate = dict(candidate)
        resolved_candidate["index"] = index_name
        prod_holders = prod_by_dataset.get(config_name, [])
        staging_holders = staging_by_dataset.get(config_name, [])

        if len(prod_holders) > 1:
            rejected.append(
                _dogpark_with_reason(
                    resolved_candidate,
                    "multiple_prod_holders",
                    indices=prod_holders,
                )
            )
            continue
        if len(staging_holders) > 1:
            rejected.append(
                _dogpark_with_reason(
                    resolved_candidate,
                    "multiple_staging_holders",
                    indices=staging_holders,
                )
            )
            continue

        conflicting_states = [
            state
            for state in ("deprecated", "delete")
            if index_name in aliases[state]
        ]
        if conflicting_states:
            rejected.append(
                _dogpark_with_reason(
                    resolved_candidate,
                    "conflicting_release_state",
                    states=conflicting_states,
                )
            )
            continue

        if index_name in aliases["prod"]:
            already_prod.append(
                _dogpark_with_reason(resolved_candidate, "already_prod")
            )
            continue
        if index_name in aliases["staging"]:
            already_staging.append(
                _dogpark_with_reason(resolved_candidate, "already_staging")
            )
            continue

        if staging_holders:
            rejected.append(
                _dogpark_with_reason(
                    resolved_candidate,
                    "different_staging_holder",
                    indices=staging_holders,
                )
            )
            continue

        if candidate["archived"]:
            pending.append(
                _dogpark_with_reason(resolved_candidate, "newest_build_archived")
            )
            continue

        build_document = candidate["_build_document"]
        readiness = _dogpark_builds.check_build_readiness(build_document, index_name)
        status = readiness["status"]
        stale_cancelled_post_index = readiness["warning"] is not None
        if not readiness["ready"]:
            pending.append(
                _dogpark_with_reason(
                    resolved_candidate,
                    "build_not_ready",
                    status=status,
                )
            )
            continue

        if stale_cancelled_post_index:
            eligible.append(
                _dogpark_with_reason(
                    resolved_candidate,
                    "ready_to_stage_after_stale_post_index",
                    status=status,
                    readiness_warning="cancelled_post_index_after_successful_index",
                )
            )
        else:
            eligible.append(
                _dogpark_with_reason(resolved_candidate, "ready_to_stage")
            )

    plan = {
        "generated_at": _dogpark_utc_now(),
        "target": {
            "es_uri": es_uri,
            "cluster_name": actual_cluster,
            "indexer_environment": expected_indexer_env,
        },
        "discovery_source": "src_build",
        "selected": [
            _dogpark_public_candidate(candidate)
            for candidate in selection["selected"]
        ],
        "eligible": eligible,
        "already_prod": already_prod,
        "already_staging": already_staging,
        "pending": pending,
        "rejected": rejected,
        "skipped": selection["skipped"],
        "ignored_configs": selection["ignored_configs"],
        "observed_aliases": {
            state: sorted(names)
            for state, names in aliases.items()
        },
    }
    plan["desired_ready_indices"] = sorted(
        entry["index"]
        for category in ("eligible", "already_prod", "already_staging")
        for entry in plan[category]
    )
    plan["counts"] = {
        category: len(plan[category])
        for category in (
            "selected",
            "eligible",
            "already_prod",
            "already_staging",
            "pending",
            "rejected",
            "skipped",
        )
    }
    return plan, {
        "build_documents": build_documents,
        "known_ownership": known_ownership,
        "aliases": aliases,
        "prod_by_dataset": prod_by_dataset,
        "staging_by_dataset": staging_by_dataset,
    }


def plan_staging_tags(
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    expected_indexer_env: str = _DOGPARK_EXPECTED_INDEXER_ENV,
) -> dict:
    """Plan staging-tag changes from newest builds without modifying aliases."""
    plan, _context = _dogpark_plan_staging_tags_with_context(
        es_uri,
        expected_cluster,
        expected_indexer_env,
    )
    return plan


def _dogpark_build_metadata_for_index(
    config_name: str,
    index_name: str,
    build_documents: list[dict],
) -> dict:
    matches = []
    for build_document in build_documents:
        if _dogpark_build_config_name(build_document) != config_name:
            continue
        registrations = build_document.get("index")
        registered = (
            isinstance(registrations, dict)
            and index_name in registrations
        )
        identity_match = index_name in (
            build_document.get("_id"),
            build_document.get("target_name"),
        )
        if not registered and not identity_match:
            continue
        started_at = _dogpark_parse_started_at(build_document.get("started_at"))
        timestamp = (
            started_at[0]
            if started_at is not None
            else float("-inf")
        )
        matches.append((1 if registered else 0, timestamp, build_document))

    if not matches:
        return {
            "build_id": None,
            "target_name": None,
            "started_at": None,
            "archived": None,
            "hub_status": "unknown",
            "registered_environment": None,
        }

    build_document = max(
        matches,
        key=lambda match: (match[0], match[1]),
    )[2]
    registrations = build_document.get("index")
    registration = (
        registrations.get(index_name)
        if isinstance(registrations, dict)
        else None
    )
    environment = (
        registration.get("environment")
        if isinstance(registration, dict)
        else None
    )
    started_at = _dogpark_parse_started_at(build_document.get("started_at"))
    return {
        "build_id": build_document.get("_id"),
        "target_name": build_document.get("target_name") or build_document.get("_id"),
        "started_at": started_at[1] if started_at is not None else None,
        "archived": "archived" in build_document,
        "hub_status": _dogpark_build_status(build_document),
        "registered_environment": environment,
    }


def _dogpark_manifest_entry(
    *,
    config_name: str,
    index_name: str | None,
    metadata: dict,
    index_uuid: str | None,
    selection: str,
    decision: str,
    reason: str,
    warnings: list,
    staging_alias: str | None,
    production_alias: str,
    observed_prod_indices: list[str],
    observed_staging_indices: list[str],
    newest_candidate_issue: dict | None = None,
) -> dict:
    entry = {
        "config": config_name,
        "build_id": metadata.get("build_id"),
        "target_name": metadata.get("target_name"),
        "index": index_name,
        "index_uuid": index_uuid,
        "started_at": metadata.get("started_at"),
        "archived": metadata.get("archived"),
        "hub_status": metadata.get("hub_status", "unknown"),
        "registered_environment": metadata.get("registered_environment"),
        "selection": selection,
        "decision": decision,
        "reason": reason,
        "warnings": warnings,
        "staging_alias": staging_alias,
        "production_alias": production_alias,
        "observed_prod_indices": observed_prod_indices,
        "observed_staging_indices": observed_staging_indices,
    }
    if newest_candidate_issue is not None:
        entry["newest_candidate_issue"] = newest_candidate_issue
    return entry


def _dogpark_issue_by_config(plan: dict) -> dict[str, dict]:
    issues = {}
    for category in ("pending", "rejected"):
        for issue in plan[category]:
            config_name = issue.get("config")
            if isinstance(config_name, str):
                issues[config_name] = issue
    return issues


def _dogpark_conflicting_release_states(
    index_name: str,
    aliases: dict[str, set[str]],
) -> list[str]:
    return [
        state
        for state in ("deprecated", "delete")
        if index_name in aliases[state]
    ]


def _dogpark_manifest_candidate(
    candidate: dict,
    aliases: dict[str, set[str]],
    es_uri: str,
    expected_indexer_env: str,
    ready_reason: str,
) -> tuple[dict | None, dict | None]:
    index_name, resolution_error = _dogpark_resolve_target_index(
        candidate,
        es_uri,
        expected_indexer_env,
    )
    if resolution_error:
        return None, resolution_error

    resolved = dict(candidate)
    resolved["index"] = index_name
    conflicting_states = _dogpark_conflicting_release_states(index_name, aliases)
    if conflicting_states:
        return None, _dogpark_with_reason(
            resolved,
            "conflicting_release_state",
            states=conflicting_states,
        )

    # Existing release aliases are durable evidence that an operator already
    # accepted this exact concrete index.  Preserve that state even when old
    # hub job metadata was later archived or pruned.
    if index_name in aliases["prod"]:
        return _dogpark_with_reason(resolved, "already_prod"), None
    if index_name in aliases["staging"]:
        return _dogpark_with_reason(resolved, "already_staging"), None
    if candidate["archived"]:
        return None, _dogpark_with_reason(
            resolved,
            "newest_build_archived",
        )

    build_document = candidate["_build_document"]
    readiness = _dogpark_builds.check_build_readiness(build_document, index_name)
    status = readiness["status"]
    stale_cancelled_post_index = readiness["warning"] is not None
    if not readiness["ready"]:
        return None, _dogpark_with_reason(
            resolved,
            "build_not_ready",
            status=status,
        )
    if stale_cancelled_post_index:
        return (
            _dogpark_with_reason(
                resolved,
                "ready_after_stale_post_index",
                status=status,
                readiness_warning="cancelled_post_index_after_successful_index",
            ),
            None,
        )
    return _dogpark_with_reason(resolved, ready_reason), None


def _dogpark_build_release_manifest(
    plan: dict,
    context: dict,
    *,
    es_uri: str,
    expected_cluster: str,
    expected_indexer_env: str,
) -> dict:
    build_documents = context["build_documents"]
    known_ownership = context["known_ownership"]
    aliases = context["aliases"]
    prod_by_dataset = context["prod_by_dataset"]
    staging_by_dataset = context["staging_by_dataset"]
    cluster_identity = _dogpark_cluster_identity(es_uri, expected_cluster)

    special_alias_holders = {
        config_name: sorted(
            _dogpark_get_alias_indices(policy["production_alias"], es_uri)
        )
        for config_name, policy in _DOGPARK_SPECIAL_ALIAS_POLICY.items()
    }
    observed_aliases = {
        _DOGPARK_PROD_ALIAS: sorted(aliases["prod"]),
        _DOGPARK_STAGING_ALIAS: sorted(aliases["staging"]),
        _DOGPARK_DEPRECATED_ALIAS: sorted(aliases["deprecated"]),
        _DOGPARK_DELETE_ALIAS: sorted(aliases["delete"]),
    }
    for config_name, policy in _DOGPARK_SPECIAL_ALIAS_POLICY.items():
        observed_aliases[policy["production_alias"]] = special_alias_holders[
            config_name
        ]

    normal_selection = _dogpark_select_latest_for_configs(
        build_documents,
        DOGPARK_RELEASE_DATASETS,
    )
    normal_selected = {
        candidate["config"]: candidate
        for candidate in normal_selection["selected"]
    }
    normal_issues = {
        issue["config"]: issue for issue in normal_selection["pending"]
    }

    entries = []
    blocked = []
    for config_name in DOGPARK_RELEASE_DATASETS:
        prod_holders = prod_by_dataset.get(config_name, [])
        staging_holders = staging_by_dataset.get(config_name, [])
        candidate = normal_selected.get(config_name)
        ready_candidate = None
        issue = normal_issues.get(
            config_name,
            {"config": config_name, "reason": "newest_candidate_unclassified"},
        )

        if len(prod_holders) > 1:
            issue = {
                "config": config_name,
                "reason": "multiple_current_prod_holders",
                "indices": prod_holders,
            }
        elif candidate is not None:
            ready_candidate, candidate_issue = _dogpark_manifest_candidate(
                candidate,
                aliases,
                es_uri,
                expected_indexer_env,
                "ready_to_stage",
            )
            if candidate_issue is not None:
                issue = candidate_issue

        if ready_candidate is not None:
            index_name = ready_candidate["index"]
            metadata = _dogpark_build_metadata_for_index(
                config_name,
                index_name,
                build_documents,
            )
            index_uuid = _dogpark_index_uuid(index_name, es_uri)
            if index_uuid is not None:
                warnings = []
                if ready_candidate.get("readiness_warning"):
                    warnings.append(ready_candidate["readiness_warning"])
                entry = _dogpark_manifest_entry(
                    config_name=config_name,
                    index_name=index_name,
                    metadata=metadata,
                    index_uuid=index_uuid,
                    selection="newest_ready",
                    decision=(
                        "keep_current_prod"
                        if index_name in prod_holders
                        else "promote"
                    ),
                    reason=ready_candidate["reason"],
                    warnings=warnings,
                    staging_alias=_DOGPARK_STAGING_ALIAS,
                    production_alias=_DOGPARK_PROD_ALIAS,
                    observed_prod_indices=prod_holders,
                    observed_staging_indices=staging_holders,
                )
                entries.append(entry)
                continue
            issue = {
                "config": config_name,
                "build_id": ready_candidate.get("build_id"),
                "index": index_name,
                "reason": "index_uuid_unavailable",
            }

        current_prod_conflicts = (
            _dogpark_conflicting_release_states(prod_holders[0], aliases)
            if len(prod_holders) == 1
            else []
        )
        if current_prod_conflicts:
            issue = {
                "config": config_name,
                "index": prod_holders[0],
                "reason": "current_prod_conflicting_release_state",
                "states": current_prod_conflicts,
                "newest_candidate_issue": issue,
            }

        if (
            len(prod_holders) == 1
            and not current_prod_conflicts
            and issue.get("reason") != "multiple_current_prod_holders"
        ):
            index_name = prod_holders[0]
            metadata = _dogpark_build_metadata_for_index(
                config_name,
                index_name,
                build_documents,
            )
            index_uuid = _dogpark_index_uuid(index_name, es_uri)
            if index_uuid is not None:
                entries.append(
                    _dogpark_manifest_entry(
                        config_name=config_name,
                        index_name=index_name,
                        metadata=metadata,
                        index_uuid=index_uuid,
                        selection="current_prod_fallback",
                        decision="keep_current_prod",
                        reason="fallback_to_current_prod",
                        warnings=[],
                        staging_alias=_DOGPARK_STAGING_ALIAS,
                        production_alias=_DOGPARK_PROD_ALIAS,
                        observed_prod_indices=prod_holders,
                        observed_staging_indices=staging_holders,
                        newest_candidate_issue=issue,
                    )
                )
                continue
            issue = {
                "config": config_name,
                "index": index_name,
                "reason": "current_prod_index_uuid_unavailable",
                "newest_candidate_issue": issue,
            }

        blocked_entry = _dogpark_manifest_entry(
            config_name=config_name,
            index_name=None,
            metadata={},
            index_uuid=None,
            selection="unresolved",
            decision="blocked",
            reason=(
                "multiple_current_prod_holders"
                if len(prod_holders) > 1
                else (
                    "current_prod_conflicting_release_state"
                    if current_prod_conflicts
                    else "no_current_prod_fallback"
                )
            ),
            warnings=[],
            staging_alias=_DOGPARK_STAGING_ALIAS,
            production_alias=_DOGPARK_PROD_ALIAS,
            observed_prod_indices=prod_holders,
            observed_staging_indices=staging_holders,
            newest_candidate_issue=issue,
        )
        entries.append(blocked_entry)
        blocked.append(blocked_entry)

    special_selection = _dogpark_select_latest_for_configs(
        build_documents,
        DOGPARK_NO_STAGING_TAG_DATASETS,
    )
    special_selected = {
        candidate["config"]: candidate
        for candidate in special_selection["selected"]
    }
    special_issues = {
        issue["config"]: issue for issue in special_selection["pending"]
    }
    for config_name in DOGPARK_NO_STAGING_TAG_DATASETS:
        policy = _DOGPARK_SPECIAL_ALIAS_POLICY[config_name]
        prod_holders = special_alias_holders[config_name]
        invalid_prod_holders = [
            {
                "index": index_name,
                "resolved_config": _dogpark_dataset_from_index(
                    index_name,
                    known_ownership,
                ),
            }
            for index_name in prod_holders
            if _dogpark_dataset_from_index(
                index_name,
                known_ownership,
            )
            != config_name
        ]
        staging_holders = staging_by_dataset.get(config_name, [])
        candidate = special_selected.get(config_name)
        ready_candidate = None
        issue = special_issues.get(
            config_name,
            {"config": config_name, "reason": "newest_candidate_unclassified"},
        )
        if candidate is not None:
            ready_candidate, candidate_issue = _dogpark_manifest_candidate(
                candidate,
                aliases,
                es_uri,
                expected_indexer_env,
                "ready_without_staging_alias",
            )
            if candidate_issue is not None:
                issue = candidate_issue
        if invalid_prod_holders:
            issue = {
                "config": config_name,
                "reason": "custom_prod_holder_ownership_conflict",
                "holders": [
                    holder["index"] for holder in invalid_prod_holders
                ],
                "holder_ownership": invalid_prod_holders,
            }
            ready_candidate = None
        elif len(prod_holders) > 1:
            issue = {
                "config": config_name,
                "reason": "multiple_custom_prod_holders",
                "indices": prod_holders,
            }
            ready_candidate = None

        if ready_candidate is not None:
            index_name = ready_candidate["index"]
            metadata = _dogpark_build_metadata_for_index(
                config_name,
                index_name,
                build_documents,
            )
            index_uuid = _dogpark_index_uuid(index_name, es_uri)
            if index_uuid is not None:
                warnings = []
                if ready_candidate.get("readiness_warning"):
                    warnings.append(ready_candidate["readiness_warning"])
                entries.append(
                    _dogpark_manifest_entry(
                        config_name=config_name,
                        index_name=index_name,
                        metadata=metadata,
                        index_uuid=index_uuid,
                        selection="newest_ready",
                        decision=(
                            "keep_current_prod"
                            if index_name in prod_holders
                            else "promote"
                        ),
                        reason=ready_candidate["reason"],
                        warnings=warnings,
                        staging_alias=None,
                        production_alias=policy["production_alias"],
                        observed_prod_indices=prod_holders,
                        observed_staging_indices=staging_holders,
                    )
                )
                continue
            issue = {
                "config": config_name,
                "build_id": ready_candidate.get("build_id"),
                "index": index_name,
                "reason": "index_uuid_unavailable",
            }

        current_prod_conflicts = (
            _dogpark_conflicting_release_states(prod_holders[0], aliases)
            if len(prod_holders) == 1
            else []
        )
        if current_prod_conflicts:
            issue = {
                "config": config_name,
                "index": prod_holders[0],
                "reason": "current_prod_conflicting_release_state",
                "states": current_prod_conflicts,
                "newest_candidate_issue": issue,
            }

        if (
            len(prod_holders) == 1
            and not invalid_prod_holders
            and not current_prod_conflicts
        ):
            index_name = prod_holders[0]
            metadata = _dogpark_build_metadata_for_index(
                config_name,
                index_name,
                build_documents,
            )
            index_uuid = _dogpark_index_uuid(index_name, es_uri)
            if index_uuid is not None:
                entries.append(
                    _dogpark_manifest_entry(
                        config_name=config_name,
                        index_name=index_name,
                        metadata=metadata,
                        index_uuid=index_uuid,
                        selection="current_prod_fallback",
                        decision="keep_current_prod",
                        reason="fallback_to_current_prod",
                        warnings=[],
                        staging_alias=None,
                        production_alias=policy["production_alias"],
                        observed_prod_indices=prod_holders,
                        observed_staging_indices=staging_holders,
                        newest_candidate_issue=issue,
                    )
                )
                continue
            issue = {
                "config": config_name,
                "index": index_name,
                "reason": "current_prod_index_uuid_unavailable",
                "newest_candidate_issue": issue,
            }

        blocked_entry = _dogpark_manifest_entry(
            config_name=config_name,
            index_name=None,
            metadata={},
            index_uuid=None,
            selection="unresolved",
            decision="blocked",
            reason=(
                "custom_prod_holder_ownership_conflict"
                if invalid_prod_holders
                else (
                    "multiple_custom_prod_holders"
                    if len(prod_holders) > 1
                    else (
                        "current_prod_conflicting_release_state"
                        if current_prod_conflicts
                        else "no_current_prod_fallback"
                    )
                )
            ),
            warnings=[],
            staging_alias=None,
            production_alias=policy["production_alias"],
            observed_prod_indices=prod_holders,
            observed_staging_indices=staging_holders,
            newest_candidate_issue=issue,
        )
        entries.append(blocked_entry)
        blocked.append(blocked_entry)

    desired_entries = [entry for entry in entries if entry["decision"] != "blocked"]
    desired_indices = [entry["index"] for entry in desired_entries]
    staging_required_indices = [
        entry["index"]
        for entry in desired_entries
        if entry["decision"] == "promote" and entry["staging_alias"] is not None
    ]
    staging_add_indices = [
        index_name
        for index_name in staging_required_indices
        if index_name not in aliases["staging"]
    ]
    managed_staging_indices = {
        index_name
        for entry in entries
        for index_name in entry["observed_staging_indices"]
    }
    staging_remove_indices = sorted(
        managed_staging_indices
        - set(staging_required_indices)
    )
    precondition = {
        "cluster_name": cluster_identity["cluster_name"],
        "cluster_uuid": cluster_identity["cluster_uuid"],
        "observed_aliases": observed_aliases,
    }
    created_at = _dogpark_utc_now()
    draft = {
        "schema": _DOGPARK_MANIFEST_SCHEMA,
        "created_at": created_at,
        "source": {
            "command": "plan_release_manifest",
            "discovery": "src_build",
            "staging_plan_generated_at": plan["generated_at"],
        },
        "target": {
            "es_uri": es_uri,
            "cluster_name": cluster_identity["cluster_name"],
            "cluster_uuid": cluster_identity["cluster_uuid"],
            "indexer_environment": expected_indexer_env,
        },
        "policy": {
            "managed_datasets": builtins.list(DOGPARK_MANAGED_DATASETS),
            "default_aliases": {
                "staging_alias": _DOGPARK_STAGING_ALIAS,
                "production_alias": _DOGPARK_PROD_ALIAS,
                "deprecated_alias": _DOGPARK_DEPRECATED_ALIAS,
                "delete_alias": _DOGPARK_DELETE_ALIAS,
            },
            "special_aliases": _DOGPARK_SPECIAL_ALIAS_POLICY,
        },
        "entries": entries,
        "observed_aliases": observed_aliases,
        "precondition_sha256": _dogpark_sha256(
            _dogpark_canonical_json(precondition)
        ),
        "desired_indices": desired_indices,
        "staging_required_indices": staging_required_indices,
        "staging_add_indices": staging_add_indices,
        "staging_remove_indices": staging_remove_indices,
        "keep_indices": [
            entry["index"]
            for entry in desired_entries
            if entry["decision"] == "keep_current_prod"
        ],
        "promotion_indices": [
            entry["index"]
            for entry in desired_entries
            if entry["decision"] == "promote"
        ],
        "blocked": blocked,
        "complete": not blocked,
        "counts": {
            "managed": len(DOGPARK_MANAGED_DATASETS),
            "desired": len(desired_entries),
            "promote": sum(
                entry["decision"] == "promote" for entry in desired_entries
            ),
            "keep_current_prod": sum(
                entry["decision"] == "keep_current_prod"
                for entry in desired_entries
            ),
            "blocked": len(blocked),
            "staging_add": len(staging_add_indices),
            "staging_remove": len(staging_remove_indices),
        },
    }
    return _dogpark_finalize_manifest(draft)


def _dogpark_prepare_release_manifest(
    es_uri: str,
    expected_cluster: str,
    expected_indexer_env: str,
) -> tuple[dict, dict]:
    plan, context = _dogpark_plan_staging_tags_with_context(
        es_uri,
        expected_cluster,
        expected_indexer_env,
    )
    manifest = _dogpark_build_release_manifest(
        plan,
        context,
        es_uri=es_uri,
        expected_cluster=expected_cluster,
        expected_indexer_env=expected_indexer_env,
    )
    return manifest, plan


def plan_release_manifest(
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    expected_indexer_env: str = _DOGPARK_EXPECTED_INDEXER_ENV,
) -> dict:
    """Build a complete manifest preview without writing files or aliases."""
    manifest, _plan = _dogpark_prepare_release_manifest(
        es_uri,
        expected_cluster,
        expected_indexer_env,
    )
    return manifest


def _dogpark_canonical_json(value) -> bytes:
    return _dogpark_json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _dogpark_pretty_json(value) -> bytes:
    return (
        _dogpark_json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")


def _dogpark_sha256(data: bytes) -> str:
    return _dogpark_hashlib.sha256(data).hexdigest()


def _dogpark_manifest_timestamp(created_at: str) -> str:
    return "".join(
        character
        for character in created_at
        if character.isdigit() or character in ("T", "Z")
    )


def _dogpark_finalize_manifest(draft: dict) -> dict:
    body = dict(draft)
    body.pop("manifest_id", None)
    body.pop("integrity", None)
    created_at = body.get("created_at")
    if not isinstance(created_at, str):
        raise ValueError("manifest has no created_at timestamp")
    identity_seed = _dogpark_sha256(_dogpark_canonical_json(body))
    manifest_id = (
        f"{_dogpark_manifest_timestamp(created_at)}-{identity_seed[:12]}"
    )
    body["manifest_id"] = manifest_id
    payload_sha256 = _dogpark_sha256(_dogpark_canonical_json(body))
    body["integrity"] = {
        "algorithm": "sha256",
        "payload_sha256": payload_sha256,
    }
    return body


def _dogpark_validate_manifest_id(manifest_id: str) -> str:
    if not isinstance(manifest_id, str):
        raise TypeError("manifest_id must be a string")
    manifest_id = manifest_id.strip()
    if (
        not manifest_id
        or len(manifest_id) > 128
        or not all(
            character
            in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for character in manifest_id
        )
    ):
        raise ValueError(
            "manifest_id must contain only letters, numbers, '-' and '_'"
        )
    return manifest_id


def _dogpark_fsync_directory(directory: str) -> None:
    try:
        directory_fd = _dogpark_os.open(directory, _dogpark_os.O_RDONLY)
    except OSError:
        return
    try:
        _dogpark_os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        _dogpark_os.close(directory_fd)


def _dogpark_write_temp_file(directory: str, data: bytes) -> str:
    descriptor, temporary_path = _dogpark_tempfile.mkstemp(
        prefix=".dogpark-release-",
        suffix=".tmp",
        dir=directory,
    )
    try:
        _dogpark_os.fchmod(descriptor, 0o640)
        with _dogpark_os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(data)
            stream.flush()
            _dogpark_os.fsync(stream.fileno())
    except BaseException:
        if descriptor >= 0:
            _dogpark_os.close(descriptor)
        try:
            _dogpark_os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise
    return temporary_path


def _dogpark_write_immutable_json(path: str, value: dict) -> str:
    directory = _dogpark_os.path.dirname(path)
    _dogpark_os.makedirs(directory, mode=0o750, exist_ok=True)
    data = _dogpark_pretty_json(value)
    temporary_path = _dogpark_write_temp_file(directory, data)
    try:
        try:
            _dogpark_os.link(temporary_path, path)
        except FileExistsError:
            with open(path, "rb") as existing:
                if existing.read() != data:
                    raise RuntimeError(
                        f"Immutable release record already exists with different content: {path}"
                    )
        _dogpark_fsync_directory(directory)
    finally:
        try:
            _dogpark_os.unlink(temporary_path)
        except FileNotFoundError:
            pass
    return _dogpark_sha256(data)


def _dogpark_replace_json(path: str, value: dict) -> str:
    directory = _dogpark_os.path.dirname(path)
    _dogpark_os.makedirs(directory, mode=0o750, exist_ok=True)
    data = _dogpark_pretty_json(value)
    temporary_path = _dogpark_write_temp_file(directory, data)
    try:
        _dogpark_os.replace(temporary_path, path)
        _dogpark_fsync_directory(directory)
    finally:
        try:
            _dogpark_os.unlink(temporary_path)
        except FileNotFoundError:
            pass
    return _dogpark_sha256(data)


@_dogpark_contextmanager
def _dogpark_manifest_lock():
    _dogpark_os.makedirs(_DOGPARK_MANIFESTS_DIR, mode=0o750, exist_ok=True)
    lock_path = _dogpark_os.path.join(_DOGPARK_MANIFESTS_DIR, ".manifest.lock")
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        _dogpark_fcntl.flock(lock_file.fileno(), _dogpark_fcntl.LOCK_EX)
        try:
            yield
        finally:
            _dogpark_fcntl.flock(lock_file.fileno(), _dogpark_fcntl.LOCK_UN)


def _dogpark_manifest_pointer_path(pointer_name: str) -> str:
    if pointer_name not in ("latest-frozen", "latest-staged", "latest-prod"):
        raise ValueError(
            f"Unsupported release pointer {pointer_name!r}"
        )
    return _dogpark_os.path.join(
        _DOGPARK_MANIFESTS_DIR,
        f"{pointer_name}.json",
    )


def _dogpark_publish_manifest_pointer(
    pointer_name: str,
    manifest: dict,
    manifest_sha256: str,
    **details,
) -> dict:
    manifest_id = _dogpark_validate_manifest_id(manifest["manifest_id"])
    pointer = {
        "schema": "dogpark.release-pointer/v1",
        "pointer": pointer_name,
        "manifest_id": manifest_id,
        "manifest_file": f"{manifest_id}.json",
        "manifest_sha256": manifest_sha256,
        "updated_at": _dogpark_utc_now(),
    }
    pointer.update(details)
    _dogpark_replace_json(_dogpark_manifest_pointer_path(pointer_name), pointer)
    return pointer


def _dogpark_verify_manifest(manifest: dict, expected_id: str | None = None) -> None:
    if not isinstance(manifest, dict):
        raise RuntimeError("Release manifest is not a JSON object")
    if manifest.get("schema") != _DOGPARK_MANIFEST_SCHEMA:
        raise RuntimeError(
            f"Unsupported release manifest schema: {manifest.get('schema')!r}"
        )
    manifest_id = _dogpark_validate_manifest_id(manifest.get("manifest_id"))
    if expected_id is not None and manifest_id != expected_id:
        raise RuntimeError(
            f"Manifest id mismatch: expected {expected_id!r}, got {manifest_id!r}"
        )
    integrity = manifest.get("integrity")
    if (
        not isinstance(integrity, dict)
        or integrity.get("algorithm") != "sha256"
    ):
        raise RuntimeError("Manifest has no SHA-256 integrity record")
    body = dict(manifest)
    body.pop("integrity", None)
    calculated = _dogpark_sha256(_dogpark_canonical_json(body))
    if calculated != integrity.get("payload_sha256"):
        raise RuntimeError("Release manifest integrity check failed")
    identity_body = dict(body)
    identity_body.pop("manifest_id", None)
    expected_manifest_id = (
        f"{_dogpark_manifest_timestamp(identity_body.get('created_at', ''))}-"
        f"{_dogpark_sha256(_dogpark_canonical_json(identity_body))[:12]}"
    )
    if manifest_id != expected_manifest_id:
        raise RuntimeError(
            "Release manifest id does not match its immutable content"
        )


def _dogpark_persist_manifest(manifest: dict) -> dict:
    _dogpark_validate_manifest_contract(manifest)
    if not manifest.get("complete", False):
        raise RuntimeError(
            "Cannot persist an incomplete release manifest"
        )
    manifest_id = manifest["manifest_id"]
    manifest_file = f"{manifest_id}.json"
    manifest_path = _dogpark_os.path.join(_DOGPARK_MANIFESTS_DIR, manifest_file)
    manifest_sha256 = _dogpark_write_immutable_json(manifest_path, manifest)
    pointer = _dogpark_publish_manifest_pointer(
        "latest-frozen",
        manifest,
        manifest_sha256,
    )
    return {
        "manifest_id": manifest_id,
        "manifest_file": manifest_file,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha256,
        "latest_pointer": _dogpark_manifest_pointer_path("latest-frozen"),
        "pointer": pointer,
    }


def _dogpark_read_json_file(path: str) -> tuple[dict, str]:
    with open(path, "rb") as stream:
        data = stream.read()
    try:
        value = _dogpark_json.loads(data.decode("utf-8"))
    except (
        UnicodeDecodeError,
        _dogpark_json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(
            f"Invalid JSON release record: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise RuntimeError(
            f"Release record is not a JSON object: {path}"
        )
    return value, _dogpark_sha256(data)


def _dogpark_load_receipt_record(
    *,
    manifest_id: str,
    receipt_id,
    receipt_file,
    receipt_sha256,
    label: str,
) -> tuple[dict, dict]:
    receipt_id = _dogpark_validate_manifest_id(receipt_id)
    expected_receipt_file = _dogpark_os.path.join(
        "receipts",
        manifest_id,
        f"{receipt_id}.json",
    )
    if receipt_file != expected_receipt_file:
        raise RuntimeError(
            f"{label} contains an invalid receipt filename"
        )
    receipt_path = _dogpark_os.path.join(
        _DOGPARK_MANIFESTS_DIR,
        expected_receipt_file,
    )
    receipt, calculated_sha256 = _dogpark_read_json_file(receipt_path)
    if receipt_sha256 != calculated_sha256:
        raise RuntimeError(
            f"{label} receipt checksum does not match the immutable file"
        )
    return receipt, {
        "receipt_id": receipt_id,
        "receipt_file": expected_receipt_file,
        "receipt_path": receipt_path,
        "receipt_sha256": calculated_sha256,
        "receipt": receipt,
    }


def _dogpark_load_manifest(manifest_id: str) -> tuple[dict, dict]:
    manifest_id = _dogpark_validate_manifest_id(manifest_id)
    pointer = None
    pointer_name = None
    if manifest_id in (
        "latest",
        "latest-frozen",
        "latest-staged",
        "latest-prod",
    ):
        pointer_name = "latest-staged" if manifest_id == "latest" else manifest_id
        pointer_path = _dogpark_manifest_pointer_path(pointer_name)
        pointer, _pointer_sha256 = _dogpark_read_json_file(pointer_path)
        if pointer.get("schema") != "dogpark.release-pointer/v1":
            raise RuntimeError(
                f"Unsupported release pointer schema in {pointer_path}"
            )
        if pointer.get("pointer") != pointer_name:
            raise RuntimeError(
                "Release pointer selector does not match its contents"
            )
        manifest_id = _dogpark_validate_manifest_id(pointer.get("manifest_id"))
        if pointer.get("manifest_file") != f"{manifest_id}.json":
            raise RuntimeError(
                "Release pointer contains an invalid manifest filename"
            )

    manifest_path = _dogpark_os.path.join(
        _DOGPARK_MANIFESTS_DIR,
        f"{manifest_id}.json",
    )
    manifest, manifest_sha256 = _dogpark_read_json_file(manifest_path)
    _dogpark_verify_manifest(manifest, manifest_id)
    if pointer is not None and pointer.get("manifest_sha256") != manifest_sha256:
        raise RuntimeError(
            "Release pointer manifest checksum does not match the immutable file"
        )
    receipt_storage = None
    staging_receipt_storage = None
    if pointer_name == "latest-staged":
        _dogpark_validate_manifest_contract(manifest)
        receipt, receipt_storage = _dogpark_load_receipt_record(
            manifest_id=manifest_id,
            receipt_id=pointer.get("receipt_id"),
            receipt_file=pointer.get("receipt_file"),
            receipt_sha256=pointer.get("receipt_sha256"),
            label="Staged release pointer",
        )
        _dogpark_verify_staging_receipt(
            receipt,
            receipt_storage["receipt_id"],
            manifest,
        )
    elif pointer_name == "latest-prod":
        _dogpark_validate_manifest_contract(manifest)
        receipt, receipt_storage = _dogpark_load_receipt_record(
            manifest_id=manifest_id,
            receipt_id=pointer.get("receipt_id"),
            receipt_file=pointer.get("receipt_file"),
            receipt_sha256=pointer.get("receipt_sha256"),
            label="Production release pointer",
        )
        staging_link = receipt.get("staging_receipt")
        if not isinstance(
            staging_link,
            dict,
        ):
            raise RuntimeError(
                "Production receipt has no staging receipt link"
            )
        pointer_links = {
            "receipt_payload_sha256": (
                receipt.get("integrity", {}).get("payload_sha256")
                if isinstance(
                    receipt.get("integrity"),
                    dict,
                )
                else None
            ),
            "staging_receipt_id": staging_link.get("receipt_id"),
            "staging_receipt_sha256": staging_link.get("receipt_sha256"),
            "transition_sha256": receipt.get("transition_sha256"),
        }
        if any(
            pointer.get(field) != value
            for field, value in pointer_links.items()
        ):
            raise RuntimeError(
                "Production release pointer metadata does not match its receipt"
            )
        staging_receipt, staging_receipt_storage = (
            _dogpark_load_receipt_record(
                manifest_id=manifest_id,
                receipt_id=staging_link.get("receipt_id"),
                receipt_file=staging_link.get("receipt_file"),
                receipt_sha256=staging_link.get("receipt_sha256"),
                label="Production staging-receipt link",
            )
        )
        _dogpark_verify_staging_receipt(
            staging_receipt,
            staging_receipt_storage["receipt_id"],
            manifest,
        )
        _dogpark_verify_prod_receipt(
            receipt,
            receipt_storage["receipt_id"],
            manifest,
            staging_receipt,
            staging_receipt_storage,
        )
    return manifest, {
        "manifest_id": manifest_id,
        "manifest_file": f"{manifest_id}.json",
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha256,
        "pointer": pointer,
        "receipt": receipt_storage,
        "staging_receipt": staging_receipt_storage,
    }


def _dogpark_validate_manifest_contract(manifest: dict) -> None:
    _dogpark_verify_manifest(manifest)
    entries = manifest.get("entries")
    if (
        not isinstance(entries, builtins.list)
        or not all(
            isinstance(entry, dict)
            for entry in entries
        )
    ):
        raise RuntimeError("Manifest entries must be a list")
    configs = [entry.get("config") for entry in entries]
    if configs != builtins.list(DOGPARK_MANAGED_DATASETS):
        raise RuntimeError(
            "Manifest entries do not contain each managed datasource exactly once"
        )
    valid_decisions = {"promote", "keep_current_prod", "blocked"}
    invalid_decisions = [
        entry.get("decision")
        for entry in entries
        if entry.get("decision") not in valid_decisions
    ]
    if invalid_decisions:
        raise RuntimeError(
            f"Manifest contains invalid release decisions: {invalid_decisions!r}"
        )
    blocked = [entry for entry in entries if entry.get("decision") == "blocked"]
    if bool(blocked) != (not manifest.get("complete", False)):
        raise RuntimeError(
            "Manifest complete flag does not agree with blocked entries"
        )
    if manifest.get("blocked") != blocked:
        raise RuntimeError(
            "Manifest blocked list is not derived from entries"
        )

    desired_entries = [entry for entry in entries if entry.get("decision") != "blocked"]
    desired_indices = [entry.get("index") for entry in desired_entries]
    if (
        not all(
            isinstance(index_name, str)
            and index_name
            for index_name in desired_indices
        )
        or len(set(desired_indices))
        != len(desired_indices)
    ):
        raise RuntimeError(
            "Manifest desired indices must be unique non-empty strings"
        )
    if manifest.get("desired_indices") != desired_indices:
        raise RuntimeError(
            "Manifest desired_indices is not derived from entries"
        )
    keep_indices = [
        entry["index"]
        for entry in desired_entries
        if entry["decision"] == "keep_current_prod"
    ]
    promotion_indices = [
        entry["index"]
        for entry in desired_entries
        if entry["decision"] == "promote"
    ]
    if manifest.get("keep_indices") != keep_indices:
        raise RuntimeError(
            "Manifest keep_indices is not derived from entries"
        )
    if manifest.get("promotion_indices") != promotion_indices:
        raise RuntimeError(
            "Manifest promotion_indices is not derived from entries"
        )

    staging_required = [
        entry["index"]
        for entry in desired_entries
        if entry.get("decision") == "promote"
        and entry.get("staging_alias") is not None
    ]
    if manifest.get("staging_required_indices") != staging_required:
        raise RuntimeError(
            "Manifest staging_required_indices is not derived from entries"
        )
    observed_aliases = manifest.get("observed_aliases")
    if not isinstance(observed_aliases, dict):
        raise RuntimeError(
            "Manifest observed_aliases must be an object"
        )
    for alias, indices in observed_aliases.items():
        if (
            not isinstance(alias, str)
            or not alias
            or not isinstance(indices, builtins.list)
            or not all(
                isinstance(index_name, str)
                and index_name
                for index_name in indices
            )
            or indices != sorted(set(indices))
        ):
            raise RuntimeError(
                f"Manifest observed alias {alias!r} has an invalid index list"
            )
    observed_staging = observed_aliases.get(_DOGPARK_STAGING_ALIAS)
    if not isinstance(observed_staging, builtins.list):
        raise RuntimeError(
            "Manifest has no observed staging alias list"
        )
    required_aliases = {
        _DOGPARK_PROD_ALIAS,
        _DOGPARK_STAGING_ALIAS,
        _DOGPARK_DEPRECATED_ALIAS,
        _DOGPARK_DELETE_ALIAS,
    } | {
        entry.get("production_alias")
        for entry in entries
        if isinstance(
            entry.get("production_alias"),
            str,
        )
    }
    missing_aliases = sorted(
        required_aliases - set(observed_aliases)
    )
    if missing_aliases:
        raise RuntimeError(
            "Manifest is missing observed managed aliases: "
            f"{missing_aliases!r}"
        )
    staging_add = [
        index_name
        for index_name in staging_required
        if index_name not in observed_staging
    ]
    if manifest.get("staging_add_indices") != staging_add:
        raise RuntimeError(
            "Manifest staging_add_indices is not derived from the frozen snapshot"
        )
    managed_staging = set()
    for entry in entries:
        observed_entry_staging = entry.get("observed_staging_indices")
        if (
            not isinstance(
                observed_entry_staging,
                builtins.list,
            )
            or not all(
                isinstance(index_name, str)
                and index_name
                for index_name in observed_entry_staging
            )
        ):
            raise RuntimeError(
                f"Manifest entry {entry.get('config')!r} has invalid observed staging holders"
            )
        managed_staging.update(observed_entry_staging)
    staging_remove = sorted(
        managed_staging - set(staging_required)
    )
    if manifest.get("staging_remove_indices") != staging_remove:
        raise RuntimeError(
            "Manifest staging_remove_indices is not derived from the frozen snapshot"
        )
    if set(staging_add) & set(staging_remove):
        raise RuntimeError(
            "Manifest cannot add and remove the same staging alias holder"
        )
    expected_counts = {
        "managed": len(DOGPARK_MANAGED_DATASETS),
        "desired": len(desired_entries),
        "promote": len(promotion_indices),
        "keep_current_prod": len(keep_indices),
        "blocked": len(blocked),
        "staging_add": len(staging_add),
        "staging_remove": len(staging_remove),
    }
    if manifest.get("counts") != expected_counts:
        raise RuntimeError(
            "Manifest counts are not derived from its entries and staging plan"
        )
    for entry in desired_entries:
        index_uuid = entry.get("index_uuid")
        if (
            not isinstance(index_uuid, str)
            or not index_uuid
        ):
            raise RuntimeError(
                f"Manifest entry {entry.get('config')!r} has no index UUID"
            )
        expected_policy = (
            _DOGPARK_SPECIAL_ALIAS_POLICY.get(entry["config"])
            or {
                "staging_alias": _DOGPARK_STAGING_ALIAS,
                "production_alias": _DOGPARK_PROD_ALIAS,
            }
        )
        if (
            entry.get("staging_alias") != expected_policy["staging_alias"]
            or entry.get("production_alias")
            != expected_policy["production_alias"]
        ):
            raise RuntimeError(
                f"Manifest entry {entry['config']!r} has invalid alias policy"
            )
        observed_prod_indices = entry.get("observed_prod_indices")
        observed_entry_staging = entry.get("observed_staging_indices")
        for field, values in (
            ("observed_prod_indices", observed_prod_indices),
            ("observed_staging_indices", observed_entry_staging),
        ):
            if (
                not isinstance(
                    values,
                    builtins.list,
                )
                or values
                != sorted(set(values))
            ):
                raise RuntimeError(
                    f"Manifest entry {entry['config']!r} has invalid {field}"
                )
        if not set(observed_prod_indices) <= set(
            observed_aliases[entry["production_alias"]]
        ):
            raise RuntimeError(
                f"Manifest entry {entry['config']!r} production holders do not "
                "match the observed alias snapshot"
            )
        if not set(
            observed_entry_staging
        ) <= set(observed_staging):
            raise RuntimeError(
                f"Manifest entry {entry['config']!r} staging holders do not "
                "match the observed alias snapshot"
            )
        is_current_prod = entry["index"] in observed_prod_indices
        if (
            entry["decision"] == "keep_current_prod" and not is_current_prod
        ) or (entry["decision"] == "promote" and is_current_prod):
            raise RuntimeError(
                f"Manifest entry {entry['config']!r} decision disagrees with "
                "its observed production holders"
            )

    target = manifest.get("target")
    precondition = {
        "cluster_name": (
            target.get("cluster_name")
            if isinstance(target, dict)
            else None
        ),
        "cluster_uuid": (
            target.get("cluster_uuid")
            if isinstance(target, dict)
            else None
        ),
        "observed_aliases": observed_aliases,
    }
    if manifest.get("precondition_sha256") != _dogpark_sha256(
        _dogpark_canonical_json(precondition)
    ):
        raise RuntimeError(
            "Manifest alias precondition digest is invalid"
        )


def _dogpark_live_aliases_for_manifest(manifest: dict, es_uri: str) -> dict:
    observed_aliases = manifest["observed_aliases"]
    return {
        alias: sorted(
            _dogpark_get_alias_indices(alias, es_uri)
        )
        for alias in observed_aliases
    }


def _dogpark_expected_post_staging_aliases(manifest: dict) -> dict:
    expected = {
        alias: builtins.list(indices)
        for alias, indices in manifest["observed_aliases"].items()
    }
    expected[_DOGPARK_STAGING_ALIAS] = sorted(
        (
            set(expected[_DOGPARK_STAGING_ALIAS])
            - set(manifest["staging_remove_indices"])
        )
        | set(manifest["staging_add_indices"])
    )
    return expected


def _dogpark_assert_manifest_target(
    manifest: dict,
    es_uri: str,
    expected_cluster: str,
) -> dict:
    target = manifest["target"]
    if target["es_uri"].rstrip("/") != es_uri.rstrip("/"):
        raise RuntimeError(
            "Manifest Elasticsearch URI does not match the requested target"
        )
    if target["cluster_name"] != expected_cluster:
        raise RuntimeError(
            "Manifest cluster name does not match the requested target"
        )
    identity = _dogpark_cluster_identity(es_uri, expected_cluster)
    if identity["cluster_uuid"] != target["cluster_uuid"]:
        raise RuntimeError(
            "Manifest cluster UUID does not match the live Elasticsearch cluster"
        )
    for entry in manifest["entries"]:
        if entry["decision"] == "blocked":
            continue
        live_uuid = _dogpark_index_uuid(entry["index"], es_uri)
        if live_uuid != entry["index_uuid"]:
            raise RuntimeError(
                f"Index identity changed for {entry['index']!r}: "
                f"expected UUID {entry['index_uuid']!r}, got {live_uuid!r}"
            )
    return identity


def _dogpark_finalize_receipt(receipt: dict) -> dict:
    body = dict(receipt)
    body.pop("receipt_id", None)
    body.pop("integrity", None)
    identity_seed = _dogpark_sha256(_dogpark_canonical_json(body))
    receipt_id = (
        f"{_dogpark_manifest_timestamp(body['verified_at'])}-{identity_seed[:12]}"
    )
    body["receipt_id"] = receipt_id
    body["integrity"] = {
        "algorithm": "sha256",
        "payload_sha256": _dogpark_sha256(_dogpark_canonical_json(body)),
    }
    return body


def _dogpark_verify_staging_receipt(
    receipt: dict,
    expected_id: str,
    manifest: dict,
) -> None:
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema") != _DOGPARK_STAGING_RECEIPT_SCHEMA
    ):
        raise RuntimeError(
            "Unsupported or invalid staging receipt"
        )
    receipt_id = _dogpark_validate_manifest_id(receipt.get("receipt_id"))
    if receipt_id != expected_id:
        raise RuntimeError(
            "Staging receipt id does not match its pointer"
        )
    integrity = receipt.get("integrity")
    if (
        not isinstance(integrity, dict)
        or integrity.get("algorithm") != "sha256"
    ):
        raise RuntimeError(
            "Staging receipt has no SHA-256 integrity record"
        )
    body = dict(receipt)
    body.pop("integrity", None)
    if _dogpark_sha256(_dogpark_canonical_json(body)) != integrity.get(
        "payload_sha256"
    ):
        raise RuntimeError(
            "Staging receipt integrity check failed"
        )
    identity_body = dict(body)
    identity_body.pop("receipt_id", None)
    verified_at = identity_body.get("verified_at")
    if not isinstance(verified_at, str):
        raise RuntimeError(
            "Staging receipt has no verification timestamp"
        )
    calculated_id = (
        f"{_dogpark_manifest_timestamp(verified_at)}-"
        f"{_dogpark_sha256(_dogpark_canonical_json(identity_body))[:12]}"
    )
    if receipt_id != calculated_id:
        raise RuntimeError(
            "Staging receipt id does not match its immutable content"
        )
    if (
        receipt.get("manifest_id") != manifest["manifest_id"]
        or receipt.get("manifest_payload_sha256")
        != manifest["integrity"]["payload_sha256"]
    ):
        raise RuntimeError(
            "Staging receipt does not belong to the selected manifest"
        )
    target = receipt.get("target")
    manifest_target = manifest["target"]
    expected_target = {
        "es_uri": manifest_target["es_uri"],
        "cluster_name": manifest_target["cluster_name"],
        "cluster_uuid": manifest_target["cluster_uuid"],
    }
    if target != expected_target:
        raise RuntimeError(
            "Staging receipt target does not match the manifest"
        )
    if (
        receipt.get("requested_additions") != manifest["staging_add_indices"]
        or receipt.get("requested_removals")
        != manifest["staging_remove_indices"]
        or receipt.get("required_staging_indices")
        != manifest["staging_required_indices"]
        or receipt.get("verified_aliases")
        != _dogpark_expected_post_staging_aliases(manifest)
    ):
        raise RuntimeError(
            "Staging receipt does not prove the manifest's expected alias state"
        )
    outcome = receipt.get("outcome")
    if outcome == "aliases_reconciled":
        expected_tagged = manifest["staging_add_indices"]
        expected_removed = manifest["staging_remove_indices"]
    elif outcome in ("already_applied", "no_aliases_required"):
        expected_tagged = []
        expected_removed = []
    else:
        raise RuntimeError(
            f"Unsupported staging receipt outcome: {outcome!r}"
        )
    if (
        receipt.get("tagged_this_run") != expected_tagged
        or receipt.get("removed_this_run") != expected_removed
    ):
        raise RuntimeError(
            "Staging receipt mutation audit does not match its outcome"
        )


def _dogpark_persist_staging_receipt(
    manifest: dict,
    manifest_storage: dict,
    receipt: dict,
) -> dict:
    receipt_id = _dogpark_validate_manifest_id(receipt["receipt_id"])
    relative_file = _dogpark_os.path.join(
        "receipts",
        manifest["manifest_id"],
        f"{receipt_id}.json",
    )
    receipt_path = _dogpark_os.path.join(_DOGPARK_MANIFESTS_DIR, relative_file)
    receipt_sha256 = _dogpark_write_immutable_json(receipt_path, receipt)
    pointer = _dogpark_publish_manifest_pointer(
        "latest-staged",
        manifest,
        manifest_storage["manifest_sha256"],
        receipt_id=receipt_id,
        receipt_file=relative_file,
        receipt_sha256=receipt_sha256,
    )
    return {
        "receipt_id": receipt_id,
        "receipt_file": relative_file,
        "receipt_path": receipt_path,
        "receipt_sha256": receipt_sha256,
        "latest_pointer": _dogpark_manifest_pointer_path("latest-staged"),
        "pointer": pointer,
    }


def _dogpark_staging_receipt_link(receipt_storage: dict) -> dict:
    receipt = receipt_storage.get("receipt")
    if not isinstance(receipt, dict):
        raise RuntimeError(
            "Staged release has no verified receipt payload"
        )
    integrity = receipt.get("integrity")
    payload_sha256 = (
        integrity.get("payload_sha256")
        if isinstance(integrity, dict)
        else None
    )
    if not isinstance(
        payload_sha256,
        str,
    ) or not payload_sha256:
        raise RuntimeError(
            "Staged release receipt has no usable payload checksum"
        )
    return {
        "receipt_id": receipt_storage["receipt_id"],
        "receipt_file": receipt_storage["receipt_file"],
        "receipt_sha256": receipt_storage["receipt_sha256"],
        "payload_sha256": payload_sha256,
    }


def _dogpark_load_current_staged_manifest(
    manifest_id: str,
) -> tuple[dict, dict]:
    selector = _dogpark_validate_manifest_id(manifest_id)
    manifest, storage = _dogpark_load_manifest("latest-staged")
    if selector not in ("latest", "latest-staged") and selector != manifest[
        "manifest_id"
    ]:
        raise RuntimeError(
            f"Manifest {selector!r} is not the current latest-staged release"
        )
    if storage.get("receipt") is None:
        raise RuntimeError(
            "Production promotion requires a verified latest-staged receipt"
        )
    return manifest, storage


def _dogpark_copy_alias_snapshot(snapshot: dict) -> dict:
    return {
        alias: sorted(
            set(indices)
        )
        for alias, indices in snapshot.items()
    }


def _dogpark_prod_alias_order(manifest: dict) -> list[str]:
    aliases = [_DOGPARK_PROD_ALIAS]
    for entry in manifest["entries"]:
        alias = entry.get("production_alias")
        if alias not in aliases:
            aliases.append(alias)
    return aliases


def _dogpark_alias_action(
    operation: str,
    index_name: str,
    alias: str,
) -> dict:
    return {
        operation: {
            "index": index_name,
            "alias": alias,
        }
    }


def _dogpark_build_prod_reconciliation(
    manifest: dict,
    staging_receipt: dict,
    mark_old_deprecated_for_deletion: bool,
) -> dict:
    """Derive one deterministic production transition from staged evidence."""
    _dogpark_validate_manifest_contract(manifest)
    if not manifest["complete"]:
        raise RuntimeError(
            "Cannot promote production from an incomplete manifest"
        )
    if not isinstance(
        mark_old_deprecated_for_deletion,
        bool,
    ):
        raise TypeError(
            "mark_old_deprecated_for_deletion must be a boolean"
        )

    expected_before = _dogpark_copy_alias_snapshot(
        staging_receipt["verified_aliases"]
    )
    production_aliases = _dogpark_prod_alias_order(manifest)
    required_aliases = [
        _DOGPARK_PROD_ALIAS,
        _DOGPARK_STAGING_ALIAS,
        _DOGPARK_DEPRECATED_ALIAS,
        _DOGPARK_DELETE_ALIAS,
    ] + [
        alias for alias in production_aliases if alias != _DOGPARK_PROD_ALIAS
    ]
    missing_aliases = [
        alias for alias in required_aliases if alias not in expected_before
    ]
    if missing_aliases:
        raise RuntimeError(
            "Staging receipt is missing managed alias snapshots: "
            f"{missing_aliases!r}"
        )

    desired_entries = [
        entry for entry in manifest["entries"] if entry["decision"] != "blocked"
    ]
    desired_by_alias = {alias: [] for alias in production_aliases}
    for entry in desired_entries:
        desired_by_alias[entry["production_alias"]].append(entry["index"])
    desired_by_alias = {
        alias: sorted(indices)
        for alias, indices in desired_by_alias.items()
    }
    desired_indices = {
        index_name
        for indices in desired_by_alias.values()
        for index_name in indices
    }

    recorded_holders = {alias: {} for alias in production_aliases}
    for entry in manifest["entries"]:
        alias = entry["production_alias"]
        for index_name in entry["observed_prod_indices"]:
            recorded_holders[alias].setdefault(index_name, []).append(
                entry["config"]
            )
    duplicate_ownership = {
        alias: {
            index_name: configs
            for index_name, configs in holders.items()
            if len(configs) != 1
        }
        for alias, holders in recorded_holders.items()
    }
    duplicate_ownership = {
        alias: holders
        for alias, holders in duplicate_ownership.items()
        if holders
    }
    if duplicate_ownership:
        raise RuntimeError(
            "Production alias holders have ambiguous datasource ownership: "
            f"{duplicate_ownership!r}"
        )
    unowned_holders = {
        alias: sorted(
            set(expected_before[alias])
            - set(recorded_holders[alias])
        )
        for alias in production_aliases
    }
    unowned_holders = {
        alias: holders
        for alias, holders in unowned_holders.items()
        if holders
    }
    if unowned_holders:
        raise RuntimeError(
            "Production aliases contain holders not owned by the staged "
            f"manifest: {unowned_holders!r}"
        )

    wrong_aliases = []
    for intended_alias, intended_indices in desired_by_alias.items():
        for other_alias in production_aliases:
            if other_alias == intended_alias:
                continue
            for index_name in intended_indices:
                if index_name in expected_before[other_alias]:
                    wrong_aliases.append(
                        {
                            "index": index_name,
                            "expected_alias": intended_alias,
                            "actual_alias": other_alias,
                        }
                    )
    if wrong_aliases:
        raise RuntimeError(
            "Desired indices have conflicting production alias policy: "
            f"{wrong_aliases!r}"
        )

    conflicting_desired = {
        alias: sorted(
            desired_indices & set(expected_before[alias])
        )
        for alias in (_DOGPARK_DEPRECATED_ALIAS, _DOGPARK_DELETE_ALIAS)
    }
    conflicting_desired = {
        alias: indices
        for alias, indices in conflicting_desired.items()
        if indices
    }
    if conflicting_desired:
        raise RuntimeError(
            "Desired production indices conflict with deprecated/delete state: "
            f"{conflicting_desired!r}"
        )

    current_prod_indices = {
        index_name
        for alias in production_aliases
        for index_name in expected_before[alias]
    }
    prod_release_conflicts = {
        alias: sorted(
            current_prod_indices
            & set(expected_before[alias])
        )
        for alias in (_DOGPARK_DEPRECATED_ALIAS, _DOGPARK_DELETE_ALIAS)
    }
    prod_release_conflicts = {
        alias: indices
        for alias, indices in prod_release_conflicts.items()
        if indices
    }
    if prod_release_conflicts:
        raise RuntimeError(
            "Current production indices conflict with deprecated/delete "
            f"state: {prod_release_conflicts!r}"
        )

    decision_conflicts = []
    for entry in desired_entries:
        is_current = entry["index"] in expected_before[
            entry["production_alias"]
        ]
        if (
            entry["decision"] == "keep_current_prod" and not is_current
        ) or (entry["decision"] == "promote" and is_current):
            decision_conflicts.append(
                {
                    "config": entry["config"],
                    "index": entry["index"],
                    "decision": entry["decision"],
                }
            )
    if decision_conflicts:
        raise RuntimeError(
            "Staged production holders disagree with manifest decisions: "
            f"{decision_conflicts!r}"
        )

    normal_promote = sorted(
        entry["index"]
        for entry in desired_entries
        if entry["production_alias"] == _DOGPARK_PROD_ALIAS
        and entry["decision"] == "promote"
    )
    missing_staging = sorted(
        set(normal_promote)
        - set(expected_before[_DOGPARK_STAGING_ALIAS])
    )
    if missing_staging:
        raise RuntimeError(
            "Normal production promotions are missing their required staging "
            f"alias: {missing_staging!r}"
        )

    expected_after = _dogpark_copy_alias_snapshot(expected_before)
    production_changes = {}
    displaced = set()
    promote = []
    keep = []
    actions = []
    for alias in production_aliases:
        before_set = set(expected_before[alias])
        desired_set = set(desired_by_alias[alias])
        additions = sorted(desired_set - before_set)
        removals = sorted(before_set - desired_set)
        retained = sorted(before_set & desired_set)
        promote.extend(additions)
        keep.extend(retained)
        displaced.update(removals)
        production_changes[alias] = {
            "add": additions,
            "remove": removals,
            "keep": retained,
        }
        expected_after[alias] = sorted(desired_set)
        actions.extend(
            _dogpark_alias_action("add", index_name, alias)
            for index_name in additions
        )
        actions.extend(
            _dogpark_alias_action("remove", index_name, alias)
            for index_name in removals
        )

    staging_clear = sorted(normal_promote)
    expected_after[_DOGPARK_STAGING_ALIAS] = sorted(
        set(expected_before[_DOGPARK_STAGING_ALIAS])
        - set(staging_clear)
    )
    actions.extend(
        _dogpark_alias_action(
            "remove",
            index_name,
            _DOGPARK_STAGING_ALIAS,
        )
        for index_name in staging_clear
    )

    deprecate = sorted(displaced - desired_indices)
    before_deprecated = set(
        expected_before[_DOGPARK_DEPRECATED_ALIAS]
    )
    expected_after[_DOGPARK_DEPRECATED_ALIAS] = sorted(
        before_deprecated | set(deprecate)
    )
    actions.extend(
        _dogpark_alias_action(
            "add",
            index_name,
            _DOGPARK_DEPRECATED_ALIAS,
        )
        for index_name in deprecate
        if index_name not in before_deprecated
    )

    before_delete = set(expected_before[_DOGPARK_DELETE_ALIAS])
    old_deprecated_to_delete = (
        sorted(before_deprecated - before_delete)
        if mark_old_deprecated_for_deletion
        else []
    )
    expected_after[_DOGPARK_DELETE_ALIAS] = sorted(
        before_delete
        | (
            before_deprecated
            if mark_old_deprecated_for_deletion
            else set()
        )
    )
    actions.extend(
        _dogpark_alias_action(
            "add",
            index_name,
            _DOGPARK_DELETE_ALIAS,
        )
        for index_name in old_deprecated_to_delete
    )

    action_pairs = []
    for action in actions:
        operation, details = next(iter(action.items()))
        action_pairs.append(
            (operation, details["index"], details["alias"])
        )
    if len(action_pairs) != len(
        set(action_pairs)
    ):
        raise RuntimeError(
            "Production reconciliation contains duplicate alias actions"
        )
    opposite_pairs = {
        (index_name, alias)
        for operation, index_name, alias in action_pairs
        if operation == "add"
    } & {
        (index_name, alias)
        for operation, index_name, alias in action_pairs
        if operation == "remove"
    }
    if opposite_pairs:
        raise RuntimeError(
            "Production reconciliation both adds and removes an alias: "
            f"{sorted(opposite_pairs)!r}"
        )

    transition_body = {
        "expected_before_aliases": expected_before,
        "expected_after_aliases": expected_after,
        "actions": actions,
        "mark_old_deprecated_for_deletion": (
            mark_old_deprecated_for_deletion
        ),
    }
    return {
        "expected_before_aliases": expected_before,
        "expected_after_aliases": expected_after,
        "actions": actions,
        "production_aliases": production_changes,
        "promote_indices": sorted(promote),
        "keep_indices": sorted(keep),
        "displaced_indices": sorted(displaced),
        "deprecate_indices": deprecate,
        "staging_clear_indices": staging_clear,
        "old_deprecated_to_delete": old_deprecated_to_delete,
        "mark_old_deprecated_for_deletion": (
            mark_old_deprecated_for_deletion
        ),
        "transition_sha256": _dogpark_sha256(
            _dogpark_canonical_json(transition_body)
        ),
    }


def _dogpark_classify_prod_state(
    reconciliation: dict,
    live_aliases: dict,
) -> str:
    expected_before = reconciliation["expected_before_aliases"]
    expected_after = reconciliation["expected_after_aliases"]
    if expected_before == expected_after and live_aliases == expected_before:
        return "no_changes_required"
    if live_aliases == expected_after:
        return "already_applied"
    if live_aliases == expected_before:
        return "ready_to_apply"
    drift = {
        alias: {
            "expected_before": expected_before[alias],
            "expected_after": expected_after[alias],
            "actual": live_aliases.get(alias),
        }
        for alias in expected_before
        if live_aliases.get(alias)
        not in (expected_before[alias], expected_after[alias])
        or (
            live_aliases.get(alias) == expected_before[alias]
            and live_aliases != expected_before
        )
        or (
            live_aliases.get(alias) == expected_after[alias]
            and live_aliases != expected_after
        )
    }
    raise RuntimeError(
        "Production alias state drifted from both the verified staging state "
        f"and exact production post-state; refusing promotion: {drift!r}"
    )


def _dogpark_affected_index_uuids(
    manifest: dict,
    actions: list[dict],
    es_uri: str,
) -> dict:
    affected = sorted(
        {
            details["index"]
            for action in actions
            for details in action.values()
        }
    )
    manifest_uuids = {
        entry["index"]: entry["index_uuid"]
        for entry in manifest["entries"]
        if entry["decision"] != "blocked"
    }
    identities = {}
    for index_name in affected:
        index_uuid = manifest_uuids.get(index_name)
        if index_uuid is None:
            index_uuid = _dogpark_index_uuid(index_name, es_uri)
        if not isinstance(
            index_uuid,
            str,
        ) or not index_uuid:
            raise RuntimeError(
                f"Could not verify UUID for affected index {index_name!r}"
            )
        identities[index_name] = index_uuid
    return identities


def _dogpark_prepare_prod_release(
    manifest_id: str,
    es_uri: str,
    expected_cluster: str,
    mark_old_deprecated_for_deletion: bool,
) -> tuple[dict, dict]:
    manifest, manifest_storage = _dogpark_load_current_staged_manifest(
        manifest_id
    )
    staging_receipt_storage = manifest_storage["receipt"]
    staging_receipt = staging_receipt_storage["receipt"]
    identity = _dogpark_assert_manifest_target(
        manifest,
        es_uri,
        expected_cluster,
    )
    reconciliation = _dogpark_build_prod_reconciliation(
        manifest,
        staging_receipt,
        mark_old_deprecated_for_deletion,
    )
    live_aliases = _dogpark_live_aliases_for_manifest(manifest, es_uri)
    state = _dogpark_classify_prod_state(reconciliation, live_aliases)
    affected_index_uuids = _dogpark_affected_index_uuids(
        manifest,
        reconciliation["actions"],
        es_uri,
    )
    plan = {
        "schema": "dogpark.production-plan/v1",
        "generated_at": _dogpark_utc_now(),
        "state": state,
        "manifest": {
            "manifest_id": manifest["manifest_id"],
            "manifest_file": manifest_storage["manifest_file"],
            "manifest_sha256": manifest_storage["manifest_sha256"],
            "payload_sha256": manifest["integrity"]["payload_sha256"],
        },
        "staging_receipt": _dogpark_staging_receipt_link(
            staging_receipt_storage
        ),
        "target": {
            "es_uri": manifest["target"]["es_uri"],
            "cluster_name": identity["cluster_name"],
            "cluster_uuid": identity["cluster_uuid"],
        },
        "policy": {
            "mark_old_deprecated_for_deletion": (
                mark_old_deprecated_for_deletion
            ),
            "physical_deletion": False,
        },
        "transition_sha256": reconciliation["transition_sha256"],
        "expected_before_aliases": reconciliation[
            "expected_before_aliases"
        ],
        "observed_aliases": live_aliases,
        "expected_after_aliases": reconciliation["expected_after_aliases"],
        "actions": reconciliation["actions"],
        "production_aliases": reconciliation["production_aliases"],
        "promote_indices": reconciliation["promote_indices"],
        "keep_indices": reconciliation["keep_indices"],
        "displaced_indices": reconciliation["displaced_indices"],
        "deprecate_indices": reconciliation["deprecate_indices"],
        "staging_clear_indices": reconciliation["staging_clear_indices"],
        "old_deprecated_to_delete": reconciliation[
            "old_deprecated_to_delete"
        ],
        "affected_index_uuids": affected_index_uuids,
        "counts": {
            "actions": len(reconciliation["actions"]),
            "promote": len(
                reconciliation["promote_indices"]
            ),
            "keep": len(reconciliation["keep_indices"]),
            "deprecate": len(
                reconciliation["deprecate_indices"]
            ),
            "staging_clear": len(
                reconciliation["staging_clear_indices"]
            ),
            "old_deprecated_to_delete": len(
                reconciliation["old_deprecated_to_delete"]
            ),
        },
    }
    return plan, {
        "manifest": manifest,
        "manifest_storage": manifest_storage,
        "staging_receipt": staging_receipt,
        "staging_receipt_storage": staging_receipt_storage,
        "identity": identity,
        "reconciliation": reconciliation,
    }


def _dogpark_post_prod_reconciliation(
    actions: list[dict],
    es_uri: str,
) -> dict:
    if not actions:
        raise RuntimeError(
            "Refusing an empty Elasticsearch alias update"
        )
    response = _dogpark_request_json(
        f"{es_uri.rstrip('/')}/_aliases",
        method="POST",
        payload={"actions": actions},
    )
    if (
        not isinstance(response, dict)
        or not response.get("acknowledged", False)
        or response.get("errors", False)
    ):
        raise RuntimeError(
            "Elasticsearch production alias update was not fully acknowledged: "
            f"{response!r}"
        )
    return response


def _dogpark_verify_prod_receipt(
    receipt: dict,
    expected_id: str,
    manifest: dict,
    staging_receipt: dict,
    staging_receipt_storage: dict,
) -> None:
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema") != _DOGPARK_PROD_RECEIPT_SCHEMA
    ):
        raise RuntimeError(
            "Unsupported or invalid production receipt"
        )
    receipt_id = _dogpark_validate_manifest_id(receipt.get("receipt_id"))
    if receipt_id != expected_id:
        raise RuntimeError(
            "Production receipt id does not match its pointer"
        )
    integrity = receipt.get("integrity")
    if (
        not isinstance(integrity, dict)
        or integrity.get("algorithm") != "sha256"
    ):
        raise RuntimeError(
            "Production receipt has no SHA-256 integrity record"
        )
    body = dict(receipt)
    body.pop("integrity", None)
    if _dogpark_sha256(_dogpark_canonical_json(body)) != integrity.get(
        "payload_sha256"
    ):
        raise RuntimeError(
            "Production receipt integrity check failed"
        )
    identity_body = dict(body)
    identity_body.pop("receipt_id", None)
    verified_at = identity_body.get("verified_at")
    if not isinstance(
        verified_at,
        str,
    ):
        raise RuntimeError(
            "Production receipt has no verification timestamp"
        )
    calculated_id = (
        f"{_dogpark_manifest_timestamp(verified_at)}-"
        f"{_dogpark_sha256(_dogpark_canonical_json(identity_body))[:12]}"
    )
    if receipt_id != calculated_id:
        raise RuntimeError(
            "Production receipt id does not match its immutable content"
        )

    if (
        receipt.get("manifest_id") != manifest["manifest_id"]
        or receipt.get("manifest_payload_sha256")
        != manifest["integrity"]["payload_sha256"]
    ):
        raise RuntimeError(
            "Production receipt does not belong to the selected manifest"
        )
    expected_staging_link = _dogpark_staging_receipt_link(
        staging_receipt_storage
    )
    if receipt.get("staging_receipt") != expected_staging_link:
        raise RuntimeError(
            "Production receipt staging evidence link is invalid"
        )
    if staging_receipt_storage.get("receipt") != staging_receipt:
        raise RuntimeError(
            "Production receipt staging evidence payload is inconsistent"
        )

    manifest_target = manifest["target"]
    expected_target = {
        "es_uri": manifest_target["es_uri"],
        "cluster_name": manifest_target["cluster_name"],
        "cluster_uuid": manifest_target["cluster_uuid"],
    }
    if receipt.get("target") != expected_target:
        raise RuntimeError(
            "Production receipt target does not match the manifest"
        )
    policy = receipt.get("policy")
    if (
        not isinstance(policy, dict)
        or not isinstance(
            policy.get("mark_old_deprecated_for_deletion"),
            bool,
        )
        or policy.get("physical_deletion") is not False
    ):
        raise RuntimeError(
            "Production receipt has an invalid release policy"
        )
    reconciliation = _dogpark_build_prod_reconciliation(
        manifest,
        staging_receipt,
        policy["mark_old_deprecated_for_deletion"],
    )
    expected_fields = {
        "transition_sha256": reconciliation["transition_sha256"],
        "expected_before_aliases": reconciliation[
            "expected_before_aliases"
        ],
        "expected_after_aliases": reconciliation["expected_after_aliases"],
        "planned_actions": reconciliation["actions"],
        "production_aliases": reconciliation["production_aliases"],
        "promote_indices": reconciliation["promote_indices"],
        "keep_indices": reconciliation["keep_indices"],
        "displaced_indices": reconciliation["displaced_indices"],
        "deprecate_indices": reconciliation["deprecate_indices"],
        "staging_clear_indices": reconciliation["staging_clear_indices"],
        "old_deprecated_to_delete": reconciliation[
            "old_deprecated_to_delete"
        ],
    }
    mismatched = {
        field: {
            "expected": value,
            "actual": receipt.get(field),
        }
        for field, value in expected_fields.items()
        if receipt.get(field) != value
    }
    if mismatched:
        raise RuntimeError(
            "Production receipt does not match the deterministic transition: "
            f"{mismatched!r}"
        )

    if receipt.get("verified_aliases") != reconciliation[
        "expected_after_aliases"
    ]:
        raise RuntimeError(
            "Production receipt does not prove the expected alias state"
        )
    outcome = receipt.get("outcome")
    if outcome == "aliases_reconciled":
        expected_observed = reconciliation["expected_before_aliases"]
        expected_applied = reconciliation["actions"]
    elif outcome == "already_applied":
        expected_observed = reconciliation["expected_after_aliases"]
        expected_applied = []
    elif outcome == "no_aliases_required":
        if reconciliation["expected_before_aliases"] != reconciliation[
            "expected_after_aliases"
        ]:
            raise RuntimeError(
                "Production receipt claims no aliases were required for a "
                "non-empty transition"
            )
        expected_observed = reconciliation["expected_before_aliases"]
        expected_applied = []
    else:
        raise RuntimeError(
            f"Unsupported production receipt outcome: {outcome!r}"
        )
    if (
        receipt.get("observed_before_aliases") != expected_observed
        or receipt.get("applied_actions") != expected_applied
    ):
        raise RuntimeError(
            "Production receipt mutation audit does not match its outcome"
        )

    affected = receipt.get("affected_index_uuids")
    expected_affected = sorted(
        {
            details["index"]
            for action in reconciliation["actions"]
            for details in action.values()
        }
    )
    if (
        not isinstance(affected, dict)
        or sorted(affected) != expected_affected
        or not all(
            isinstance(index_uuid, str)
            and index_uuid
            for index_uuid in affected.values()
        )
    ):
        raise RuntimeError(
            "Production receipt affected index identities are invalid"
        )
    desired_uuids = {
        entry["index"]: entry["index_uuid"]
        for entry in manifest["entries"]
        if entry["decision"] != "blocked"
    }
    desired_identity_mismatches = {
        index_name: {
            "expected": desired_uuids[index_name],
            "actual": affected.get(index_name),
        }
        for index_name in expected_affected
        if index_name in desired_uuids
        and affected.get(index_name) != desired_uuids[index_name]
    }
    if desired_identity_mismatches:
        raise RuntimeError(
            "Production receipt desired index identities do not match the "
            f"manifest: {desired_identity_mismatches!r}"
        )


def _dogpark_persist_prod_receipt(
    manifest: dict,
    manifest_storage: dict,
    receipt: dict,
) -> dict:
    receipt_id = _dogpark_validate_manifest_id(receipt["receipt_id"])
    relative_file = _dogpark_os.path.join(
        "receipts",
        manifest["manifest_id"],
        f"{receipt_id}.json",
    )
    receipt_path = _dogpark_os.path.join(_DOGPARK_MANIFESTS_DIR, relative_file)
    receipt_sha256 = _dogpark_write_immutable_json(receipt_path, receipt)
    staging_link = receipt["staging_receipt"]
    pointer = _dogpark_publish_manifest_pointer(
        "latest-prod",
        manifest,
        manifest_storage["manifest_sha256"],
        receipt_id=receipt_id,
        receipt_file=relative_file,
        receipt_sha256=receipt_sha256,
        receipt_payload_sha256=receipt["integrity"]["payload_sha256"],
        staging_receipt_id=staging_link["receipt_id"],
        staging_receipt_sha256=staging_link["receipt_sha256"],
        transition_sha256=receipt["transition_sha256"],
    )
    return {
        "receipt_id": receipt_id,
        "receipt_file": relative_file,
        "receipt_path": receipt_path,
        "receipt_sha256": receipt_sha256,
        "latest_pointer": _dogpark_manifest_pointer_path("latest-prod"),
        "pointer": pointer,
    }


def _dogpark_apply_staging_manifest_locked(
    manifest: dict,
    manifest_storage: dict,
    es_uri: str,
    expected_cluster: str,
) -> dict:
    _dogpark_validate_manifest_contract(manifest)
    if not manifest["complete"]:
        raise RuntimeError(
            "Cannot apply staging from an incomplete manifest"
        )
    identity = _dogpark_assert_manifest_target(
        manifest,
        es_uri,
        expected_cluster,
    )
    expected_before = manifest["observed_aliases"]
    expected_after = _dogpark_expected_post_staging_aliases(manifest)
    aliases_before_apply = _dogpark_live_aliases_for_manifest(manifest, es_uri)
    if expected_before == expected_after and aliases_before_apply == expected_before:
        state = "no_changes_required"
    elif aliases_before_apply == expected_after:
        state = "already_applied"
    elif aliases_before_apply == expected_before:
        state = "not_applied"
    else:
        drift = {
            alias: {
                "expected_before": expected_before[alias],
                "expected_after": expected_after[alias],
                "actual": aliases_before_apply[alias],
            }
            for alias in expected_before
            if aliases_before_apply[alias]
            not in (expected_before[alias], expected_after[alias])
        }
        raise RuntimeError(
            "Alias state drifted from the frozen manifest; refusing staging: "
            f"{drift!r}"
        )

    response = None
    tagged = []
    untagged = []
    applied = False
    if state == "not_applied" and (
        manifest["staging_add_indices"]
        or manifest["staging_remove_indices"]
    ):
        try:
            response = _dogpark_post_staging_reconciliation(
                manifest["staging_add_indices"],
                manifest["staging_remove_indices"],
                es_uri,
            )
        except BaseException as exc:
            raise RuntimeError(
                "Staging alias application failed after manifest "
                f"{manifest['manifest_id']!r} was frozen"
            ) from exc
        tagged = builtins.list(manifest["staging_add_indices"])
        untagged = builtins.list(manifest["staging_remove_indices"])
        applied = True

    verified_aliases = _dogpark_live_aliases_for_manifest(manifest, es_uri)
    if verified_aliases != expected_after:
        raise RuntimeError(
            "Staging alias readback does not match the frozen manifest"
        )
    verified_at = _dogpark_utc_now()
    receipt = _dogpark_finalize_receipt(
        {
            "schema": _DOGPARK_STAGING_RECEIPT_SCHEMA,
            "verified_at": verified_at,
            "manifest_id": manifest["manifest_id"],
            "manifest_payload_sha256": manifest["integrity"]["payload_sha256"],
            "target": {
                "es_uri": manifest["target"]["es_uri"],
                "cluster_name": identity["cluster_name"],
                "cluster_uuid": identity["cluster_uuid"],
            },
            "outcome": (
                "aliases_reconciled"
                if applied
                else (
                    "already_applied"
                    if state == "already_applied"
                    else "no_aliases_required"
                )
            ),
            "requested_additions": manifest["staging_add_indices"],
            "requested_removals": manifest["staging_remove_indices"],
            "tagged_this_run": tagged,
            "removed_this_run": untagged,
            "required_staging_indices": manifest["staging_required_indices"],
            "verified_aliases": verified_aliases,
        }
    )
    receipt_storage = _dogpark_persist_staging_receipt(
        manifest,
        manifest_storage,
        receipt,
    )
    return {
        "manifest": {
            "manifest_id": manifest["manifest_id"],
            "manifest_file": manifest_storage["manifest_file"],
            "manifest_sha256": manifest_storage["manifest_sha256"],
        },
        "receipt": receipt,
        "receipt_storage": receipt_storage,
        "applied": applied,
        "tagged": tagged,
        "untagged": untagged,
        "elasticsearch_response": response,
    }


def freeze_release_manifest(
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    expected_indexer_env: str = _DOGPARK_EXPECTED_INDEXER_ENV,
) -> dict:
    """Persist one complete immutable desired-production manifest."""
    with _dogpark_manifest_lock():
        manifest = plan_release_manifest(
            es_uri,
            expected_cluster,
            expected_indexer_env,
        )
        if not manifest["complete"]:
            raise RuntimeError(
                "Release manifest is incomplete; blocked datasources: "
                f"{[entry['config'] for entry in manifest['blocked']]!r}"
            )
        storage = _dogpark_persist_manifest(manifest)
    return {"manifest": manifest, "storage": storage}


def get_release_manifest(manifest_id: str = "latest-frozen") -> dict:
    """Load an immutable manifest via an id or a verified latest pointer."""
    manifest, _storage = _dogpark_load_manifest(manifest_id)
    _dogpark_validate_manifest_contract(manifest)
    return manifest


def plan_prod_release(
    manifest_id: str = "latest-staged",
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    mark_old_deprecated_for_deletion: bool = True,
) -> dict:
    """Preview production aliases from the current verified staged release.

    Supplying an exact manifest id pins operator intent, but that id must still
    be the manifest selected by ``latest-staged`` so a frozen-yet-unstaged
    release can never bypass staging evidence.
    """
    plan, _context = _dogpark_prepare_prod_release(
        manifest_id,
        es_uri,
        expected_cluster,
        mark_old_deprecated_for_deletion,
    )
    return plan


def release_prod_manifest(
    manifest_id: str = "latest-staged",
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    mark_old_deprecated_for_deletion: bool = True,
) -> dict:
    """Atomically promote the current staged manifest and persist evidence."""
    with _dogpark_manifest_lock():
        plan, context = _dogpark_prepare_prod_release(
            manifest_id,
            es_uri,
            expected_cluster,
            mark_old_deprecated_for_deletion,
        )
        reconciliation = context["reconciliation"]
        response = None
        applied = False
        if plan["state"] == "ready_to_apply":
            if not reconciliation["actions"]:
                raise RuntimeError(
                    "Production plan is marked ready but has no alias actions"
                )
            try:
                response = _dogpark_post_prod_reconciliation(
                    reconciliation["actions"],
                    es_uri,
                )
            except Exception as exc:
                raise RuntimeError(
                    "Production alias application did not complete with a "
                    "verified response; retry the same staged manifest to "
                    "determine whether Elasticsearch applied it"
                ) from exc
            applied = True

        verified_aliases = _dogpark_live_aliases_for_manifest(
            context["manifest"],
            es_uri,
        )
        if verified_aliases != reconciliation["expected_after_aliases"]:
            raise RuntimeError(
                "Production alias readback does not match the deterministic "
                "post-state; no production receipt was published"
            )

        if applied:
            outcome = "aliases_reconciled"
            applied_actions = reconciliation["actions"]
        elif plan["state"] == "already_applied":
            outcome = "already_applied"
            applied_actions = []
        else:
            outcome = "no_aliases_required"
            applied_actions = []
        receipt = _dogpark_finalize_receipt(
            {
                "schema": _DOGPARK_PROD_RECEIPT_SCHEMA,
                "verified_at": _dogpark_utc_now(),
                "manifest_id": context["manifest"]["manifest_id"],
                "manifest_payload_sha256": context["manifest"]["integrity"][
                    "payload_sha256"
                ],
                "staging_receipt": plan["staging_receipt"],
                "target": plan["target"],
                "policy": plan["policy"],
                "transition_sha256": reconciliation["transition_sha256"],
                "outcome": outcome,
                "expected_before_aliases": reconciliation[
                    "expected_before_aliases"
                ],
                "observed_before_aliases": plan["observed_aliases"],
                "expected_after_aliases": reconciliation[
                    "expected_after_aliases"
                ],
                "verified_aliases": verified_aliases,
                "planned_actions": reconciliation["actions"],
                "applied_actions": applied_actions,
                "production_aliases": reconciliation[
                    "production_aliases"
                ],
                "promote_indices": reconciliation["promote_indices"],
                "keep_indices": reconciliation["keep_indices"],
                "displaced_indices": reconciliation["displaced_indices"],
                "deprecate_indices": reconciliation["deprecate_indices"],
                "staging_clear_indices": reconciliation[
                    "staging_clear_indices"
                ],
                "old_deprecated_to_delete": reconciliation[
                    "old_deprecated_to_delete"
                ],
                "affected_index_uuids": plan["affected_index_uuids"],
            }
        )
        _dogpark_verify_prod_receipt(
            receipt,
            receipt["receipt_id"],
            context["manifest"],
            context["staging_receipt"],
            context["staging_receipt_storage"],
        )
        receipt_storage = _dogpark_persist_prod_receipt(
            context["manifest"],
            context["manifest_storage"],
            receipt,
        )
        return {
            "plan": plan,
            "receipt": receipt,
            "receipt_storage": receipt_storage,
            "applied": applied,
            "elasticsearch_response": response,
        }


def assign_staging_manifest(
    manifest_id: str,
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
) -> dict:
    """Apply staging aliases from one frozen manifest without rediscovery."""
    with _dogpark_manifest_lock():
        manifest, storage = _dogpark_load_manifest(manifest_id)
        return _dogpark_apply_staging_manifest_locked(
            manifest,
            storage,
            es_uri,
            expected_cluster,
        )


def assign_staging_tags(
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
    expected_indexer_env: str = _DOGPARK_EXPECTED_INDEXER_ENV,
) -> dict:
    """Freeze the inferred release and apply its exact staging reconciliation."""
    frozen = freeze_release_manifest(
        es_uri,
        expected_cluster,
        expected_indexer_env,
    )
    manifest = frozen["manifest"]
    result = assign_staging_manifest(
        manifest["manifest_id"],
        es_uri,
        expected_cluster,
    )
    result["counts"] = manifest.get("counts", {})
    result["desired_indices"] = manifest.get("desired_indices", [])
    return result


def _dogpark_exact_index_name(index_name) -> str:
    if not isinstance(index_name, str):
        raise TypeError("index_name must be a string")
    normalized = index_name.strip()
    if not normalized:
        raise ValueError("index_name must not be empty")
    if any(character in normalized for character in "*?,"):
        raise ValueError(
            "index_name must be one exact index name; wildcards and lists are not allowed"
        )
    return normalized


def assign_staging_tag(
    index_name: str,
    es_uri: str = _DOGPARK_ES_URI,
    expected_cluster: str = _DOGPARK_EXPECTED_CLUSTER,
) -> dict:
    """Assign ``dingo_staging`` to one exact operator-selected index.

    This is an explicit readiness override: it does not require the index to be
    the newest build or its final hub job to be successful. Cluster identity,
    concrete-index existence, release-state conflicts, known datasource
    ownership, and special-dataset staging policy are still enforced.
    """
    index_name = _dogpark_exact_index_name(index_name)
    actual_cluster = _dogpark_validate_cluster(es_uri, expected_cluster)
    if not _dogpark_index_exists(index_name, es_uri):
        raise RuntimeError(
            f"Concrete Elasticsearch index {index_name!r} was not found"
        )

    build_documents = _dogpark_load_build_documents()
    known_ownership = _dogpark_build_index_ownership(build_documents)
    dataset = _dogpark_dataset_from_index(index_name, known_ownership)
    if dataset in DOGPARK_NO_STAGING_TAG_DATASETS:
        raise RuntimeError(
            f"Staging tags are disabled by policy for datasource {dataset!r}"
        )

    aliases = _dogpark_get_alias_inventory(es_uri)
    prod_holders = []
    staging_holders = []
    if dataset is not None:
        prod_holders = _dogpark_indices_by_dataset(
            aliases["prod"],
            known_ownership,
        ).get(dataset, [])
        staging_holders = _dogpark_indices_by_dataset(
            aliases["staging"],
            known_ownership,
        ).get(dataset, [])

    if len(prod_holders) > 1:
        raise RuntimeError(
            f"Datasource {dataset!r} has multiple production holders: {prod_holders!r}"
        )
    if len(staging_holders) > 1:
        raise RuntimeError(
            f"Datasource {dataset!r} has multiple staging holders: {staging_holders!r}"
        )

    conflicting_states = [
        state
        for state in ("deprecated", "delete")
        if index_name in aliases[state]
    ]
    if conflicting_states:
        raise RuntimeError(
            f"Index {index_name!r} has conflicting release state(s): "
            f"{conflicting_states!r}"
        )

    result = {
        "generated_at": _dogpark_utc_now(),
        "mode": "single_index_override",
        "target": {
            "es_uri": es_uri,
            "cluster_name": actual_cluster,
        },
        "index": index_name,
        "dataset": dataset,
        "readiness_checks": "bypassed_by_explicit_operator_selection",
        "applied": False,
        "tagged": [],
    }

    if index_name in aliases["prod"]:
        result["reason"] = "already_prod"
        return result
    if index_name in aliases["staging"]:
        result["reason"] = "already_staging"
        return result
    if staging_holders:
        raise RuntimeError(
            f"Datasource {dataset!r} already has a different staging holder: "
            f"{staging_holders!r}"
        )

    response = _dogpark_post_staging_aliases([index_name], es_uri)
    result["reason"] = "explicit_index_staged"
    result["applied"] = True
    result["tagged"] = [index_name]
    result["elasticsearch_response"] = response
    return result


# Alias mutation is intentionally confirmation-gated by the hub terminal.
freeze_release_manifest.confirm = True
assign_staging_tags.confirm = True
assign_staging_manifest.confirm = True
assign_staging_tag.confirm = True
release_prod_manifest.confirm = True

"""Alias-authoritative staging and release operations shared by Hub hooks.

Locks serialize hook mutations within one Hub process. Elasticsearch remains the
source of release state; no build database or release records are consulted.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime
import re
import threading

from hooks.dogpark_hook_utils import builds

DEPRECATED_ALIAS = "dingo_deprecated"
DELETE_ALIAS = "dingo_to_be_deleted"
ALIAS_PROFILES = {
    "default": {
        "staging_alias": "dingo_staging",
        "special_staging_aliases": {
            "ubergraph_nodes_info": "ubergraph_nodes_mapping_staging",
            "ubergraph_nodes": "ubergraph_nodes_staging",
        },
        "deprecated_alias": DEPRECATED_ALIAS,
        "delete_alias": DELETE_ALIAS,
    },
    "testing": {
        "staging_alias": "prod_testing_staging",
        "special_staging_aliases": {
            "ubergraph_nodes_info": "ubergraph_nodes_mapping_testing_staging",
            "ubergraph_nodes": "ubergraph_nodes_testing_staging",
        },
        "deprecated_alias": "prod_testing_deprecated",
        "delete_alias": "prod_testing_to_be_deleted",
    },
}
_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def _exact_name(name):
    if (
        not isinstance(name, str)
        or not name
        or name != name.strip()
        or name in {".", ".."}
        or name.startswith(("_", "-", "+"))
        or any(char.isspace() or char in '*?,#:/\\<>|"[]' for char in name)
    ):
        raise ValueError(f"Expected one exact concrete index name: {name!r}")
    return name


def validate_index_names(index_names):
    """Validate an explicit batch without interpreting aliases or wildcards."""
    if index_names is None or isinstance(index_names, (str, bytes, Mapping)):
        raise TypeError("index_names must be a list of exact index names")
    names = [_exact_name(name) for name in index_names]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate index names are not allowed")
    return names


def _sources(sources):
    if sources is None:
        return {}
    if not isinstance(sources, Mapping):
        raise TypeError("sources must map index names to datasource names")
    result = dict(sources)
    for name, source in result.items():
        _exact_name(name)
        if source not in builds.DOGPARK_MANAGED_DATASETS:
            raise ValueError(f"Unknown managed datasource: {source!r}")
    return result


def resolve_dataset(index_name, source=None):
    """Resolve a complete build name, exact dataset name, or explicit override."""
    _exact_name(index_name)
    inferred = index_name if index_name in builds.DOGPARK_MANAGED_DATASETS else None
    if inferred is None:
        match = re.fullmatch(r"(.+)_(\d{8})_(?:(?:ci|test|prod|dev)_)?([a-z0-9]+)", index_name)
        if match and match[1] in builds.DOGPARK_MANAGED_DATASETS:
            try:
                datetime.strptime(match[2], "%Y%m%d")
            except ValueError:
                pass
            else:
                inferred = match[1]
    if source is not None:
        if source not in builds.DOGPARK_MANAGED_DATASETS:
            raise ValueError(f"Unknown managed datasource: {source!r}")
        if inferred is not None and inferred != source:
            raise ValueError(f"Datasource override conflicts with index {index_name!r}")
        return source
    if inferred is None:
        raise ValueError(f"Cannot resolve datasource for {index_name!r}; supply sources={{index_name: datasource}}")
    return inferred


def _profile(alias_profile):
    if not isinstance(alias_profile, str) or alias_profile not in ALIAS_PROFILES:
        raise ValueError(f"Unknown alias profile: {alias_profile!r}")
    return ALIAS_PROFILES[alias_profile]


def alias_policy(dataset, *, alias_profile="default"):
    if dataset not in builds.DOGPARK_MANAGED_DATASETS:
        raise ValueError(f"Unknown managed datasource: {dataset!r}")
    profile = _profile(alias_profile)
    staging = profile["special_staging_aliases"].get(dataset, profile["staging_alias"])
    return {"staging_alias": staging, "production_alias": staging.removesuffix("_staging")}


def _managed_aliases(alias_profile="default"):
    profile = _profile(alias_profile)
    aliases = {profile["deprecated_alias"], profile["delete_alias"]}
    for source in builds.DOGPARK_MANAGED_DATASETS:
        aliases.update(alias_policy(source, alias_profile=alias_profile).values())
    return aliases


def _production_aliases():
    return {
        alias_policy(source, alias_profile=profile)["production_alias"]
        for profile in ALIAS_PROFILES
        for source in builds.DOGPARK_MANAGED_DATASETS
    }


def _body(response):
    return response.body if hasattr(response, "body") else response


def _cluster_uuid(client):
    identity = _body(client.info())
    value = identity.get("cluster_uuid") if isinstance(identity, dict) else None
    if not isinstance(value, str) or not value or value == "_na_":
        raise RuntimeError("Elasticsearch returned no usable cluster UUID")
    return value


@contextmanager
def cluster_lock(cluster_uuid):
    """Share a short mutation lock across restore, manual stage, and release."""
    with _LOCKS_GUARD:
        lock = _LOCKS.setdefault(cluster_uuid, threading.RLock())
    with lock:
        yield


def _inventory(client, extra_names=(), *, alias_profile="default"):
    response = _body(
        client.indices.get(
            index=sorted(_managed_aliases(alias_profile) | set(extra_names)),
            features=["settings", "aliases"],
            expand_wildcards="all",
            allow_no_indices=True,
            ignore_unavailable=True,
        )
    )
    if not isinstance(response, dict):
        raise RuntimeError("Elasticsearch index inventory is not an object")
    result = {}
    for name, info in response.items():
        if not isinstance(info, dict):
            raise RuntimeError("Elasticsearch returned invalid index metadata")
        settings = info.get("settings", {}).get("index", {})
        result[name] = {"uuid": settings.get("uuid"), "aliases": set(info.get("aliases", {}))}
    return result


def _uuid(inventory, name):
    value = inventory.get(name, {}).get("uuid")
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"Concrete index {name!r} is missing or has no UUID")
    return value


def _require_active_primaries(client, names):
    """Do not expose an incomplete restore through staging or production."""
    if not names:
        return
    health = _body(client.cluster.health(index=sorted(names), level="indices"))
    indices = health.get("indices", {}) if isinstance(health, dict) else {}
    for name in names:
        index_health = indices.get(name, {})
        primaries = index_health.get("number_of_shards", 0)
        if (
            index_health.get("status") not in {"yellow", "green"}
            or not isinstance(primaries, int)
            or primaries <= 0
            or index_health.get("active_primary_shards") != primaries
        ):
            raise RuntimeError(f"Index {name!r} does not have all active primary shards")


def _selected(names, sources):
    selected = {}
    for name in names:
        if any(name in _managed_aliases(profile) for profile in ALIAS_PROFILES):
            raise ValueError(f"Concrete index {name!r} conflicts with a managed alias name")
        dataset = resolve_dataset(name, sources.get(name))
        if dataset in selected:
            raise ValueError(f"Select at most one index per datasource: {dataset!r}")
        selected[dataset] = name
    return selected


def _holders(inventory, sources, alias_profile):
    profile = _profile(alias_profile)
    groups = {source: {"staging": [], "prod": []} for source in builds.DOGPARK_MANAGED_DATASETS}
    staging_aliases = {alias_policy(source, alias_profile=alias_profile)["staging_alias"] for source in groups}
    production_aliases = {alias_policy(source, alias_profile=alias_profile)["production_alias"] for source in groups}
    for name, info in inventory.items():
        active = info["aliases"] & (staging_aliases | production_aliases)
        if not active:
            continue
        source = resolve_dataset(name, sources.get(name))
        policy = alias_policy(source, alias_profile=alias_profile)
        if active - set(policy.values()):
            raise RuntimeError(f"Index {name!r} has aliases for another datasource policy: {sorted(active)!r}")
        if info["aliases"] & {profile["deprecated_alias"], profile["delete_alias"]}:
            raise RuntimeError(f"Active index {name!r} also has deprecated/delete state")
        _uuid(inventory, name)
        if policy["staging_alias"] in active:
            groups[source]["staging"].append(name)
        if policy["production_alias"] in active:
            groups[source]["prod"].append(name)
    return groups


def _validate_holders(groups, selected):
    for source in selected:
        for role, names in groups[source].items():
            if len(names) > 1:
                raise RuntimeError(f"Datasource {source!r} has multiple {role} holders: {sorted(names)!r}")


def _staging_state(cluster_uuid, selected, groups, inventory, alias_profile):
    return {
        "cluster_uuid": cluster_uuid,
        "alias_profile": alias_profile,
        "sources": {name: source for source, name in selected.items()},
        "staged": {
            source: {name: _uuid(inventory, name) for name in sorted(groups[source]["staging"])} for source in selected
        },
    }


def _alias_state(inventory, alias_profile):
    aliases = _managed_aliases(alias_profile)
    return {
        alias: sorted(name for name, info in inventory.items() if alias in info["aliases"]) for alias in sorted(aliases)
    }


def _action(operation, name, alias):
    details = {"index": name, "alias": alias}
    if operation == "remove":
        details["must_exist"] = True
    return {operation: details}


def _apply(client, cluster_uuid, inventory, names, actions, dry_run, alias_profile):
    expected = _alias_state(inventory, alias_profile)
    identities = {name: _uuid(inventory, name) for name in names if name in inventory}
    expected_index_aliases = {name: set(inventory[name]["aliases"]) for name in identities}
    for action in actions:
        operation, details = next(iter(action.items()))
        holders = set(expected[details["alias"]])
        index_aliases = expected_index_aliases.setdefault(details["index"], set())
        if operation == "add":
            holders.add(details["index"])
            index_aliases.add(details["alias"])
        else:
            holders.remove(details["index"])
            index_aliases.remove(details["alias"])
        expected[details["alias"]] = sorted(holders)
    if dry_run:
        return {"applied": False, "verified": False, "expected_aliases": expected}
    if _cluster_uuid(client) != cluster_uuid:
        raise RuntimeError("Elasticsearch cluster identity changed before alias mutation")
    before = _inventory(client, names, alias_profile=alias_profile)
    if _alias_state(before, alias_profile) != _alias_state(inventory, alias_profile) or any(
        _uuid(before, name) != uuid or before[name]["aliases"] != inventory[name]["aliases"]
        for name, uuid in identities.items()
    ):
        raise RuntimeError("Alias or index identity drifted before mutation")
    if _cluster_uuid(client) != cluster_uuid:
        raise RuntimeError("Elasticsearch cluster identity changed before alias mutation")
    response = None
    error = None
    if actions:
        try:
            response = _body(client.indices.update_aliases(actions=actions))
        except Exception as exc:
            error = exc
        if error is None and (
            not isinstance(response, dict) or not response.get("acknowledged") or response.get("errors")
        ):
            raise RuntimeError(f"Elasticsearch alias update was not fully acknowledged: {response!r}")
    after = _inventory(client, names, alias_profile=alias_profile)
    verified = (
        _cluster_uuid(client) == cluster_uuid
        and _alias_state(after, alias_profile) == expected
        and all(
            _uuid(after, name) == uuid and after[name]["aliases"] == expected_index_aliases[name]
            for name, uuid in identities.items()
        )
    )
    if not verified:
        raise RuntimeError(
            "Alias update did not reach the expected state; retry the same explicit index names"
        ) from error
    return {
        "applied": bool(actions),
        "verified": True,
        "expected_aliases": expected,
        "recovered_after_error": error is not None,
    }


def stage_indices(client, index_names, *, sources=None, expected_staging=None, dry_run=False, alias_profile="default"):
    """Replace staging per selected datasource; dry runs permit unrestored targets."""
    if not isinstance(dry_run, bool):
        raise TypeError("dry_run must be a boolean")
    profile = _profile(alias_profile)
    names = validate_index_names(index_names)
    sources = _sources(sources)
    selected = _selected(names, sources)
    cluster_uuid = _cluster_uuid(client)
    with cluster_lock(cluster_uuid):
        inventory = _inventory(client, names, alias_profile=alias_profile)
        groups = _holders(inventory, sources, alias_profile)
        _validate_holders(groups, selected)
        current = _staging_state(cluster_uuid, selected, groups, inventory, alias_profile)
        if expected_staging is not None and current != expected_staging:
            raise RuntimeError("Staging changed while indices were restored; refusing to replace newer staging")
        missing = [name for name in names if name not in inventory]
        if missing and not dry_run:
            raise RuntimeError(f"Concrete staging indices are missing: {missing!r}")
        _require_active_primaries(client, set(names) - set(missing))
        actions = []
        affected = set(names) - set(missing)
        for source, name in selected.items():
            info = inventory.get(name, {"aliases": set()})
            if info["aliases"] & {profile["deprecated_alias"], profile["delete_alias"]}:
                raise RuntimeError(f"Incoming index {name!r} has deprecated/delete state")
            if name in inventory:
                _uuid(inventory, name)
            # Re-staging an index already live does not displace another candidate.
            if name in groups[source]["prod"]:
                continue
            policy = alias_policy(source, alias_profile=alias_profile)
            for previous in groups[source]["staging"]:
                if previous != name:
                    affected.add(previous)
                    actions.append(_action("remove", previous, policy["staging_alias"]))
                    if not inventory[previous]["aliases"] & _production_aliases():
                        actions.append(_action("add", previous, profile["delete_alias"]))
            if name not in groups[source]["staging"]:
                actions.append(_action("add", name, policy["staging_alias"]))
        result = _apply(client, cluster_uuid, inventory, affected, actions, dry_run, alias_profile)
        return {
            "cluster_uuid": cluster_uuid,
            "alias_profile": alias_profile,
            "dry_run": dry_run,
            "indices": names,
            "sources": current["sources"],
            "actions": actions,
            "missing_indices": missing,
            "staging_state": current,
            **result,
        }


def release_indices(client, index_names=None, *, sources=None, dry_run=False, alias_profile="default"):
    """Promote selected staging holders, preserving every unselected datasource."""
    if not isinstance(dry_run, bool):
        raise TypeError("dry_run must be a boolean")
    profile = _profile(alias_profile)
    sources = _sources(sources)
    names = None if index_names is None else validate_index_names(index_names)
    cluster_uuid = _cluster_uuid(client)
    with cluster_lock(cluster_uuid):
        inventory = _inventory(client, names or (), alias_profile=alias_profile)
        groups = _holders(inventory, sources, alias_profile)
        if names is None:
            names = sorted(name for roles in groups.values() for name in roles["staging"])
        selected = _selected(names, sources)
        _validate_holders(groups, selected)
        for name in names:
            _uuid(inventory, name)
        _require_active_primaries(client, names)
        actions = []
        affected = set(names)
        already_live = []
        for source, name in selected.items():
            _uuid(inventory, name)
            if name in groups[source]["prod"] and name not in groups[source]["staging"]:
                already_live.append(name)
                continue
            if name not in groups[source]["staging"]:
                raise RuntimeError(f"Index {name!r} is not the current staging holder")
            policy = alias_policy(source, alias_profile=alias_profile)
            for previous in groups[source]["prod"]:
                if previous != name:
                    affected.add(previous)
                    actions.extend(
                        [
                            _action("remove", previous, policy["production_alias"]),
                            _action("add", previous, profile["deprecated_alias"]),
                        ]
                    )
            if name not in groups[source]["prod"]:
                actions.append(_action("add", name, policy["production_alias"]))
            actions.append(_action("remove", name, policy["staging_alias"]))
        result = _apply(client, cluster_uuid, inventory, affected, actions, dry_run, alias_profile)
        return {
            "cluster_uuid": cluster_uuid,
            "alias_profile": alias_profile,
            "dry_run": dry_run,
            "indices": names,
            "sources": {name: source for source, name in selected.items()},
            "actions": actions,
            "already_live": already_live,
            **result,
        }


def _cleanup_targets(inventory, index_names, alias, alias_profile):
    profile = _profile(alias_profile)
    lifecycle = {profile["deprecated_alias"], profile["delete_alias"]}
    names = (
        sorted(name for name, info in inventory.items() if alias in info["aliases"])
        if index_names is None
        else validate_index_names(index_names)
    )
    for name in names:
        _uuid(inventory, name)
        protected = inventory[name]["aliases"] - lifecycle
        if protected:
            raise RuntimeError(f"Index {name!r} has protected aliases: {sorted(protected)!r}")
    return names


def mark_for_deletion(client, index_names=None, *, alias_profile="default", dry_run=True):
    """Tag deprecated indices for deletion; leave physical removal to a separate call."""
    if not isinstance(dry_run, bool):
        raise TypeError("dry_run must be a boolean")
    profile = _profile(alias_profile)
    names = None if index_names is None else validate_index_names(index_names)
    cluster_uuid = _cluster_uuid(client)
    with cluster_lock(cluster_uuid):
        inventory = _inventory(client, names or (), alias_profile=alias_profile)
        names = _cleanup_targets(inventory, names, profile["deprecated_alias"], alias_profile)
        actions = []
        already_marked = []
        for name in names:
            flags = inventory[name]["aliases"]
            if profile["delete_alias"] in flags:
                already_marked.append(name)
            elif profile["deprecated_alias"] in flags:
                actions.append(_action("add", name, profile["delete_alias"]))
            else:
                raise RuntimeError(f"Index {name!r} is not deprecated in alias profile {alias_profile!r}")
        result = _apply(client, cluster_uuid, inventory, names, actions, dry_run, alias_profile)
        return {
            "cluster_uuid": cluster_uuid,
            "alias_profile": alias_profile,
            "dry_run": dry_run,
            "indices": names,
            "index_uuids": {name: _uuid(inventory, name) for name in names},
            "actions": actions,
            "already_marked": already_marked,
            **result,
        }


def delete_indices(client, index_names=None, *, alias_profile="default", dry_run=True, expected_index_uuids=None):
    """Delete only lifecycle-tagged indices with no application or serving aliases."""
    if not isinstance(dry_run, bool):
        raise TypeError("dry_run must be a boolean")
    profile = _profile(alias_profile)
    names = None if index_names is None else validate_index_names(index_names)
    cluster_uuid = _cluster_uuid(client)
    with cluster_lock(cluster_uuid):
        inventory = _inventory(client, names or (), alias_profile=alias_profile)
        names = _cleanup_targets(inventory, names, profile["delete_alias"], alias_profile)
        for name in names:
            if profile["delete_alias"] not in inventory[name]["aliases"]:
                raise RuntimeError(f"Index {name!r} is not marked for deletion in alias profile {alias_profile!r}")
        identities = {name: _uuid(inventory, name) for name in names}
        if expected_index_uuids is not None and (
            not isinstance(expected_index_uuids, Mapping) or dict(expected_index_uuids) != identities
        ):
            raise RuntimeError("Deletion selection or index UUIDs changed from the reviewed preview")
        result = {
            "cluster_uuid": cluster_uuid,
            "alias_profile": alias_profile,
            "dry_run": dry_run,
            "indices": names,
            "index_uuids": identities,
            "applied": False,
            "verified": False,
            "deleted": [],
        }
        if dry_run:
            return result
        if _cluster_uuid(client) != cluster_uuid:
            raise RuntimeError("Elasticsearch cluster identity changed before deletion")
        before = _inventory(client, names, alias_profile=alias_profile)
        _cleanup_targets(before, names, profile["delete_alias"], alias_profile)
        if any(before[name] != inventory[name] for name in names):
            raise RuntimeError("Alias or index identity drifted before deletion")
        if _cluster_uuid(client) != cluster_uuid:
            raise RuntimeError("Elasticsearch cluster identity changed before deletion")
        error = None
        if names:
            try:
                response = _body(
                    client.indices.delete(
                        index=names, expand_wildcards="none", allow_no_indices=False, ignore_unavailable=False
                    )
                )
                if not isinstance(response, dict) or not response.get("acknowledged") or response.get("errors"):
                    error = RuntimeError(f"Elasticsearch deletion was not fully acknowledged: {response!r}")
            except Exception as exc:
                error = exc
        after = _inventory(client, names, alias_profile=alias_profile)
        if _cluster_uuid(client) != cluster_uuid or any(name in after for name in names):
            raise RuntimeError(
                "Deletion did not reach the expected state; inspect exact indices before retrying"
            ) from error
        return {
            **result,
            "applied": bool(names),
            "verified": True,
            "deleted": names,
            "recovered_after_error": error is not None,
        }

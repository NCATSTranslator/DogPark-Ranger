#!/usr/bin/env python3
"""Stage and release explicit indices using the selected Hub indexer environment."""

from __future__ import annotations

from contextlib import contextmanager as _dogpark_release_contextmanager
from elasticsearch import Elasticsearch as _DogParkReleaseElasticsearch
from hooks.dogpark_hook_utils import aliases as _dogpark_aliases

__all__ = ["assign_staging_tag", "assign_staging_tags", "release_prod", "mark_for_deletion", "delete_indices"]


@_dogpark_release_contextmanager
def _dogpark_release_client(indexer_env):
    if not isinstance(indexer_env, str) or not indexer_env:
        raise ValueError("indexer_env must name a configured Hub indexer environment")
    manager = globals().get("im")
    if manager is None:
        manager = getattr(globals().get("ssm"), "index_manager", None)
    if manager is None:
        raise RuntimeError("The Hub index manager is not available")
    try:
        args = dict(manager[indexer_env]["args"])
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Unknown or invalid indexer environment: {indexer_env!r}") from exc
    client = _DogParkReleaseElasticsearch(**args)
    try:
        yield client
    finally:
        client.close()


def assign_staging_tag(
    indexer_env: str, index_name: str, *, dry_run: bool = False,
    source: str | None = None, alias_profile: str = "default",
):
    """Stage one concrete index; replace only its datasource's staged holder."""
    sources = None if source is None else {index_name: source}
    return assign_staging_tags(
        indexer_env, [index_name], dry_run=dry_run, sources=sources, alias_profile=alias_profile,
    )


def assign_staging_tags(
    indexer_env: str, index_names: list[str], *, dry_run: bool = False,
    sources: dict | None = None, alias_profile: str = "default",
):
    """Stage an explicit batch. Custom names require an index-to-source mapping."""
    with _dogpark_release_client(indexer_env) as client:
        result = _dogpark_aliases.stage_indices(
            client, index_names, sources=sources, dry_run=dry_run, alias_profile=alias_profile,
        )
    return {"indexer_env": indexer_env, **result}


def release_prod(
    indexer_env: str, index_names: list[str] | None = None, *, dry_run: bool = False,
    sources: dict | None = None, alias_profile: str = "default",
):
    """Promote all currently staged indices, or an explicit subset, on one cluster."""
    with _dogpark_release_client(indexer_env) as client:
        result = _dogpark_aliases.release_indices(
            client, index_names, sources=sources, dry_run=dry_run, alias_profile=alias_profile,
        )
    return {"indexer_env": indexer_env, **result}


def mark_for_deletion(
    indexer_env: str, index_names: list[str] | None = None, *, dry_run: bool = True,
    alias_profile: str = "default",
):
    """Preview or mark deprecated indices for deletion; never delete them."""
    with _dogpark_release_client(indexer_env) as client:
        result = _dogpark_aliases.mark_for_deletion(
            client, index_names, dry_run=dry_run, alias_profile=alias_profile,
        )
    return {"indexer_env": indexer_env, **result}


def delete_indices(
    indexer_env: str, index_names: list[str] | None = None, *, dry_run: bool = True,
    alias_profile: str = "default", expected_index_uuids: dict | None = None,
):
    """Preview or delete only deletion-tagged indices with no serving aliases."""
    with _dogpark_release_client(indexer_env) as client:
        result = _dogpark_aliases.delete_indices(
            client, index_names, dry_run=dry_run, alias_profile=alias_profile,
            expected_index_uuids=expected_index_uuids,
        )
    return {"indexer_env": indexer_env, **result}


assign_staging_tag.confirm = "dry_run"
assign_staging_tags.confirm = "dry_run"
release_prod.confirm = "dry_run"
mark_for_deletion.confirm = "dry_run"
delete_indices.confirm = "dry_run"

# BioThings 1.3 registers __all__; older Hubs need explicit registration.
if "hub" in globals() and not hasattr(getattr(hub, "server", None), "terminal"):
    for _dogpark_release_command in __all__:
        hub.commands[_dogpark_release_command] = globals()[_dogpark_release_command]
        hub.hidden[_dogpark_release_command] = False

# Hub hooks

Run commands against the Hub with `dogpark run COMMAND [ARGS]`.

| Hook | Commands |
| --- | --- |
| `dogpark_release.py` | Assign staging aliases, promote indices, and manage deletion tags/removal. |
| `dogpark_snapshots.py` | Create, inspect, and install snapshots. See [snapshot usage](snapshots-native.md). |
| `dogpark_dev.py` | Reload snapshot hooks after uploading edits. |

## Releases

Snapshot installation restores indices and assigns staging aliases by default.
There is no separate release-staging step or frozen release manifest. Elasticsearch
aliases hold the current state independently on each cluster.

Use a configured Hub **indexer environment** for tagging and production release.
For example, `ci_snapshots` is a snapshot environment that restores into the
`itrb-ci` indexer environment; the names are not interchangeable.

```bash
dogpark run install_snapshots ci_snapshots --index-names '["alliance_20261105_abcd1234"]' --dry-run
dogpark run install_snapshots ci_snapshots --index-names '["alliance_20261105_abcd1234"]'

# Validate staging, then preview and release all currently staged datasets.
dogpark run release_prod itrb-ci --dry-run
dogpark run release_prod itrb-ci
```

For a one-off release, pass exact concrete index names with `--index-names`.
Without that option, release promotes all currently staged indices under the
managed staging aliases. A dry run previews current state; it does not freeze it.
Use the reviewed exact names on execution when the selection must stay fixed.

| Dataset | Staging alias | Production alias |
| --- | --- | --- |
| Normal edge datasets | `dingo_staging` | `dingo` |
| `ubergraph_nodes` | `ubergraph_nodes_staging` | `ubergraph_nodes` |
| `ubergraph_nodes_info` | `ubergraph_nodes_mapping_staging` | `ubergraph_nodes_mapping` |

Production release derives each production alias by removing `_staging`. It
replaces production holders only for the selected datasets, removes their
staging aliases, and marks displaced production indices `dingo_deprecated`.
Other production and staging holders remain in place. It does not age deprecated
indices into deletion or physically delete indices.

The same staging helper handles installation and manual tagging. To stage an
already installed index, including recovery after restoration succeeded but
alias assignment failed:

```bash
dogpark run assign_staging_tag itrb-ci alliance_20261105_abcd1234 --dry-run
dogpark run assign_staging_tag itrb-ci alliance_20261105_abcd1234
```

`assign_staging_tags INDEXER_ENV --index-names '["INDEX_NAME", "OTHER_INDEX"]'`
handles an explicit batch. Staging replaces the prior staged holder of each
selected dataset and marks it `dingo_to_be_deleted`, unless it still serves production.
An already-live index is a no-op. Deprecated or deletion-tagged incoming indices
are rejected.

Standard names use `DATASET_YYYYMMDD_ID`, with a valid date and lowercase
alphanumeric ID. Legacy names may insert `ci`, `test`, `prod`, or `dev` between
the date and ID, for example `alliance_20261015_ci_9e0eed3c`.
Exact dataset names are also recognized unless they collide
with a managed alias, such as `ubergraph_nodes`. Custom names require
`--sources '{"custom-index":"alliance"}'` for batch tagging or release, or
`--source alliance` for single-index tagging. Include mappings for custom active
holders too. Source names are checked against the managed dataset list; release
does not consult build history or infer the newest build.

Aliases and index identities are checked before and after each update. Hook alias
mutations share a short per-cluster lock within the Hub process. A restore whose
selected staging state changed while it ran remains installed and reports a
conflict instead of replacing that newer staging selection. Retry with the same
explicit names after reviewing the live state. Dedicated auxiliary staging aliases
are separate from Retriever's currently configured live node and mapping lookups.

The old `plan_staging_tags`, `plan_release_manifest`, `freeze_release_manifest`,
`get_release_manifest`, `assign_staging_manifest`, `plan_prod_release`, and
`release_prod_manifest` commands are removed. Use `--dry-run` on tagging or
`release_prod` for previews. Existing manifest files are no longer release inputs.

## Cleanup

Lifecycle tags have separate purposes:

| Event | Result |
| --- | --- |
| A staging index is replaced before release | The old staging index receives `dingo_to_be_deleted`, unless it still serves production. |
| A production index is replaced | The old production index receives `dingo_deprecated`. |
| `mark_for_deletion` selects a deprecated index | It also receives `dingo_to_be_deleted`. |
| `delete_indices` selects a deletion-tagged index | Its concrete Elasticsearch index is physically removed. |

Both cleanup commands default to dry runs. They never run as part of installation
or production release. There is no automatic release-count or time-based aging.

```bash
# Review deprecated indices, then explicitly mark the chosen names.
dogpark run mark_for_deletion itrb-ci --index-names '["OLD_INDEX"]'
dogpark run mark_for_deletion itrb-ci --index-names '["OLD_INDEX"]' --no-dry-run

# Review deletion candidates and their index_uuids.
dogpark run delete_indices itrb-ci --index-names '["OLD_INDEX"]'
```

To execute deletion, pass `--no-dry-run` and preferably the preview's
`--expected-index-uuids '{"OLD_INDEX":"REVIEWED_UUID"}'`. Only exact concrete
indices tagged for deletion in the selected profile are eligible. Any serving,
staging, foreign-profile, or other application alias blocks cleanup. The whole
selection is checked before mutation, with cluster/index identity checks and
post-operation verification. Omitting `--index-names` selects the current
profile's deprecated or deletion-tagged indices, respectively.

## Isolated CI tests

All alias-changing commands, including installation, accept
`--alias-profile testing`. The default profile retains the production names
shown above. The testing profile uses `prod_testing_staging` → `prod_testing`,
separate auxiliary staging/production pairs, and `prod_testing_deprecated` /
`prod_testing_to_be_deleted`. Selection and readback stay within that profile.

```bash
dogpark run assign_staging_tags itrb-ci --index-names '["INDEX_NAME"]' --alias-profile testing --dry-run
dogpark run release_prod itrb-ci --alias-profile testing --dry-run
```

Use existing recovered, untagged CI indices to test manual tagging, then the
isolated release aliases. Snapshot creation has already been validated onsite;
the remaining snapshot test needs only one installation of an existing ad-hoc
snapshot. Installation/staging tests can run on the test cluster described in
[snapshot configuration](snapshots-native.md#test-cluster-configuration).
Production-release tests on that cluster are excluded. A full workflow on the
dev cluster can be scheduled later. No live test or reload is performed by these
configuration changes.

## Load and update

Deploy the hook files, `hooks/__init__.py`, and `hooks/dogpark_hook_utils/` under
the Hub project root. Configure `HOOKS_FOLDER` to load this directory and keep
the project root on Python's import path. Initial loading requires a Hub restart.

For a snapshot-hook update on su06:

```bash
scp hooks/dogpark_snapshots.py su06e:/opt/home/dogpark/DogPark-Ranger/hooks/
dogpark run reload_snapshot_hooks
```

Upload any changed shared helpers to `hooks/dogpark_hook_utils/` too. Reload reads
the server's snapshot hook and build/alias helpers; it does not copy files or reload
Hub configuration. Let active hook jobs finish before reloading. Release-hook
and reload-command changes require a Hub restart for this migration. Restarting
also removes the old manifest commands from the command catalog; the native
hook loader does not unregister commands removed from a hook's exports.

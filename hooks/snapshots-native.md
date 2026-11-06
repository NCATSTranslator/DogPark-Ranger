# Snapshot hooks

`snapshot_env` selects an Elasticsearch cluster and repository through the Hub's
`SNAPSHOT_CONFIG`. It is an environment name, not a repository name.

| Environment | Cluster | Use |
| --- | --- | --- |
| `s3_dogpark` | `su12` | Create and inspect snapshots. |
| `ci_snapshots` | `itrb-ci` | Inspect and install snapshots on CI. |
| `test_snapshots` | `itrb-test` | Inspect, install, and stage snapshots on test after applying the configuration overlay below. |

These environments expose the shared `dogpark_repository`. Configure the target
repository as read-only. Inspection and installation require repository settings
that resolve without build metadata. See [hook loading](README.md#load-and-update).

## Default selection

| Command | With no `index_names` |
| --- | --- |
| `create_snapshots` | Select the newest build per managed dataset, if ready in the source indexer environment. |
| `get_snapshots_release` | List all compatible completed snapshots in the repository, including older builds. No Hub build lookup. |
| `install_snapshots` | Look for snapshots of the newest managed builds in the target repository. Skip missing or incomplete snapshots. |

Automatic creation and installation do not fall back to older builds.

## Create, inspect, install

Review the dry run before submitting each batch. Finish creation before starting installation.

```bash
dogpark run create_snapshots s3_dogpark --dry-run
dogpark run --no-wait create_snapshots s3_dogpark
dogpark status JOB_ID

dogpark run get_snapshots_release ci_snapshots

dogpark run install_snapshots ci_snapshots --dry-run
dogpark run --no-wait install_snapshots ci_snapshots
dogpark status JOB_ID
```

Put `--no-wait` before the hook name. After the Hub returns a job ID, the CLI can
exit while the job continues. Keep the Hub running; jobs do not resume after a
Hub restart. Installation jobs finish after recovery and, by default, staging verification.
A batch is staged only after every requested restore succeeds.

Installation returns one tracked batch task. Its result contains per-index
`indices`, the `staged` flag, and the verified `staging` alias result. Cancelling
the task stops monitoring and prevents pending staging; an already submitted
Elasticsearch restore may continue on the cluster.

## Explicit indices

Pass a JSON list, even for one index. Replace `INDEX_NAME` with the concrete name:

```bash
dogpark run create_snapshots s3_dogpark --index-names '["INDEX_NAME"]' --dry-run
dogpark run get_snapshots_release ci_snapshots --index-names '["INDEX_NAME"]'
dogpark run install_snapshots ci_snapshots --index-names '["INDEX_NAME"]' --dry-run
```

Remove `--dry-run` to execute creation or installation. Use the same explicit list
to keep a batch fixed between preview and execution. An empty list does nothing.
Wildcards and duplicate names are rejected. Installation accepts names directly;
inspection output is optional.

## Behavior

- **Create:** requires Hub index registration in the selected source environment,
  even for explicit names. Missing source indices are skipped. An existing
  same-name snapshot is replaced; `replace` in the preview refers to the snapshot.
- **Inspect:** returns `index_name`, `repository`, `snapshot`, and `snapshot_uuid`,
  sorted by name. Includes only `SUCCESS` snapshots with zero failed shards and
  exactly one index named after the snapshot. Missing or incompatible snapshots
  are skipped. It does not check destination conflicts or ES version compatibility.
- **Install:** an explicit list requires every snapshot to be complete. Existing
  destination indices or aliases block the batch. Restores snapshot settings
  without aliases or global cluster state. Elasticsearch enforces version compatibility.
  After every index has recovered its primary shards, installation assigns the
  staging aliases described in [release usage](README.md#releases) in one update.
  Replaced staging indices receive `to_be_deleted` in the selected alias profile,
  unless still live. Failed batches leave
  existing staging unchanged, although successful restores remain installed.
  Recovery monitoring has a deadline; a timeout does not delete the restored index.

Use `--alias-profile testing` for isolated CI aliases, or `--no-stage` to restore
without changing release aliases. `--sources`
accepts an index-to-dataset mapping for custom names when staging is enabled.
One-off and batch installs follow the same path. Auxiliary node and mapping
builds are included in automatic snapshot discovery.

Installation captures snapshot UUIDs during preflight and checks them again
before submission. It still restores by name; do not replace snapshots during
installation. After an uncertain submission, inspect the target before retrying;
Elasticsearch may already be restoring the index. If restoration completed but
staging failed, use `assign_staging_tag` or `assign_staging_tags` on the verified
installed indices instead of restoring them again.

## Test cluster configuration

In the deployment's `config.py`, after the existing CI snapshot configuration:

```python
from hub.snapshot_config import add_test_snapshots

INDEX_CONFIG, SNAPSHOT_CONFIG = add_test_snapshots(INDEX_CONFIG, SNAPSHOT_CONFIG)
```

Deploy `hub/snapshot_config.py` with the hooks. This overlay adds an `itrb-test`
indexer environment at `http://tier1-dogpark.test.transltr.io:9200` and a
`test_snapshots` snapshot environment. It reuses `ci_snapshots` repository/cloud
settings, forces the repository read-only, and replaces the complete copied
indexer reference so CI hosts cannot leak into the test connection. Existing
explicit test indexer settings, including auth/TLS, are preserved. An existing
test endpoint equal to CI is rejected. CI settings must exist before applying
the overlay; this repository does not contain their deployed storage credentials.

The test cluster must already expose the same snapshot repository registered
read-only. The overlay and installer do not register it. Configuration changes
require a Hub restart. Once connectivity is available, inspect and preview a
single installation:

```bash
dogpark run get_snapshots_release test_snapshots --index-names '["INDEX_NAME"]'
dogpark run install_snapshots test_snapshots --index-names '["INDEX_NAME"]' --dry-run
```

Live test execution remains deferred. Test-cluster validation covers restoration
and staging only; it does not call `release_prod`. The dev cluster can support a
separate full release workflow later.

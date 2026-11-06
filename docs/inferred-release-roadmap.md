# Inferred Release Roadmap

> Historical design. The hooks now use alias-based staging and production release.
> See [current release usage](../hooks/README.md#releases). The manifest commands
> and readiness rules below describe the superseded implementation.

## Motivation

DogPark plugins now schedule dumps, uploads, builds, indexing, and snapshots.
Those builds no longer form a reliable date-based batch: a datasource can be
built more than once in a day, while different datasources can finish on
different days. Release membership must therefore come from build metadata and
current Elasticsearch state rather than a date embedded in an index name.

The release manifest is the durable batch identity. Discovery may be
continuous, but staging and production operate on a frozen selection.

## Invariants

- Limit discovery to the explicit release-datasource allowlist.
- Select exactly one newest build per build configuration using `started_at`.
- Select the newest record before validation; never fall back to an older
  non-production build when the newest record is not ready.
- Infer Elasticsearch names from completed index registrations, not from the
  Mongo target collection name.
- For a newly unaliased candidate, require a successful final hub job and an
  exact concrete index on Elasticsearch. Prefer a registration for the target
  indexer environment; accept a legacy registration with no environment only
  when no exact-environment registration exists. Narrowly tolerate a legacy
  stale `cancelled` no-op `post-index` record only when it immediately follows
  a successful main index, shares the same non-empty logfile for the exact
  resolved index, has no recorded error, and is surfaced as a readiness warning.
- Validate the Elasticsearch cluster identity before reading or changing alias
  state.
- Preserve special-dataset staging and production alias policy.
- Keep planning read-only, make application idempotent, and explain every skip
  or rejection.
- Once manifests are introduced, never rediscover builds between approving a
  manifest and applying it.

## Phase 1: Infer staging candidates — implemented diagnostic

Implemented initially in `hub-hooks/dogpark_release.py`:

1. Read a narrow projection of `_id`, `target_name`, `build_config`,
   `started_at`, `archived`, `jobs`, and `index` directly from the BioThings
   `src_build` collection.
2. Independently select the newest record for each configured release
   datasource by `started_at`.
3. Resolve the actual Elasticsearch name from the selected build's `index`
   registrations, preferring the target indexer environment and retaining the
   legacy missing-environment fallback described above. Reject ambiguity and
   verify the exact concrete index exists.
4. Build index-to-datasource ownership from hub history, with the standard
   timestamped-name parser only as a fallback for old records.
5. Inspect current `dingo`, `dingo_staging`, `dingo_deprecated`, and
   `dingo_to_be_deleted` alias holders.
6. Classify each newest candidate:
   - already production: unchanged;
   - already staging: unchanged;
   - special dataset with `staging_tag: null`: skipped by policy;
   - archived, incomplete, missing, or unorderable build metadata: pending;
   - stale no-error `cancelled` post-index immediately after a successful main
     index: eligible with an explicit warning;
   - absent from Elasticsearch: pending;
   - deprecated, deletion-tagged, conflicting, or ambiguously registered:
     rejected;
   - otherwise: eligible for staging.
7. Return the full read-only plan from `plan_staging_tags`; manifest-backed
   commands in Phase 3 perform the inferred alias mutation.
8. Provide `assign_staging_tag INDEX_NAME` as a confirmation-gated operator
   override for one exact concrete index. It bypasses inferred selection and
   readiness, but retains cluster, existence, alias-conflict, known ownership,
   and special-dataset policy checks.

This phase does not remove or replace a different staging holder. It reports
that condition as a conflict so an automatic run cannot silently discard an
unpromoted candidate.

### Why the hook does not call `list_mongo_builds`

`list_mongo_builds` remains the best operator-facing diagnostic command and is
useful for checking what the hub sees. In BioThings 1.2.x it is itself a
projected `src_build` query, but it intentionally omits `jobs` and `index`.
Calling `build(id)` afterward for every selected datasource would invoke
`BuildManager.build_info()`, which may also calculate document counts against
the target backend. A single projected `src_build` read supplies exactly the
metadata this planner needs without those extra side effects.

`lsmerge` is not suitable for release selection because it omits archived
builds and sorts build identifiers rather than selecting by `started_at`.

## Phase 2: Freeze an immutable manifest — implemented

Build a complete desired-state manifest for every configured datasource:

- use the newest ready candidate; or
- retain its current production index when the newest build is pending or
  rejected.

Persist a content-identified JSON manifest before any alias mutation. It records
the build configuration, build ID, target and concrete index names, `started_at`,
hub status, index registration environment, index UUID, target cluster UUID,
observed aliases, decision, reason, and special-dataset alias policy.

Store runtime artifacts outside the hook-loader directory beneath the hub
process working directory, or beneath `DOGPARK_RELEASE_RECORDS_DIR` when that
environment override is set:

```text
release-records/manifests/live/
```

The hook writes immutable files without replacement and advances
`latest-frozen.json` atomically only after the manifest is durable. The manifest
ID is derived from its timestamp and canonical content digest. The immutable
file—not fresh discovery—is the input to later staging and production
operations.

Implemented commands:

```text
plan_release_manifest
freeze_release_manifest
get_release_manifest [ID|latest-frozen|latest-staged|latest-prod]
```

After production promotion, `get_release_manifest latest-prod` loads the same
manifest through its verified production receipt.

## Phase 3: Reconcile staging — implemented

Apply a reviewed, frozen manifest without discovering builds again:

- stage `desired - current_prod`;
- preserve `desired ∩ current_prod`;
- atomically remove or replace stale staging holders per datasource;
- abort unless alias state is exactly the frozen pre-state or the already-
  applied expected post-state, and abort if cluster identity changed.

This phase turns the conservative Phase 1 staging-holder conflict into an
explicit desired-state replacement. Additions and removals are sent in one
Elasticsearch alias transaction. Application also verifies every concrete
index UUID, reads the resulting aliases back exactly, writes an immutable
receipt, and only then advances `latest-staged.json`. A retry accepts either the
frozen pre-state or the already-applied post-state; any other state is drift.

Implemented commands:

```text
assign_staging_manifest MANIFEST_ID
assign_staging_tags  # freeze a new manifest, then apply that exact ID
```

The explicit single-index override remains outside this lifecycle and never
changes a manifest pointer.

## Phase 4: Reconcile production — implemented

Production is a set reconciliation against the same frozen manifest:

```text
promote   = desired - current_prod
keep      = desired ∩ current_prod
deprecate = current_prod - desired
```

Normal `promote` entries must carry `dingo_staging`; `keep` entries remain in
place. The two no-staging datasets use their custom production aliases instead:

- `ubergraph_nodes_info` uses `ubergraph_nodes_mapping`;
- `ubergraph_nodes` uses `ubergraph_nodes`.

The planner rejects a desired index found under the wrong production alias and
rejects production holders that the staged manifest cannot attribute to one
datasource. This keeps a cross-policy or unowned holder from being silently
unserved. It deprecates only attributable production holders displaced from
the complete desired set. All production-alias additions and removals,
staging-tag removals, deprecation tags, and deletion tags are sent in one
Elasticsearch `_aliases` transaction.

Implemented commands:

```text
plan_prod_release [MANIFEST_ID]
release_prod_manifest [MANIFEST_ID]
```

Both default to `latest-staged`. An explicit manifest ID is accepted only when
it is the manifest currently referenced by `latest-staged`; an arbitrary
frozen or previously staged manifest cannot be promoted. Loading the staged
input validates the manifest and staging-receipt checksums and content IDs,
their linkage, target identity, and verified staging state. The hook then
rechecks the live cluster, every affected concrete-index UUID, and all managed
alias holders immediately before mutation. `plan_prod_release` is read-only;
`release_prod_manifest` is confirmation-gated by the hub terminal.

The transition is anchored to the staging receipt's verified alias snapshot.
A first application accepts only that exact pre-state. A retry accepts the
exact expected post-state and writes an idempotent production receipt without
issuing another alias request. Any third state is drift and aborts before
mutation. After an exact post-mutation readback, the hook writes an immutable
`dogpark.production-receipt/v1` record and atomically advances
`latest-prod.json`. The receipt includes the staging-receipt link, complete
pre- and post-state, planned and applied actions, transition digest, affected
index UUIDs, and the promoted, retained, displaced, deprecated,
staging-cleared, and deletion-tagged sets.

By default the hook preserves the legacy release aging behavior: indices that
already carried `dingo_deprecated` before this promotion also receive
`dingo_to_be_deleted`, while indices newly displaced by this promotion receive
only `dingo_deprecated`. This is alias tagging only. The hook never physically
deletes an Elasticsearch index.

Rollback remains a future, separate command. The production receipt records
the exact pre- and post-state needed for a safe rollback, but production
promotion does not automatically reverse itself.

This supersedes the old `releaseProd()` input model, which expected every input
name to be staging and removed the production alias from every current holder.
The hook instead promotes a complete, receipt-backed desired state and preserves
unchanged production holders.

## Repository and runtime layout

```text
hub-hooks/
  dogpark_release.py
  README.md
docs/
  inferred-release-roadmap.md
release-records/
  manifests/
    live/
```

The release datasource and special-dataset policies are currently duplicated
between TypeScript and the standalone hook. A later cleanup should put that
policy in one shared configuration source or hub build-config metadata.
Stored manifests are intentionally checked against the hook's current policy;
a policy change therefore requires an explicit migration or a newly frozen
manifest before promotion.

## Test plan

- multiple builds for one datasource on the same day;
- input records returned out of order and timestamp ties;
- archived or in-progress newest builds with an older successful build present;
- missing build configurations and malformed build metadata;
- absent Elasticsearch indices and failed index registration;
- already-production and already-staging candidates;
- deprecated/delete-tagged candidates and multiple alias holders;
- special datasets with no staging tag;
- empty idempotent runs and exact bulk alias payloads;
- immutable manifest writes and `latest` pointer atomicity;
- production reconciliation preserving unchanged datasource holders;
- production refusal for an unreceipted or non-current staged manifest;
- special production aliases with no staging requirement;
- one atomic production alias payload and exact post-state readback;
- idempotent production retry, alias drift rejection, and UUID drift rejection;
- immutable production receipts and `latest-prod` pointer atomicity;
- legacy deprecated-to-delete aging without physical index deletion.

## Rollout criteria

1. Run `plan_staging_tags` repeatedly and review decisions without mutation.
2. Confirm the selected candidates match manual hub and Elasticsearch checks.
3. Run `plan_release_manifest` and review every promotion, fallback, special
   alias, and stale-staging removal.
4. Freeze and stage one controlled release, then verify both pointers and the
   receipt can be loaded with `get_release_manifest latest-staged`.
5. Repeat the same `assign_staging_manifest` ID and confirm it records an
   idempotent already-applied receipt without rediscovery.
6. Run `plan_prod_release` and review the complete pre-state, post-state, alias
   actions, deprecation set, and legacy deletion-tag aging.
7. Run `release_prod_manifest` for that current staged manifest, verify
   `latest-prod`, then repeat the command and confirm the retry performs no
   second Elasticsearch alias request.
8. Add a separate receipt-backed rollback command before treating automatic
   reversal as an operational capability.

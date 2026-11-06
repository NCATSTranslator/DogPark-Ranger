"""Configuration overlay for restoring shared snapshots on the test cluster."""

from copy import deepcopy


TEST_ES_URL = "http://tier1-dogpark.test.transltr.io:9200"


def add_test_snapshots(
    index_config,
    snapshot_config,
    *,
    host=TEST_ES_URL,
    indexer_env="itrb-test",
    snapshot_env="test_snapshots",
    template_env="ci_snapshots",
):
    """Copy CI repository settings and bind a separate test indexer environment.

    Call after the deployment config defines its CI snapshot environment. Inputs
    are not mutated. Existing test indexer settings, including auth/TLS, are kept.
    """
    if not isinstance(host, str) or not host.startswith(("http://", "https://")):
        raise ValueError("host must be the test Elasticsearch HTTP(S) URL")
    if not all(isinstance(name, str) and name for name in (indexer_env, snapshot_env, template_env)):
        raise ValueError("Environment names must be non-empty strings")
    if snapshot_env == template_env:
        raise ValueError("Test and template snapshot environments must be different")
    indices = deepcopy(index_config)
    snapshots = deepcopy(snapshot_config)
    try:
        environment = deepcopy(snapshots["env"][template_env])
        repository = environment["repository"]
        settings = repository.setdefault("settings", {})
        if not isinstance(settings, dict):
            raise TypeError("repository settings must be a dictionary")
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"A configured {template_env!r} snapshot repository is required") from exc
    if snapshot_env in snapshots["env"]:
        raise ValueError(f"Snapshot environment {snapshot_env!r} already exists")
    template_indexer = environment.get("indexer")
    template_indexer = template_indexer.get("name", template_indexer.get("env")) if isinstance(template_indexer, dict) else template_indexer
    if indexer_env == template_indexer:
        raise ValueError("Test and template indexer environments must be different")
    indices.setdefault("env", {}).setdefault(indexer_env, {
        "host": host,
        "indexer": {"args": {"request_timeout": 300, "retry_on_timeout": False, "max_retries": 0}},
        "index": [],
    })
    # Native SnapshotManager expands indexer into a dict containing client args.
    # Replace the whole value so copied CI hosts cannot leak into the test target.
    environment["indexer"] = indexer_env
    environment.pop("name", None)
    settings["readonly"] = True
    snapshots["env"][snapshot_env] = environment
    return indices, snapshots

"""Development commands loaded once by the Hub's hook loader."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from biothings.utils.hub import HubShell

    hub: HubShell

__all__ = ["reload_snapshot_hooks"]


def reload_snapshot_hooks():
    """Read updated snapshot hooks and build helpers without restarting the Hub."""
    import importlib
    from pathlib import Path
    from hooks.dogpark_hook_utils import builds

    # Hook files have no __file__ in the Hub; imported modules do.
    path = Path(builds.__file__).resolve().parents[1] / "dogpark_snapshots.py"
    code = compile(path.read_text(encoding="utf-8"), str(path), "exec")
    importlib.invalidate_caches()
    importlib.reload(builds)
    server = getattr(hub, "server", None)
    if hasattr(server, "terminal"):
        # Keep BioThings 1.3's command catalog and hook origins up to date.
        server.process_hook_file(str(path))
    else:
        exec(code, hub.extra_ns, hub.extra_ns)
    return {"reloaded": [str(Path(builds.__file__).resolve()), str(path)]}


# BioThings 1.3 registers __all__; older Hubs need explicit registration.
if "hub" in globals() and not hasattr(getattr(hub, "server", None), "terminal"):
    hub.commands["reload_snapshot_hooks"] = reload_snapshot_hooks
    hub.hidden["reload_snapshot_hooks"] = False

from pathlib import Path

from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class KgDumper(LastModifiedHTTPDumper):
    """Use declared KGX releases as the dump gate when they are available."""

    def remote_is_better(self, remotefile, localfile):
        if getattr(self, "_release_check_failed", False):
            self.logger.warning("Skipping %s because its release could not be checked", remotefile)
            return False

        manifest_metadata = getattr(self.__class__, "__metadata__", {}).get("src_meta", {})
        has_declared_release = any(manifest_metadata.get(key) is not None for key in ("release", "graph"))
        if not has_declared_release:
            return super().remote_is_better(remotefile, localfile)

        if localfile is None or not Path(localfile).is_file():
            self.logger.debug("Local archive %s is missing", localfile)
            return True

        current_release = self.current_release
        if current_release is None:
            self.logger.debug("No current release is recorded for %s", self.src_name)
            return True

        if self.release != current_release:
            self.logger.info(
                "New release available for %s (remote: %s, current: %s)",
                self.src_name,
                self.release,
                current_release,
            )
            return True

        self.logger.debug("Release %s is already current; no download needed", self.release)
        return False

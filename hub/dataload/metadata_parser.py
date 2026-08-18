def get_kgx_release(self):
    """Determine the current KGX release from the configured metadata."""

    # "self" is a dumper instance, see:
    # https://github.com/biothings/biothings.api/blob/master/biothings/hub/dataload/dumper.py
    from datetime import datetime

    manifest_metadata = self.__class__.__metadata__.get("src_meta", {})
    self._release_check_failed = False

    def retain_current_release(message):
        current_release = getattr(self, "current_release", None)
        if current_release is None:
            raise ValueError(message)

        self._release_check_failed = True
        self.logger.warning("%s; retaining current release %s", message, current_release)
        return current_release

    # Release metadata is authoritative when present. Graph metadata is for
    # sources that do not publish a separate release document; switching
    # between the two can also switch version formats and cause a false dump.
    metadata_entry = manifest_metadata.get("release")
    metadata_kind = "release"
    if metadata_entry is None:
        metadata_entry = manifest_metadata.get("graph")
        metadata_kind = "graph"

    if metadata_entry is not None:
        if isinstance(metadata_entry, dict):
            metadata = metadata_entry
        else:
            try:
                meta_res = self.client.get(url=metadata_entry)
            except Exception as exc:
                self.logger.warning("Unable to fetch KGX %s metadata %s: %s", metadata_kind, metadata_entry, exc)
                return retain_current_release(f"Unable to fetch KGX {metadata_kind} metadata")

            if not meta_res.ok:
                return retain_current_release(f"Unable to fetch KGX {metadata_kind} metadata")

            try:
                metadata = meta_res.json()
            except Exception as exc:
                self.logger.warning("Unable to read KGX %s metadata %s: %s", metadata_kind, metadata_entry, exc)
                return retain_current_release(f"Unable to read KGX {metadata_kind} metadata")

        if not isinstance(metadata, dict):
            return retain_current_release(f"Invalid KGX {metadata_kind} metadata")

        # automat
        if "release_version" in metadata:
            release = metadata["release_version"]
            self.logger.info("version determined automat style: %s", release)
            return release

        # dingo
        if "version" in metadata:
            release = metadata["version"]
            self.logger.info("version determined DINGO style: %s", release)
            return release

        # legacy dingo
        transform_info = metadata.get("transform")
        if isinstance(transform_info, dict):
            source_version = transform_info.get("source_version")
            transform_version = transform_info.get("transform_version")
            if source_version and transform_version:
                release = f"{source_version}-{transform_version}"
                self.logger.info("version determined legacy DINGO style: %s", release)
                return release

        return retain_current_release(f"Unable to determine a version from configured KGX {metadata_kind} metadata")

    # Sources without release or graph metadata use the artifact timestamp as
    # their release identifier.
    # shamelessly stolen from
    # https://github.com/biothings/pending.api/blob/master/plugins/upheno_ontology/version.py
    def get_last_edited_version():
        dates = []

        for url in self.__class__.SRC_URLS:
            res = self.client.head(url, allow_redirects=True)
            last_modified = res.headers.get("Last-Modified")
            if not last_modified:
                raise KeyError(f"No Last-Modified header for URL: {url}")

            # parse the Last-Modified header
            dt = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").date()
            dates.append(dt)

        # Return the latest date
        latest_version = max(dates)
        return latest_version.isoformat()

    self.logger.info("No KGX release metadata configured; checking Last-Modified for a version string")
    try:
        return get_last_edited_version()
    except Exception as exc:
        self.logger.warning("Unable to determine a release from Last-Modified: %s", exc)
        return retain_current_release("Unable to determine a KGX release")

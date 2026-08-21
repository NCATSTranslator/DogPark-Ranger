from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from biothings.hub.dataload.dumper import LastModifiedHTTPDumper

# Root-level src_dump field holding the KGX metadata documents resolved at dump
# time. Read back by hub.dataload.uploader.kgUploader.KGXUploader.
KGX_METADATA_FIELD = "kgx_metadata"
KGX_METADATA_KEYS = ("graph", "release")

# Release metadata is authoritative when present. Graph metadata is for sources
# that do not publish a separate release document; switching between the two can
# also switch version formats and cause a false dump.
KGX_RELEASE_PRECEDENCE = ("release", "graph")


class KgDumper(LastModifiedHTTPDumper):
    """Dumper for KGX sources published with graph/release metadata documents.

    Provides the KGX release strategy itself, so manifests using this class must
    NOT declare a "release" entry -- pointing "class" at anything else silently
    falls back to the SDK's Last-Modified semantics instead.

    The metadata documents are declared in the manifest's "__metadata__" section,
    relative to the archive URL. They are fetched once per dump cycle and reused for
    both the release check and the src_dump record.
    """

    # replaced with a per-cycle dict on first use; see load_kgx_metadata()
    _kgx_metadata = None

    def remote_is_better(self, remotefile, localfile):
        if getattr(self, "_release_check_failed", False):
            self.logger.warning("Skipping %s because its release could not be checked", remotefile)
            return False

        sources = self.kgx_metadata_sources()
        if not any(sources.get(key) is not None for key in KGX_METADATA_KEYS):
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

    # entry point for set release
    def set_release(self):
        # start of a dump cycle: discard anything memoized for the previous one
        self._kgx_metadata = {}
        self.release = self.get_kgx_release()

    def kgx_metadata_sources(self):
        """Where to read each KGX metadata document from, keyed by KGX_METADATA_KEYS.

        A value is a URL to fetch or an already-inlined document; a key the manifest
        does not declare is absent, which is how a source says it publishes no such
        document.
        """
        declared = getattr(self.__class__, "__metadata__", {}).get("src_meta", {})
        return {
            key: self.resolve_kgx_metadata_url(declared[key]) for key in KGX_METADATA_KEYS if key in declared
        }

    def resolve_kgx_metadata_url(self, entry):
        """Resolve a declared metadata URL against the archive it belongs to.

        Manifests declare these relative to "data_url" -- "graph-metadata.json" for
        the document beside the archive, "../latest-release.json" for the one a level
        up -- so the store location is written once and a source pinned to a dated
        release directory needs no special casing. Absolute URLs pass through, as do
        documents inlined in the manifest.
        """
        urls = getattr(self.__class__, "SRC_URLS", None) or []
        if not isinstance(entry, str) or not urls:
            return entry
        # multiple archives are assumed to sit together; the first one sets the base
        return urljoin(urls[0], entry)

    def load_kgx_metadata(self, meta_key):
        """Return a declared KGX metadata document, fetching it at most once per cycle.

        set_release() runs before the download and post_dump() runs after it, and
        both need these documents. Resolving them once means the release string and
        the metadata stored alongside it can never describe different upstream
        releases -- the "latest" URLs move, so refetching could disagree.

        Returns None when the key isn't declared or couldn't be read; a failure is
        memoized too, deliberately, so the two phases stay consistent.
        """
        if self._kgx_metadata is None:
            self._kgx_metadata = {}
        if meta_key in self._kgx_metadata:
            return self._kgx_metadata[meta_key]

        entry = self.kgx_metadata_sources().get(meta_key)
        if isinstance(entry, dict):
            document = entry  # already inlined in the manifest
        elif isinstance(entry, str):
            document = self.fetch_kgx_metadata(meta_key, entry)
        else:
            document = None

        self._kgx_metadata[meta_key] = document
        return document

    def fetch_kgx_metadata(self, meta_key, url):
        """Fetch a single KGX metadata document, returning None if it can't be read."""
        try:
            res = self.client.get(url)
            if not res.ok:
                self.logger.warning("Unable to fetch KGX %s metadata %s", meta_key, url)
                return None
            metadata = res.json()
        except Exception as exc:
            self.logger.warning("Unable to read KGX %s metadata %s: %s", meta_key, url, exc)
            return None

        if not isinstance(metadata, dict):
            self.logger.warning("Ignoring non-object KGX %s metadata at %s", meta_key, url)
            return None

        self.logger.info("Fetched KGX %s metadata from %s", meta_key, url)
        return metadata

    def get_kgx_release(self):
        """Determine the current KGX release from the declared metadata."""
        self._release_check_failed = False

        sources = self.kgx_metadata_sources()
        metadata_kind = next(
            (key for key in KGX_RELEASE_PRECEDENCE if sources.get(key) is not None),
            None,
        )
        if metadata_kind is None:
            return self.get_release_from_last_modified()

        metadata = self.load_kgx_metadata(metadata_kind)
        if metadata is None:
            return self.retain_current_release(f"Unable to read KGX {metadata_kind} metadata")

        release = self.extract_kgx_version(metadata)
        if release is None:
            return self.retain_current_release(
                f"Unable to determine a version from configured KGX {metadata_kind} metadata"
            )
        return release

    def extract_kgx_version(self, metadata):
        """Pull a version string out of a KGX metadata document, or None."""
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

        return None

    def get_release_from_last_modified(self):
        """Sources without release or graph metadata use the artifact timestamp.

        shamelessly stolen from
        https://github.com/biothings/pending.api/blob/master/plugins/upheno_ontology/version.py
        """
        self.logger.info("No KGX release metadata configured; checking Last-Modified for a version string")
        try:
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
            return max(dates).isoformat()
        except Exception as exc:
            self.logger.warning("Unable to determine a release from Last-Modified: %s", exc)
            return self.retain_current_release("Unable to determine a KGX release")

    def retain_current_release(self, message):
        """Keep the recorded release when the check failed, and gate the dump on it."""
        current_release = getattr(self, "current_release", None)
        if current_release is None:
            raise ValueError(message)

        self._release_check_failed = True
        self.logger.warning("%s; retaining current release %s", message, current_release)
        return current_release


    #hook from metadata fetching
    def post_dump(self, *args, **kwargs):
        super().post_dump(*args, **kwargs)
        self.capture_kgx_metadata()

    def capture_kgx_metadata(self):
        """Record the KGX metadata documents for the release that was just downloaded.

        Pins src_meta to the release actually in the data folder and keeps a
        source's nodes and edges masters in agreement even when upstream publishes
        mid-cycle. Never raises: capture must not fail an otherwise good dump.
        """
        # Start from what was captured previously so a transient fetch failure
        # leaves the last known-good documents in place rather than dropping them.
        captured = dict(self.src_doc.get(KGX_METADATA_FIELD) or {})

        for meta_key in KGX_METADATA_KEYS:
            document = self.load_kgx_metadata(meta_key)
            if document is not None:
                captured[meta_key] = document

        if captured:
            # register_status("success") deep-copies src_doc right after post_dump,
            # which is what writes this to src_dump.
            self.src_doc[KGX_METADATA_FIELD] = captured

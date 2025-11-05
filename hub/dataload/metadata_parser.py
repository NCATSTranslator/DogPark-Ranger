def get_release_for_renci_kgs(self):
    # "self" is a dumper instance, see:
    # https://github.com/biothings/biothings.api/blob/master/biothings/hub/dataload/dumper.py
    import re
    from datetime import datetime

    manifest_metadata = self.__class__.__metadata__.get('src_meta', {})

    # use pre-generated version if present
    # avoid duplicate calculation if there are multiple files
    generated_version = manifest_metadata.get('generated_version', None)
    if generated_version is not None:
        return manifest_metadata["generated_version"]

    # parse metadata location from manifest
    meta_url = manifest_metadata.get('url', None)

    # fallback to parsing from data_url
    if meta_url is None or not isinstance(meta_url, str):
        data_urls = self.__class__.SRC_URLS
        d_url = data_urls[0]
        meta_url = d_url.rpartition('/')[0] + "/"

    res = self.client.get(url=meta_url)

    metadata_files =[]

    if res.ok:
        # get a list of files present on index page

        # files = re.findall(r'href="([^"/][^"]+)"', res.text)
        files = re.findall(r'href="([^"]+)"', res.text)

        for f in files:
            # check possible candidates for metadata json
            if f.endswith('metadata.json') or f.endswith('meta.json'):
                self.logger.info(f"metadata candidates found {f}")
                metadata_files.append(f)

    if metadata_files:
        for f in metadata_files:
            metadata_file = meta_url + f
            meta_res = self.client.get(url=metadata_file)
            if meta_res.ok:
                try:
                    metadata = meta_res.json()
                except ValueError as e:
                    self.logger.warning(f"Invalid JSON returned: {metadata_file}: {e}")
                    continue

                # automat
                if "graph_version" in metadata:
                    generated_version = metadata["graph_version"]
                    self.logger.info(f"version determined automat style: {generated_version}")
                    break

                # dingo
                if "transform" in metadata:
                    transform_info = metadata['transform']
                    if isinstance(transform_info, dict):
                        source_version = transform_info.get("source_version", None)
                        transform_version = transform_info.get("transform_version", None)
                        if source_version and transform_version:
                            generated_version = f"{source_version}-{transform_version}"
                            self.logger.info(f"version determined DINGO style: {generated_version}")
                            break
                        else:
                            raise ValueError(f"failed to parse version info DINGO style, source_version={source_version}, transform_version={transform_version}")

    if generated_version is not None:
        manifest_metadata['generated_version'] = generated_version
        return generated_version

    # fallback to last edited date if everything failed
    # shamelessly stolen from
    # https://github.com/biothings/pending.api/blob/master/plugins/upheno_ontology/version.py
    def get_last_edited_version():
        dates = []
        date_pattern = re.compile(r'\d{1,2}_\d{1,2}_\d{4}')

        for url in self.__class__.SRC_URLS:
            res = self.client.head(url, allow_redirects=True)
            header = res.headers['Content-Disposition']
            match = date_pattern.findall(header)
            if len(match) != 1:
                raise ValueError("Parser Date Extraction Error")
            dates.append(datetime.strptime(match[0], '%d_%m_%Y').date())

        if dates[0] < dates[1]:
            latest_version = dates[1]
        else:
            latest_version = dates[0]

        return latest_version.isoformat()

    self.logger.info("checking last edited date for version string")
    try:
        generated_version = get_last_edited_version()
    except Exception as e:
        self.logger.error(e)
        raise ValueError("can't determine version string from metadata")


    manifest_metadata['generated_version'] = generated_version

    return generated_version

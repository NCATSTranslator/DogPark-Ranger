import json
import requests
from biothings.hub.dataload.uploader import BaseSourceUploader

import config

logging = config.logger

class KGXUploader(BaseSourceUploader):
    # from template
    # __metadata__ = {"src_meta" : $__metadata__}

    def generate_doc_src_master(self):
        """Parse metadata from graph/release json files provided with KG datasets"""

        # modify and  inject __metadata__ here

        if hasattr(self.__class__, "__metadata__"):
            meta = self.__class__.__metadata__
            try:
                meta_src = meta["src_meta"]
                graph_loc = meta_src["graph"]
                release_loc = meta_src["release"]
                graph_metadata = requests.get(graph_loc).json()
                release_metadata = requests.get(release_loc).json()

                # injection
                self.__class__.__metadata__ = {
                    "src_meta": {
                        "graph": graph_metadata,
                        "release": release_metadata,
                    },
                }


            except KeyError as e:
                logging.info(f"Can't locate metadata file: {e}. Injection bypassed.")
            except requests.exceptions.RequestException as e:
                logging.info(f"Error getting remote metadata: {e}. Injection bypassed.")
            except json.JSONDecodeError as e:
                logging.info(f"Error parsing remote metadata {e}. Injection bypassed.")

        return super().generate_doc_src_master()


PUBLICATIONS_FIELD_NAME = "publications"
PUBLICATIONS_INFO_FIELD_NAME = "publications_info"
PMID_FIELD_NAME = "pmid"


def flatten_publications(data: dict) -> list:
    pmid_list = data[PUBLICATIONS_FIELD_NAME]
    publications_info = data[PUBLICATIONS_INFO_FIELD_NAME]

    def extend_pub_info(pmid: str):
        pub_info = publications_info.get(pmid, None)

        if pub_info is not None:
            # use underscore in pub_info field names
            pub_info = {
                (field.replace(" ", "_") if " " in field else field): value
                for field, value in pub_info.items()
            }


            pub_info[PMID_FIELD_NAME] = pmid
            return pub_info

    return list(map(extend_pub_info, pmid_list))


def process_publications(doc):
    if PUBLICATIONS_FIELD_NAME in doc and PUBLICATIONS_INFO_FIELD_NAME in doc:
        doc[PUBLICATIONS_INFO_FIELD_NAME] = flatten_publications(doc)
def merged_edges_mapping(cls):
    default_text = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    default_keyword = {"type": "keyword", "normalizer": "keyword_lowercase_normalizer"}

    edges_props = {
            "agent_type": default_keyword,
            "domain_range_exclusion": {"type": "boolean", "index": False},
            "id": {"type": "long", "index": False},
            "kg2_ids": {"type": "text", "index": False},
            "knowledge_level": default_keyword,
            "predicate": default_keyword,
            "all_predicates": default_keyword,
            "primary_knowledge_source": default_keyword,
            "publications": default_keyword,
            "publications_info": {
                "type": "object",
                "enabled": False,
            },
            "qualified_object_aspect": {
                "type": "text",
                "index": False,
            },
            "qualified_object_direction": {
                "type": "text",
                "index": False,
            },
            "qualified_predicate": {
                "type": "text",
                "index": False,
            },
    }

    nodes_props = {
        "all_categories": default_keyword,
        "all_names": default_text,
        "category": default_keyword,
        "description": {"type": "text", "index": False},
        "equivalent_curies": default_keyword,
        "id": default_keyword,
        "iri": {"type": "text", "index": False},
        "name": default_text,
        "publications": default_keyword,
    }


    return {
            **edges_props,
            "subject": {
                "type": "object",
                "properties": nodes_props
            },
            "object": {
                "type": "object",
                "properties": nodes_props
            },
    }


def nodes_mapping(cls):
    default_text = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    default_keyword = {"type": "keyword", "normalizer": "keyword_lowercase_normalizer"}


    nodes_props = {
        "all_categories": default_keyword,
        "all_names": default_text,
        "category": default_keyword,
        "description": {"type": "text", "index": False},
        "equivalent_curies": default_keyword,
        "id": default_keyword,
        "iri": {"type": "text", "index": False},
        "name": default_text,
        "publications": default_text,
    }

    return nodes_props
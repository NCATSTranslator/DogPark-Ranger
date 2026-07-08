GANDALF_INFORES = "infores:dogpark-tier0"


def process_sources(edge):
    """Normalize KGX edge sources the same way Gandalf's KGX loader does."""
    raw = edge.get("sources", [])

    if len(raw) == 0:
        sources = [
            {
                "resource_id": edge["primary_knowledge_source"],
                "resource_role": "primary_knowledge_source",
                "upstream_resource_ids": [],
            }
        ]

        if (
            "aggregator_knowledge_source" in edge
            and len(edge["aggregator_knowledge_source"]) > 0
        ):
            aggregators = list(reversed(edge["aggregator_knowledge_source"]))
            previous_source = edge["primary_knowledge_source"]
            for aggregator_source in aggregators:
                sources.append(
                    {
                        "resource_id": aggregator_source,
                        "resource_role": "aggregator_knowledge_source",
                        "upstream_resource_ids": [previous_source],
                    }
                )
                previous_source = aggregator_source
    else:
        sources = [
            {
                "resource_id": source["resource_id"],
                "resource_role": source["resource_role"],
                "upstream_resource_ids": source.get("upstream_resource_ids", []),
            }
            for source in raw
        ]

    all_upstream = {
        uid for source in sources for uid in source["upstream_resource_ids"]
    }
    top_ids = [
        source["resource_id"]
        for source in sources
        if source["resource_id"] not in all_upstream
    ]

    edge["sources"] = [
        {
            "resource_id": GANDALF_INFORES,
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": top_ids,
        }
    ] + sources

    return edge

def info_mapping(cls):
    """Mapping for compressed ubergraph adjacency list."""
    return ({
        "chunk_index": {
            "type": "integer"
        },
        "value": {
            "type": "keyword",
            "index": False
        }
    })
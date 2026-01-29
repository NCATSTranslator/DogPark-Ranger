def info_mapping(cls):
     return ({
             "chunk_index": {
                 "type": "integer"
             },
             "value": {
                 "type": "keyword",
                 "index": False
             }
     })

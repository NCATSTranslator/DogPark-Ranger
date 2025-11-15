import sys
import os
import json
import re
import argparse
from pymongo import MongoClient
from mongomock import MongoClient as MockMongoClient
from typing import Dict, List, Tuple

# --------------------------
# CONFIGURATION
# --------------------------
# MONGO_URI = "mongodb://localhost:27017"
# DB_NAME = "db_name"
# NODES_COLLECTION = "nodes_collection_name"
# EDGES_COLLECTION = "edges_collection_name"
# BATCH_SIZE = 2000                  # cursor batch size
# MAX_ITEMS = None                   # None for all, or set number for testing
# PREFIX_VERSION = "vTest"           # prefix for fields, types, uids
# SCHEMA_PATH = "schema.dgraph"      # original schema file
# --------------------------

def create_versioned_schema(schema_path, version_prefix):
    """
    Create a versioned copy of the schema file
    """
    base_dir = os.path.dirname(schema_path)
    base_name = os.path.basename(schema_path)
    versioned_schema_path = os.path.join(base_dir, f"{base_name}.{version_prefix}")

    print(f"Creating versioned schema at {versioned_schema_path}", file=sys.stderr)

    with open(schema_path, 'r') as infile, open(versioned_schema_path, 'w') as outfile:
        in_type_def = False
        current_type = None
        schema_metadata_section = False
        
        for line in infile:
            if "# SchemaMetadata" in line:
                schema_metadata_section = True
                outfile.write(line)
                continue
                
            if schema_metadata_section:
                if line.strip() == "" or (line.strip().startswith("#") and "# SchemaMetadata" not in line):
                    schema_metadata_section = False
                outfile.write(line)
                continue

            if line.strip().startswith('#') or not line.strip():
                outfile.write(line)
                continue
                
            type_match = re.match(r'type\s+(\w+)\s*\{', line)
            if type_match:
                in_type_def = True
                current_type = type_match.group(1)
                if current_type == "SchemaMetadata":
                    outfile.write(line)
                    continue
                outfile.write(f"type {version_prefix}_{current_type} {{\n")
                continue
            
            if in_type_def and '}' in line:
                in_type_def = False
                current_type = None
                outfile.write(line)
                continue
            
            if in_type_def:
                if current_type == "SchemaMetadata":
                    outfile.write(line)
                    continue
                field_match = re.match(r'(\s*)(\w+)(\s*)', line)
                if field_match:
                    whitespace = field_match.group(1)
                    field_name = field_match.group(2)
                    more_whitespace = field_match.group(3)
                    rest_of_line = line[field_match.end():]
                    outfile.write(f"{whitespace}{version_prefix}_{field_name}{more_whitespace}{rest_of_line}")
                else:
                    outfile.write(line)
                continue
            
            pred_match = re.match(r'(\w+)(\s*:.*)', line)
            if pred_match:
                predicate = pred_match.group(1)
                rest = pred_match.group(2)
                if predicate.startswith("schema_metadata_"):
                    outfile.write(line)
                    continue
                outfile.write(f"{version_prefix}_{predicate}{rest}\n")
                continue
            
            outfile.write(line)
    
    return versioned_schema_path

# --------------------------
# STREAMING FUNCTIONS
# --------------------------

def sanitize_uid(uid):
    """Sanitize a string to be a valid UID."""
    return re.sub(r'[^A-Za-z0-9_]', '_', str(uid))


def clean(d):
    """Remove None, NaN, empty lists, empty strings recursively and prefix field names"""
    if isinstance(d, dict):
        return {
            # Only add prefix if the key is NOT 'uid'
            (f"{PREFIX_VERSION}_{k}" if not k.startswith(PREFIX_VERSION + "_") and k != 'uid' else k): clean(v)
            for k, v in d.items()
            if v is not None and not (isinstance(v, float) and v != v) and v != [] and v != ""
        }
    if isinstance(d, list):
        cleaned_list = [clean(x) for x in d if x is not None and x != ""]
        return cleaned_list if cleaned_list else None
    return d


def node_to_dgraph(doc):
    node = {
        "dgraph.type": f"{PREFIX_VERSION}_Node",
        "uid": f"_:{PREFIX_VERSION}_node_{sanitize_uid(doc['id'])}",
        "id": doc.get("id"),
        "name": doc.get("name"),
        "in_taxon": doc.get("in_taxon", []),
        "information_content": doc.get("information_content"),
        "category": doc.get("category", []),
        "inheritance": doc.get("inheritance"),
        "provided_by": doc.get("provided_by", []),
        "description": doc.get("description"),
        "equivalent_identifiers": doc.get("equivalent_identifiers", []),
    }
    return clean(node)


def source_to_dgraph(doc):
    src = {
        "dgraph.type": f"{PREFIX_VERSION}_Source",
        "uid": f"_:{PREFIX_VERSION}_source_{sanitize_uid(doc['resource_id'])}",
        "resource_id": doc.get("resource_id"),
        "resource_role": doc.get("resource_role"),
        "upstream_resource_ids": doc.get("upstream_resource_ids", []),
        "source_record_urls": doc.get("source_record_urls", []),
    }
    return clean(src)


def edge_to_dgraph(doc):
    edge = {
        "dgraph.type": f"{PREFIX_VERSION}_Edge",
        "uid": f"_:{PREFIX_VERSION}_edge_{sanitize_uid(doc['id'])}",
        "predicate": doc.get("predicate"),
        "predicate_ancestors": doc.get("predicate_ancestors", []),
        "agent_type": doc.get("agent_type"),
        "knowledge_level": doc.get("knowledge_level"),
        "publications": doc.get("publications", []),
        "source_inforeses": doc.get("source_inforeses", []),
        "subject_form_or_variant_qualifier": doc.get("subject_form_or_variant_qualifier"),
        "qualified_predicate": doc.get("qualified_predicate"),
        "disease_context_qualifier": doc.get("disease_context_qualifier"),
        "frequency_qualifier": doc.get("frequency_qualifier"),
        "onset_qualifier": doc.get("onset_qualifier"),
        "sex_qualifier": doc.get("sex_qualifier"),
        "original_subject": doc.get("original_subject"),
        "original_predicate": doc.get("original_predicate"),
        "original_object": doc.get("original_object"),
        "allelic_requirement": doc.get("allelic_requirement"),
        "update_date": doc.get("update_date"),
        "z_score": doc.get("z_score"),
        "has_evidence": doc.get("has_evidence", []),
        "has_confidence_score": doc.get("has_confidence_score"),
        "has_count": doc.get("has_count"),
        "has_total": doc.get("has_total"),
        "has_percentage": doc.get("has_percentage"),
        "has_quotient": doc.get("has_quotient"),
        "eid": doc.get("id"),
        "ecategory": doc.get("category", []),
        "subject": {"uid": f"_:{PREFIX_VERSION}_node_{sanitize_uid(doc['subject'])}"},
        "object": {"uid": f"_:{PREFIX_VERSION}_node_{sanitize_uid(doc['object'])}"},
        "sources": [{"uid": f"_:{PREFIX_VERSION}_source_{sanitize_uid(s.get('resource_id'))}"} for s in doc.get("sources", []) if s.get('resource_id')],
    }
    return clean(edge)


def stream_collection(node_col, edge_col, node_fn, edge_fn, source_fn, out_file):
    """
    Stream nodes and edges as a single JSON array to the given output file/stream.
    """
    out_file.write("[\n")
    first = True

    # Nodes
    total_count = 0
    cursor = node_col.find({}, batch_size=BATCH_SIZE)
    for doc in cursor:
        if MAX_ITEMS is not None and total_count >= MAX_ITEMS:
            break
        item = node_fn(doc)
        if not first:
            out_file.write(",\n")
        out_file.write(json.dumps(item))
        first = False
        total_count += 1

    # --- Edges + Sources ---
    for doc in edge_col.find({}, batch_size=BATCH_SIZE):
        if MAX_ITEMS is not None and total_count >= MAX_ITEMS:
            break

        # Emit each embedded source as its own object
        for src in doc.get("sources", []):
            src_item = source_fn(src)
            if not first:
                out_file.write(",\n")
            out_file.write(json.dumps(src_item))
            first = False

        # Now emit the edge itself
        edge_item = edge_fn(doc)
        if not first:
            out_file.write(",\n")
        out_file.write(json.dumps(edge_item))
        first = False

        total_count += 1

    out_file.write("\n]\n")


def load_mock_data(db, nodes_file, edges_file):
    """Loads data from JSONL files into the mock MongoDB."""
    print(f"Loading mock data from {nodes_file} and {edges_file}", file=sys.stderr)
    
    # Load nodes
    nodes_collection = db[NODES_COLLECTION]
    with open(nodes_file, 'r') as f:
        # Use a list comprehension for concise loading
        nodes_data = [json.loads(line) for line in f if line.strip()]
    if nodes_data:
        nodes_collection.insert_many(nodes_data)
    print(f"Loaded {len(nodes_data)} nodes into mock DB.", file=sys.stderr)

    # Load edges
    edges_collection = db[EDGES_COLLECTION]
    with open(edges_file, 'r') as f:
        edges_data = [json.loads(line) for line in f if line.strip()]
    if edges_data:
        edges_collection.insert_many(edges_data)
    print(f"Loaded {len(edges_data)} edges into mock DB.", file=sys.stderr)


def main():
    # --- Argument Parser for mock data ---
    parser = argparse.ArgumentParser(description="Stream data from MongoDB to Dgraph JSON format.")
    
    # Mock data arguments
    parser.add_argument('--mock', nargs=2, metavar=('NODES_FILE', 'EDGES_FILE'),
                        help='Use mock data from specified JSONL files instead of a live MongoDB connection.')
    
    # Configuration arguments
    parser.add_argument('--mongo_uri', default="mongodb://localhost:27017", help='MongoDB connection URI.')
    parser.add_argument('--db_name', default="db_name", help='MongoDB database name.')
    parser.add_argument('--nodes_collection', default="nodes_collection_name", help='Nodes collection name.')
    parser.add_argument('--edges_collection', default="edges_collection_name", help='Edges collection name.')
    parser.add_argument('--batch_size', type=int, default=2000, help='Cursor batch size.')
    parser.add_argument('--max_items', type=int, default=None, help='Maximum number of items to process. Set to None for no limit.')
    parser.add_argument('--prefix_version', default="prefix_version", help='Prefix for Dgraph fields, types, and UIDs.')
    parser.add_argument('--schema_path', default="schema.dgraph", help='Path to the original Dgraph schema file.')
    parser.add_argument('--output_file', default=None, help='Path to output JSON file. If not provided, streams to stdout.')

    args = parser.parse_args()

    # --- Update Global Variables from Arguments ---
    global MONGO_URI, DB_NAME, NODES_COLLECTION, EDGES_COLLECTION, BATCH_SIZE, MAX_ITEMS, PREFIX_VERSION, SCHEMA_PATH
    MONGO_URI = args.mongo_uri
    DB_NAME = args.db_name
    NODES_COLLECTION = args.nodes_collection
    EDGES_COLLECTION = args.edges_collection
    BATCH_SIZE = args.batch_size
    MAX_ITEMS = args.max_items if args.max_items else None
    PREFIX_VERSION = args.prefix_version
    SCHEMA_PATH = args.schema_path


    # --- Create versioned schema first ---
    create_versioned_schema(SCHEMA_PATH, PREFIX_VERSION)

    if args.mock:
        # Use mongomock
        print("Using mock MongoDB.", file=sys.stderr)
        client = MockMongoClient()
        db = client[DB_NAME]
        # Load mock data from provided files
        load_mock_data(db, args.mock[0], args.mock[1])
    else:
        # Use real MongoDB connection
        print(f"Connecting to real MongoDB at {MONGO_URI}", file=sys.stderr)
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

    if args.output_file:
        print(f"Writing output to file: {args.output_file}", file=sys.stderr)
        with open(args.output_file, 'w') as f:
            stream_collection(
                db[NODES_COLLECTION],
                db[EDGES_COLLECTION],
                node_to_dgraph,
                edge_to_dgraph,
                source_to_dgraph,
                f  # Pass the file handle
            )
    else:
        print("Streaming output to stdout.", file=sys.stderr)
        stream_collection(
            db[NODES_COLLECTION],
            db[EDGES_COLLECTION],
            node_to_dgraph,
            edge_to_dgraph,
            source_to_dgraph,
            sys.stdout  # Pass stdout
        )


if __name__ == "__main__":
    main()

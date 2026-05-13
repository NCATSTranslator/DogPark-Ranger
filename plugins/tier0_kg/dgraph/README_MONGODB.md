# MongoDB to Dgraph JSON Parser (`parser_mongodb.py`)

## Overview

This Python script is designed to read node and edge data from a MongoDB database, transform it into a Dgraph-compatible JSON format, and stream the output to `stdout`. This allows for efficient, on-the-fly data loading into Dgraph using the `dgraph live` loader.

The script also supports a mock mode for local testing using JSONL files, and automatically generates a versioned Dgraph schema based on a template.

## Requirements

*   Python 3.x
*   `pymongo`
*   `mongomock` (for testing with local files)

## Installation

Install the required Python libraries using pip:

```bash
pip install pymongo mongomock
```

## Usage

The script can be run in two primary modes: connecting to a live MongoDB instance or using local mock data.

### 1. Live MongoDB Mode

This is the default mode. The script connects to the specified MongoDB instance and streams the data. The output should be piped to the `dgraph live` command.

**Example:**

```bash
python3 parser_mongodb.py \
    --mongo_uri "mongodb://your_mongo_host:27017" \
    --db_name "your_db" \
    --nodes_collection "your_nodes" \
    --edges_collection "your_edges" \
    --prefix_version "vMyData1" \
    --schema_path "schema.dgraph" \
    --max_items -1 \
    | dgraph live \
        -f - \
        -s schema.dgraph.vMyData1 \
        --alpha your_dgraph_alpha:9080 \
        --zero your_dgraph_zero:5080 \
        --format=json
```

### 2. Mock Mode (for Local Testing)

Use the `--mock` flag to read data from local JSONL files instead of connecting to MongoDB. This is useful for testing and development.

**Example:**

```bash
python3 parser_mongodb.py \
    --mock /path/to/nodes.jsonl /path/to/edges.jsonl \
    --prefix_version "vTestLocal" \
    > output.json
```
This will generate a Dgraph-compatible `output.json` file that can be loaded separately:
```bash
dgraph live -f output.json -s schema.dgraph.vTestLocal --alpha ...
```

### 3. Writing to a File

By default, the script streams to `stdout`. To save the output to a file instead, use the `--output_file` argument. This is useful for saving the generated JSON for later use.

**Example:**

```bash
python3 parser_mongodb.py \
    --mongo_uri "mongodb://your_mongo_host:27017" \
    --db_name "your_db" \
    --output_file "dgraph_data.json"
```

## Command-Line Arguments

All configuration settings can be controlled via command-line arguments.

| Argument | Description | Default Value |
|---|---|---|
| `--mock` | Use mock data from specified JSONL files. Expects two paths: `NODES_FILE` and `EDGES_FILE`. | `None` |
| `--output_file` | Path to an output JSON file. If not provided, the script streams to `stdout`. | `None` |
| `--mongo_uri` | MongoDB connection URI. | `"mongodb://localhost:27017"` |
| `--db_name` | MongoDB database name. | `"db_name"` |
| `--nodes_collection` | The name of the nodes collection in MongoDB. | `"nodes_collection_name"` |
| `--edges_collection` | The name of the edges collection in MongoDB. | `"edges_collection_name"` |
| `--batch_size` | The batch size for the MongoDB cursor. | `2000` |
| `--max_items` | Maximum number of items to process. Useful for testing. Set to `None` for no limit. | `None` |
| `--prefix_version` | The version string to prefix to all Dgraph types and predicates. | `"prefix_version"` |
| `--schema_path` | Path to the source Dgraph schema file. | `"schema.dgraph"` |

To see all options, run:
```bash
python3 parser_mongodb.py --help
```

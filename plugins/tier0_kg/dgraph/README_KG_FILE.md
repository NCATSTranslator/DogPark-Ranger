# DGraph Knowledge Graph Processing Tools

This directory contains three main tools for processing knowledge graph data with DGraph:

1. **`parser_kg_file.py`** - Converts JSONL knowledge graph data to RDF format for DGraph
2. **`dgraph_set_active_version.py`** - Manages schema versions in DGraph
3. **`dgraph_drop_data_version.py`** - Drops specific versioned schema elements from DGraph

## Prerequisites

- Python 3.7+
- DGraph database running and accessible
- Required Python packages:
  - `bmt` (Biolink Model Toolkit)
  - `requests`
  - `json`
  - `argparse`

## parser_kg_file.py - JSONL to RDF Converter

Converts JSONL node and edge files to RDF format compatible with DGraph, applying version prefixes to all schema elements.

### Usage

```bash
python parser_kg_file.py <nodes_file> <edges_file> <output_file> <schema_file> <version_prefix> [options]
```

### Parameters

- `nodes_file`: Path to JSONL file containing nodes
- `edges_file`: Path to JSONL file containing edges
- `output_file`: Path for RDF output file (use `-` for stdout)
- `schema_file`: Path to DGraph schema file
- `version_prefix`: Version identifier (alphanumeric + underscores only)

### Options

- `--include-all-fields`: Include all JSON fields as versioned predicates
- `--stream`: Output to stdout instead of file

### Examples

#### Basic Conversion
```bash
# Convert small dataset to RDF file
python parser_kg_file.py nodes.jsonl edges.jsonl output.rdf schema.dgraph v1_0

# Convert with all fields included
python parser_kg_file.py nodes.jsonl edges.jsonl output.rdf schema.dgraph v1_0 --include-all-fields
```

#### Streaming to DGraph Live
```bash
# Stream directly to DGraph without creating intermediate file
python parser_kg_file.py nodes.jsonl edges.jsonl - schema.dgraph v1_0 --stream | \
  dgraph live -f /dev/stdin \
  -s schema.dgraph.v1_0 \
  --alpha localhost:19080 \
  --zero localhost:5080 \
  --format=rdf
```

#### File-based Processing
```bash
# Process and save to file first, then load
python parser_kg_file.py nodes.jsonl edges.jsonl kg_v1.rdf schema.dgraph v1

# Load the generated RDF file
cat kg_v1.rdf | dgraph live \
  -f /dev/stdin \
  -s schema.dgraph.v1 \
  --alpha localhost:19080 \
  --zero localhost:5080 \
  --format=rdf
```

### Output

The parser creates:
- **RDF file**: Contains all nodes and edges in DGraph-compatible RDF format
- **Versioned schema**: Creates `schema.dgraph.{version_prefix}` with versioned predicates and types

## dgraph_set_active_version.py - Version Management

Manages schema versions in DGraph by setting active versions and creating new version entries.

### Usage

```bash
python dgraph_set_active_version.py [options]
```

### Options

- `--version, -v`: Schema version to set as active
- `--endpoint, -e`: DGraph endpoint (default: `http://localhost:8080`)
- `--create, -c`: Create version if it doesn't exist
- `--list, -l`: List all schema versions without making changes

### Examples

#### List Current Versions
```bash
# List versions on local DGraph
python dgraph_set_active_version.py --list

# List versions on remote DGraph
python dgraph_set_active_version.py --list --endpoint http://localhost:8080
```

#### Set Active Version
```bash
# Set existing version as active
python dgraph_set_active_version.py --version v1_0

# Create new version and set as active
python dgraph_set_active_version.py --version v2_0 --create

# Work with remote DGraph instance
python dgraph_set_active_version.py --version v1_prod --endpoint http://localhost:8080
```

### Output Example

```
Connecting to Dgraph at http://localhost:8080

Current Schema Versions:
------------------------
 [ ] v1_0
 [✓] v1_1
 [ ] v2_0

Successfully set schema version 'v2_0' as active

Current Schema Versions:
------------------------
 [ ] v1_0
 [ ] v1_1
 [✓] v2_0
```

## dgraph_drop_data_version.py - Data Cleanup

Removes all schema elements (predicates and types) with a specific version prefix from DGraph.

### Usage

```bash
python dgraph_drop_data_version.py <version_prefix> <endpoint>
```

### Parameters

- `version_prefix`: Version prefix to drop (e.g., `v1_0`)
- `endpoint`: DGraph endpoint URL

### Examples

#### Drop Version Data
```bash
# Drop all v1_0 schema elements from local DGraph
python dgraph_drop_data_version.py v1_0 "http://localhost:8080"
```

### Interactive Confirmation

The script will show what will be deleted and ask for confirmation:

```
Dropping all schema elements with prefix: v1_0
Found 245 predicates and 2 types with prefix v1_0
Predicates to drop:
  - v1_0_id
  - v1_0_category
  - v1_0_predicate
  ...
Types to drop:
  - v1_0_Node
  - v1_0_Edge

Are you sure you want to drop these schema elements? (y/n):
```

⚠️ **Warning**: This operation is irreversible. All data associated with the version will be permanently deleted.

## Common Workflows

### Complete Data Loading Workflow

1. **Convert and Load Data**
```bash
# Convert JSONL to RDF with streaming
python parser_kg_file.py nodes.jsonl edges.jsonl - schema.dgraph v3_0 --stream | \
  dgraph live -f /dev/stdin -s schema.dgraph.v3_0 \
  --alpha localhost:19080 --zero localhost:5080 --format=rdf
```

2. **Set New Version as Active**
```bash
# Create and activate the new version
python dgraph_set_active_version.py --version v3_0 --create
```

3. **Verify Loading**
```bash
# Check that the version is active
python dgraph_set_active_version.py --list
```

### Version Cleanup Workflow

1. **List Current Versions**
```bash
python dgraph_set_active_version.py --list
```

2. **Drop Old Version**
```bash
python dgraph_drop_data_version.py v1_0 "http://localhost:8080"
```

3. **Verify Cleanup**
```bash
python dgraph_set_active_version.py --list
```

### Development Testing Workflow

1. **Load Test Data**
```bash
python parser_kg_file.py test_nodes.jsonl test_edges.jsonl - schema.dgraph test_v1 --stream | \
  dgraph live -f /dev/stdin -s schema.dgraph.test_v1 \
  --alpha localhost:9080 --zero localhost:5080 --format=rdf
```

2. **Test Queries Against Test Version**
```bash
# Set test version as active for testing
python dgraph_set_active_version.py --version test_v1 --create
```

3. **Clean Up Test Data**
```bash
python dgraph_drop_data_version.py test_v1 "http://localhost:8080"
```

### Production Deployment Workflow

1. **Load Production Data**
```bash
python parser_kg_file.py /data/prod_nodes.jsonl /data/prod_edges.jsonl prod_v2.rdf schema.dgraph prod_v2

# Load to production DGraph
cat prod_v2.rdf | dgraph live -f /dev/stdin -s schema.dgraph.prod_v2 \
  --alpha production-server:9080 --zero production-server:5080 --format=rdf
```

2. **Activate New Production Version**
```bash
python dgraph_set_active_version.py --version prod_v2 --create \
  --endpoint http://production-server:8080
```

3. **Clean Up Old Production Version (after verification)**
```bash
python dgraph_drop_data_version.py prod_v1 "http://production-server:8080"
```

## Troubleshooting

### Common Issues

#### Connection Issues
```bash
# Verify DGraph is running
curl http://localhost:8080/health

# Check if ports are accessible
telnet localhost 9080
telnet localhost 5080
```

### Validation

#### Verify Data Loading
```graphql
# Query to check loaded data
{
  nodeCount(func: type(v1_0_Node)) {
    count(uid)
  }
  edgeCount(func: type(v1_0_Edge)) {
    count(uid)
  }
}
```

#### Check Schema
```bash
# Query schema
curl -X POST localhost:8080/query -H "Content-Type: application/dql" -d 'schema {}'
```

For more detailed information about the DGraph query language and administration, consult the [DGraph Documentation](https://dgraph.io/docs/).
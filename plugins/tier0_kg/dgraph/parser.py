import argparse
import bmt
import json
import os
import re
import sys
import uuid
# from biothings.utils.common import uncompressall

biolink = bmt.Toolkit()

_predicate_ancestors_cache = {}  # Cache for predicate ancestors


def sanitize_uid(uid):
    """Sanitize a string to be a valid RDF blank node label."""
    return re.sub(r'[^A-Za-z0-9_]', '_', str(uid))

def sanitize_field_name(name):
    """Make a safe predicate/field name (alphanumeric + underscore)."""
    return re.sub(r'[^A-Za-z0-9_]', '_', str(name))

def rdf_literal(value):
    """
    Return a properly quoted/escaped RDF literal text (including surrounding quotes).
    - Strings -> JSON-escaped string (includes quotes)
    - Dicts -> stored as a quoted JSON string
    - Other primitives (int/float/bool/etc) -> converted to string and JSON-quoted
    """
    if isinstance(value, str):
        # Replace actual newlines with spaces and strip surrounding whitespace.
        v = value.replace('\r', '').replace('\n', '').strip()
        return json.dumps(v)
    if isinstance(value, dict):
        # store dict as a JSON string literal
        return json.dumps(json.dumps(value))
    # For numbers, booleans, None and others — convert to string then JSON-quote
    return json.dumps("" if value is None else str(value))

def emit_nested_object_as_bnode(parent_uid, pred, obj, version_prefix, lines):
    """
    Create a blank node for `obj` (a dict) and append triples:
      parent_uid <version_pred> _:bnode .
      _:bnode <version_field> "value" .
    Recursively handles nested dicts and lists.
    """
    bnode = f'_:b{uuid.uuid4().hex[:12]}'
    lines.append(f'{parent_uid} <{version_prefix}_{pred}> {bnode} .')

    for k, v in obj.items():
        pk = sanitize_field_name(k)
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    emit_nested_object_as_bnode(bnode, pk, item, version_prefix, lines)
                else:
                    lines.append(f'{bnode} <{version_prefix}_{pk}> {rdf_literal(item)} .')
        elif isinstance(v, dict):
            emit_nested_object_as_bnode(bnode, pk, v, version_prefix, lines)
        else:
            lines.append(f'{bnode} <{version_prefix}_{pk}> {rdf_literal(v)} .')

def extract_predicate_type(predicate):
    """Extract predicate type from predicate, removing biolink: prefix"""
    if predicate.startswith('biolink:'):
        return predicate[8:]
    return predicate

def extract_category_type(category):
    """Extract category type, removing biolink: prefix"""
    if category and isinstance(category, str) and category.startswith('biolink:'):
        return category[8:]
    return category

def extract_all_predicates_ancestors(predicate_type):
    """
    Extract all ancestor predicates for a given predicate type using Biolink Model.
    Results are cached for better performance.

    Args:
        predicate_type: The predicate type (without biolink: prefix)

    Returns:
        List of predicates including the original predicate and all its ancestors
    """
    # Check cache first
    if predicate_type in _predicate_ancestors_cache:
        return _predicate_ancestors_cache[predicate_type]

    try:
        # Add the current predicate to the list (it's its own ancestor)
        all_predicates = []

        # Get ancestors from BMT toolkit
        ancestors = biolink.get_ancestors(predicate_type, formatted=True)
        if ancestors:
            # Remove 'biolink:' prefix from ancestors
            ancestors = [extract_predicate_type(anc) for anc in ancestors]
            # Add them to our list
            all_predicates.extend(ancestors)
    except Exception as e:
        # If there's an error, just use the current predicate
        print(f"Warning: Could not get ancestors for predicate '{predicate_type}': {e}", file=sys.stderr)
        all_predicates = [predicate_type]

    # Cache the result
    _predicate_ancestors_cache[predicate_type] = all_predicates
    return all_predicates

def extract_all_source_inforeses(edge):
    """
    Extract all unique inforeses from:
      - resource_id fields inside the 'sources' list (if present)
      - any field in edge whose name ends with '_source' (including lists)
    Returns a set of unique strings.
    """
    inforeses = set()

    # 1. From sources list
    sources = edge.get("sources", [])
    if isinstance(sources, list):
        for src in sources:
            if isinstance(src, dict):
                rid = src.get("resource_id")
                if rid:
                    inforeses.add(rid)

    # 2. From any *_source field in edge
    for k, v in edge.items():
        if k.endswith("_source"):
            # Could be a string or list
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, str):
                        inforeses.add(item)
            elif isinstance(v, str):
                inforeses.add(v)

    return sorted(inforeses)

def extract_all_sources(edge):
    """
    Extracts all unique sources from the 'sources' list field and any other
    field in the edge dictionary ending with '_source'.

    The unique source is defined by the (resource_id, resource_role) pair.

    Args:
        edge: A dictionary representing an edge/association.

    Returns:
        A list of dictionaries, where each dictionary contains
        'resource_id' and 'resource_role'.
    """
    
    # Use a set of tuples (resource_id, resource_role) to check for duplicates
    unique_sources: set[Tuple[str, str]] = set() 
    
    # The final list of reduced source dictionaries
    filtered_sources: List[Dict[str, str]] = []
    
    # --- 1. Process the existing 'sources' list field ---
    edge_sources = edge.get("sources", [])
    
    if edge_sources:
        for source in edge_sources:
            resource_id = source.get("resource_id")
            resource_role = source.get("resource_role")
            
            # Ensure values are strings for consistency
            if isinstance(resource_id, str) and isinstance(resource_role, str):
                source_tuple = (resource_id, resource_role)
                
                if source_tuple not in unique_sources:
                    unique_sources.add(source_tuple)
                    filtered_sources.append({
                        "resource_id": resource_id,
                        "resource_role": resource_role
                    })

    # --- 2. Process other fields ending with '_source' ---
    for key, value in edge.items():
        # Check if the key ends with '_source', is not the original 'sources' list field, and has a string value
        if key.endswith('_source') and key != 'sources' and isinstance(value, str):
            # Value (e.g., infores:goa) goes to resource_id
            resource_id = value 
            # Key (e.g., primary_knowledge_source) goes to resource_role
            resource_role = key
            source_tuple = (resource_id, resource_role)

            if source_tuple not in unique_sources:
                unique_sources.add(source_tuple)
                filtered_sources.append({
                    "resource_id": resource_id,
                    "resource_role": resource_role
                })
                
    return filtered_sources

def node_to_rdf(node, version_prefix, include_remaining=False):
    """
    Convert a node to RDF triples with version prefixes
    
    Args:
        node: Node data dictionary
        version_prefix: Version prefix to add to all types and predicates
        include_remaining: if True, emit all other fields not explicitly handled
    """
    uid = f'_:{sanitize_uid(node["id"])}'

    # Add version prefix to type
    node_type = f'{version_prefix}_Node'

    lines = [
        f'{uid} <dgraph.type> "{node_type}" .',
        f'{uid} <{version_prefix}_id> {rdf_literal(node.get("id"))} .',
    ]

    handled = set(["id"])

    # name
    node_name = node.get("name", "")
    if node_name:
        lines.append(f'{uid} <{version_prefix}_name> {rdf_literal(node_name)} .')
        handled.add("name")

    # description
    node_description = node.get("description", "")
    if node_description:
        lines.append(f'{uid} <{version_prefix}_description> {rdf_literal(node_description)} .')
        handled.add("description")

    node_inheritance = node.get("inheritance", "")
    if node_inheritance:
        lines.append(f'{uid} <{version_prefix}_inheritance> {rdf_literal(node_inheritance)} .')
        handled.add("inheritance")

    node_information_content = node.get("information_content", "")
    if node_information_content:
        lines.append(f'{uid} <{version_prefix}_information_content> {rdf_literal(node_information_content)} .')
        handled.add("information_content")

    # Handle category list
    category = node.get("category", [])
    if category:
        for cat in category:
            lines.append(f'{uid} <{version_prefix}_category> {rdf_literal(extract_category_type(cat))} .')
        handled.add("category")
    
    # Handle provided_by list
    provided_by = node.get("provided_by", [])
    if provided_by:
        for pb in provided_by:
            lines.append(f'{uid} <{version_prefix}_provided_by> {rdf_literal(pb)} .')
        handled.add("provided_by")

    # Handle equivalent_identifiers list
    equivalent_identifiers = node.get("equivalent_identifiers", [])
    if equivalent_identifiers:
        for eq in equivalent_identifiers:
            lines.append(f'{uid} <{version_prefix}_equivalent_identifiers> {rdf_literal(eq)} .')
        handled.add("equivalent_identifiers")
    
    # Handle in_taxon list
    in_taxon = node.get("in_taxon", [])
    if in_taxon:
        for tax in in_taxon:
            lines.append(f'{uid} <{version_prefix}_in_taxon> {rdf_literal(tax)} .')
        handled.add("in_taxon")

    # Optionally emit any other remaining fields
    if include_remaining:
        handled.add("negated")
        for key, val in node.items():
            if key in handled:
                continue
            # skip internal / meta if present
            if key.startswith("_"):
                continue

            pred = sanitize_field_name(key)
            # lists -> multiple predicate lines
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        emit_nested_object_as_bnode(uid, pred, item, version_prefix, lines)
                    else:
                        lines.append(f'{uid} <{version_prefix}_{pred}> {rdf_literal(item)} .')
            elif isinstance(val, dict):
                emit_nested_object_as_bnode(uid, pred, val, version_prefix, lines)
            else:
                lines.append(f'{uid} <{version_prefix}_{pred}> {rdf_literal(val)} .')

    return lines

def create_versioned_schema(schema_path, version_prefix):
    """
    Create a versioned copy of the schema file
    
    Args:
        schema_path: Path to original schema file
        version_prefix: Version prefix to add to all types and predicates
    
    Returns:
        Path to the new versioned schema file
    """
    # Create output filename with version
    base_dir = os.path.dirname(schema_path)
    base_name = os.path.basename(schema_path)
    versioned_schema_path = os.path.join(base_dir, f"{base_name}.{version_prefix}")

    # # Check if the file already exists
    # if os.path.exists(versioned_schema_path):
    #     print(f"ERROR: Schema file {versioned_schema_path} already exists.", file=sys.stderr)
    #     print("To prevent accidental overwriting, please delete the existing file first", file=sys.stderr)
    #     print("or choose a different version prefix.", file=sys.stderr)
    #     return None
    
    print(f"Creating versioned schema at {versioned_schema_path}", file=sys.stderr)
    
    # Process the schema file line by line
    with open(schema_path, 'r') as infile, open(versioned_schema_path, 'w') as outfile:
        # Track if we're in a type definition block
        in_type_def = False
        current_type = None
        schema_metadata_section = False
        
        for line in infile:
            # Check if we're entering the SchemaMetadata section
            if "# SchemaMetadata" in line:
                schema_metadata_section = True
                outfile.write(line)
                continue
                
            # If we're in the SchemaMetadata section, don't version the fields
            if schema_metadata_section:
                # Check if we're leaving the SchemaMetadata section (empty line or new section)
                if line.strip() == "" or line.strip().startswith("#") and "# SchemaMetadata" not in line:
                    schema_metadata_section = False
                outfile.write(line)
                continue

            # Skip comments and empty lines
            if line.strip().startswith('#') or not line.strip():
                outfile.write(line)
                continue
                
            # Check if line starts a type definition
            type_match = re.match(r'type\s+(\w+)\s*\{', line)
            if type_match:
                in_type_def = True
                current_type = type_match.group(1)

                # Don't rename SchemaMetadata type
                if current_type == "SchemaMetadata":
                    outfile.write(line)
                    continue

                # Rename the type
                outfile.write(f"type {version_prefix}_{current_type} {{\n")
                continue
            
            # Check if line ends a type definition
            if in_type_def and '}' in line:
                in_type_def = False
                current_type = None
                outfile.write(line)
                continue
            
            # Inside a type definition, also prefix the field names
            if in_type_def:
                # Don't rename fields in SchemaMetadata
                if current_type == "SchemaMetadata":
                    outfile.write(line)
                    continue

                # Match field name at start of line (with optional whitespace)
                field_match = re.match(r'(\s*)(\w+)(\s*)', line)
                if field_match:
                    whitespace = field_match.group(1)
                    field_name = field_match.group(2)
                    more_whitespace = field_match.group(3)
                    rest_of_line = line[field_match.end():]
                    
                    # Add prefix to field name
                    outfile.write(f"{whitespace}{version_prefix}_{field_name}{more_whitespace}{rest_of_line}")
                else:
                    outfile.write(line)
                continue
            
            # Regular predicate definition
            # Format is typically: predicate: type @directive .
            pred_match = re.match(r'(\w+)(\s*:.*)', line)
            if pred_match:
                predicate = pred_match.group(1)
                rest = pred_match.group(2)

                # Don't add version prefix to schema_metadata fields
                if predicate.startswith("schema_metadata_"):
                    outfile.write(line)
                    continue

                # Add version prefix to predicate
                outfile.write(f"{version_prefix}_{predicate}{rest}\n")
                continue
            
            # If we get here, just copy the line
            outfile.write(line)
    
    return versioned_schema_path

def edge_to_rdf_with_edge_type(edge, edge_counter, version_prefix, include_remaining=False):
    """
    Convert an edge to RDF triples using Edge type instead of facets, with version prefixes
    """
    subj_uid = f'_:{sanitize_uid(edge["subject"])}'
    obj_uid = f'_:{sanitize_uid(edge["object"])}'
    edge_uid = f'_:edge_{version_prefix}_{edge_counter}'  # Include version in edge ID
    
    # Extract predicate type from predicate
    predicate_type = extract_predicate_type(edge.get("predicate", ""))

    # Add version prefix to type
    edge_type = f'{version_prefix}_Edge'

    lines = [
        f'{edge_uid} <dgraph.type> "{edge_type}" .',
        f'{edge_uid} <{version_prefix}_object> {obj_uid} .',
        f'{edge_uid} <{version_prefix}_subject> {subj_uid} .',
        f'{edge_uid} <{version_prefix}_predicate> {json.dumps(predicate_type)} .',
    ]

    handled = set(["subject", "object", "predicate"])
    
    # Get all ancestor predicates using our cached function
    predicate_ancestors = extract_all_predicates_ancestors(predicate_type)
    # Add predicate_ancestors field with ancestor predicates
    for pred in predicate_ancestors:
        lines.append(f'{edge_uid} <{version_prefix}_predicate_ancestors> {json.dumps(pred)} .')
        handled.add("predicate_ancestors")

    # Get all ancestor predicates using our cached function
    all_source_inforeses = extract_all_source_inforeses(edge)
    # Add predicate_ancestors field with ancestor predicates
    for pred in all_source_inforeses:
        lines.append(f'{edge_uid} <{version_prefix}_source_inforeses> {json.dumps(pred)} .')
        handled.add("source_inforeses")

    edge_agent_type = edge.get("agent_type", "")
    if edge_agent_type:
        lines.append(f'{edge_uid} <{version_prefix}_agent_type> {json.dumps(edge_agent_type)} .')
        handled.add("agent_type")

    edge_knowledge_level = edge.get("knowledge_level", "")
    if edge_knowledge_level:
        lines.append(f'{edge_uid} <{version_prefix}_knowledge_level> {json.dumps(edge_knowledge_level)} .')
        handled.add("knowledge_level")

    edge_publications = edge.get("publications", [])
    if edge_publications:
        for pub in edge_publications:
            lines.append(f'{edge_uid} <{version_prefix}_publications> {rdf_literal(pub)} .')
        handled.add("publications")

    edge_subject_form_or_variant_qualifier = edge.get("subject_form_or_variant_qualifier", "")
    if edge_subject_form_or_variant_qualifier:
        lines.append(f'{edge_uid} <{version_prefix}_subject_form_or_variant_qualifier> {json.dumps(edge_subject_form_or_variant_qualifier)} .')
        handled.add("subject_form_or_variant_qualifier")

    edge_qualified_predicate = edge.get("qualified_predicate", "")
    if edge_qualified_predicate:
        lines.append(f'{edge_uid} <{version_prefix}_qualified_predicate> {json.dumps(edge_qualified_predicate)} .')
        handled.add("qualified_predicate")

    edge_frequency_qualifier = edge.get("frequency_qualifier", "")
    if edge_frequency_qualifier:
        lines.append(f'{edge_uid} <{version_prefix}_frequency_qualifier> {json.dumps(edge_frequency_qualifier)} .')
        handled.add("frequency_qualifier")

    edge_onset_qualifier = edge.get("onset_qualifier", "")
    if edge_onset_qualifier:
        lines.append(f'{edge_uid} <{version_prefix}_onset_qualifier> {json.dumps(edge_onset_qualifier)} .')
        handled.add("onset_qualifier")

    edge_sex_qualifier = edge.get("sex_qualifier", "")
    if edge_sex_qualifier:
        lines.append(f'{edge_uid} <{version_prefix}_sex_qualifier> {json.dumps(edge_sex_qualifier)} .')
        handled.add("sex_qualifier")

    edge_original_subject = edge.get("original_subject", "")
    if edge_original_subject:
        lines.append(f'{edge_uid} <{version_prefix}_original_subject> {json.dumps(edge_original_subject)} .')
        handled.add("original_subject")

    edge_original_predicate = edge.get("original_predicate", "")
    if edge_original_predicate:
        lines.append(f'{edge_uid} <{version_prefix}_original_predicate> {json.dumps(edge_original_predicate)} .')
        handled.add("original_predicate")

    edge_original_object = edge.get("original_object", "")
    if edge_original_object:
        lines.append(f'{edge_uid} <{version_prefix}_original_object> {json.dumps(edge_original_object)} .')
        handled.add("original_object")

    edge_allelic_requirement = edge.get("allelic_requirement", "")
    if edge_allelic_requirement:
        lines.append(f'{edge_uid} <{version_prefix}_allelic_requirement> {json.dumps(edge_allelic_requirement)} .')
        handled.add("allelic_requirement")

    edge_update_date = edge.get("update_date", "")
    if edge_update_date:
        lines.append(f'{edge_uid} <{version_prefix}_update_date> {json.dumps(edge_update_date)} .')
        handled.add("update_date")

    edge_z_score = edge.get("z_score", "")
    if edge_z_score:
        lines.append(f'{edge_uid} <{version_prefix}_z_score> {rdf_literal(edge_z_score)} .')
        handled.add("z_score")

    edge_has_evidence = edge.get("has_evidence", [])
    if edge_has_evidence:
        for evidence in edge_has_evidence:
            lines.append(f'{edge_uid} <{version_prefix}_has_evidence> {rdf_literal(evidence)} .')
        handled.add("has_evidence")

    edge_has_confidence_score = edge.get("has_confidence_score", "")
    if edge_has_confidence_score:
        lines.append(f'{edge_uid} <{version_prefix}_has_confidence_score> {rdf_literal(edge_has_confidence_score)} .')
        handled.add("has_confidence_score")

    edge_has_count = edge.get("has_count", "")
    if edge_has_count:
        lines.append(f'{edge_uid} <{version_prefix}_has_count> {rdf_literal(edge_has_count)} .')
        handled.add("has_count")

    edge_has_total = edge.get("has_total", "")
    if edge_has_total:
        lines.append(f'{edge_uid} <{version_prefix}_has_total> {rdf_literal(edge_has_total)} .')
        handled.add("has_total")

    edge_has_percentage = edge.get("has_percentage", "")
    if edge_has_percentage:
        lines.append(f'{edge_uid} <{version_prefix}_has_percentage> {rdf_literal(edge_has_percentage)} .')
        handled.add("has_percentage")

    edge_has_quotient = edge.get("has_quotient", "")
    if edge_has_quotient:
        lines.append(f'{edge_uid} <{version_prefix}_has_quotient> {rdf_literal(edge_has_quotient)} .')
        handled.add("has_quotient")

    # edge_sources = edge.get("sources", [])
    # if edge_sources:
    #     filtered_sources = [
    #         {
    #             "resource_id": source.get("resource_id"),
    #             "resource_role": source.get("resource_role")
    #         }
    #         for source in edge_sources
    #     ]
    #     sources_string = json.dumps(filtered_sources)
    #     escaped_sources_string = sources_string.replace('"', '\\"')
    #     lines.append(f'{edge_uid} <{version_prefix}_sources> "{escaped_sources_string}" .')
    #     handled.add("sources")

    # Get all sources from other *_source fields including sources list
    all_sources = extract_all_sources(edge)
    # Add sources field with all sources

    # if all_sources:
    #     sources_string = json.dumps(all_sources)
    #     escaped_sources_string = sources_string.replace('"', '\\"')
    #     lines.append(f'{edge_uid} <{version_prefix}_sources> "{escaped_sources_string}" .')
    #     handled.add("sources")
    if all_sources:
        for source_obj in all_sources:
            # Create a new blank node for each source
            source_bnode = f'_:b{uuid.uuid4().hex[:12]}'
            # Link the edge to the new source node
            lines.append(f'{edge_uid} <{version_prefix}_sources> {source_bnode} .')
            # Define the new source node
            lines.append(f'{source_bnode} <dgraph.type> "{version_prefix}_Source" .')
            lines.append(f'{source_bnode} <{version_prefix}_resource_id> {rdf_literal(source_obj.get("resource_id"))} .')
            lines.append(f'{source_bnode} <{version_prefix}_resource_role> {rdf_literal(source_obj.get("resource_role"))} .')
        handled.add("sources")


    edge_eid = edge.get("id", "")
    if edge_eid:
        lines.append(f'{edge_uid} <{version_prefix}_eid> {json.dumps(edge_eid)} .')
        handled.add("eid")

    edge_category = edge.get("category", "")
    if edge_category:
        for cat in edge_category:
            lines.append(f'{edge_uid} <{version_prefix}_ecategory> {rdf_literal(extract_category_type(cat))} .')
        handled.add("ecategory")

    # Handle publications as list
    publications = edge.get("publications", [])
    if publications:
        for pub in publications:
            lines.append(f'{edge_uid} <{version_prefix}_publications> {json.dumps(pub)} .')
        handled.add("publications")

    # Optionally emit any other remaining fields
    if include_remaining:
        for key, val in edge.items():
            if key in handled:
                continue
            if key.startswith("_"):
                continue
            pred = sanitize_field_name(key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        emit_nested_object_as_bnode(edge_uid, pred, item, version_prefix, lines)
                    else:
                        lines.append(f'{edge_uid} <{version_prefix}_{pred}> {rdf_literal(item)} .')
            elif isinstance(val, dict):
                emit_nested_object_as_bnode(edge_uid, pred, val, version_prefix, lines)
            else:
                lines.append(f'{edge_uid} <{version_prefix}_{pred}> {rdf_literal(val)} .')

    return lines

def convert_jsonl_to_rdf(nodes_path, edges_path, out_path, schema_path, version_prefix, include_all_fields=False, stream_output=False):
    """
    Convert JSONL files to RDF format for DGraph using Edge type and version prefixes.
    If include_all_fields is True, emit any remaining fields found in records.
    
    Args:
        nodes_path: Path to nodes JSONL file
        edges_path: Path to edges JSONL file  
        out_path: Output RDF file path
        schema_path: Path to schema file to version
        version_prefix: Version prefix to add to all types and predicates
        include_all_fields: If True, emit all other fields not explicitly handled
        stream_output: If True, write to stdout instead of file
    """
    
    if stream_output:
        print(f"Converting {nodes_path} and {edges_path} to RDF with version {version_prefix} (streaming to stdout)",
              file=sys.stderr)
        out_file = sys.stdout
    else:
        print(f"Converting {nodes_path} and {edges_path} to RDF with version {version_prefix}: {out_path}", file=sys.stderr)
        out_file = open(out_path, "w", buffering=8192*4)  # Larger buffer for better I/O performance
    
    try:
        # First create the versioned schema
        versioned_schema_path = create_versioned_schema(schema_path, version_prefix)
        if versioned_schema_path is None:
            print("Conversion aborted due to schema file conflict.", file=sys.stderr)
            if not stream_output:
                out_file.close()
            return False
        
        node_count = 0
        edge_count = 0
        predicate_types = set()
        duplicate_count = 0
        edge_tracker = {}  # Track (subject, object) pairs for duplicate detection
        
        # Process nodes first
        print("Processing nodes...", file=sys.stderr)
        with open(nodes_path, buffering=8192*4) as f:  # Larger read buffer
            for line in f:
                if line.strip():
                    node = json.loads(line)
                    for rdf_line in node_to_rdf(node, version_prefix, include_remaining=include_all_fields):
                        out_file.write(rdf_line + "\n")
                    node_count += 1
                    
                    if node_count % 50000 == 0:  # Less frequent progress updates
                        print(f"Processed {node_count} nodes...", file=sys.stderr)

        print(f"Processed {node_count} nodes total.", file=sys.stderr)

        # Process edges in streaming fashion with duplicate detection
        print("Processing edges...", file=sys.stderr)
        with open(edges_path, buffering=8192*4) as f:  # Larger read buffer
            batch_lines = []
            batch_size = 1000  # Process in batches for better performance
            skipped_negated_count = 0  # Count edges skipped due to negated=true
            
            for line in f:
                if line.strip():
                    batch_lines.append(line)
                    
                    if len(batch_lines) >= batch_size:
                        # Process batch
                        for batch_line in batch_lines:
                            edge = json.loads(batch_line)
                            
                            # Skip edges explicitly marked as negated
                            neg = edge.get("negated", False)
                            neg_val = False
                            if isinstance(neg, str):
                                neg_val = neg.strip().lower() in ("true", "1", "yes")
                            else:
                                neg_val = bool(neg)
                            if neg_val:
                                skipped_negated_count += 1
                                continue

                            # Track duplicates efficiently
                            edge_key = (edge["subject"], edge["object"])
                            if edge_key in edge_tracker:
                                # This is a duplicate - assign unique ID
                                edge_tracker[edge_key] += 1
                                edge["_duplicate_id"] = edge_tracker[edge_key]
                                duplicate_count += 1
                            else:
                                # First occurrence
                                edge_tracker[edge_key] = 0
                                edge["_duplicate_id"] = 0
                            
                            # Process edge immediately (streaming)
                            predicate_type = extract_predicate_type(edge.get("predicate", "has_edge"))
                            predicate_types.add(predicate_type)
                            
                            for rdf_line in edge_to_rdf_with_edge_type(edge, edge_count, version_prefix, include_remaining=include_all_fields):
                                out_file.write(rdf_line + "\n")
                            edge_count += 1
                        
                        batch_lines = []  # Clear batch
                        
                        if edge_count % 50000 == 0:  # Less frequent progress updates
                            print(f"Processed {edge_count} edges...", file=sys.stderr)
            
            # Process remaining items in batch
            for batch_line in batch_lines:
                edge = json.loads(batch_line)
                
                # Skip edges explicitly marked as negated
                neg = edge.get("negated", False)
                neg_val = False
                if isinstance(neg, str):
                    neg_val = neg.strip().lower() in ("true", "1", "yes")
                else:
                    neg_val = bool(neg)
                if neg_val:
                    skipped_negated_count += 1
                    continue

                # Track duplicates efficiently
                edge_key = (edge["subject"], edge["object"])
                if edge_key in edge_tracker:
                    edge_tracker[edge_key] += 1
                    edge["_duplicate_id"] = edge_tracker[edge_key]
                    duplicate_count += 1
                else:
                    edge_tracker[edge_key] = 0
                    edge["_duplicate_id"] = 0
                
                predicate_type = extract_predicate_type(edge.get("predicate", "has_edge"))
                predicate_types.add(predicate_type)
                
                for rdf_line in edge_to_rdf_with_edge_type(edge, edge_count, version_prefix, include_remaining=include_all_fields):
                    out_file.write(rdf_line + "\n")
                edge_count += 1
        
        print(f"Processed {edge_count} edges total.", file=sys.stderr)

        # ...existing code...
        print(f"Found {duplicate_count} duplicate edges (preserved with unique identifiers)", file=sys.stderr)
        print(f"Found {len(predicate_types)} unique predicate types:", file=sys.stderr)
        for pred_type in sorted(predicate_types)[:10]:
            print(f"  - {pred_type}", file=sys.stderr)
        if len(predicate_types) > 10:
            print(f"  ... and {len(predicate_types) - 10} more", file=sys.stderr)

        if stream_output:
            print(f"Conversion complete. RDF data streamed to stdout.", file=sys.stderr)
        else:
            print(f"Conversion complete. Versioned RDF data written to {out_path}", file=sys.stderr)

        print(f"Versioned schema written to {versioned_schema_path}", file=sys.stderr)

        return True

    finally:
        # Close the output file if we opened one
        if not stream_output and out_file is not sys.stdout:
            out_file.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert JSONL node/edge files to Dgraph RDF with versioned predicates."
    )
    # parser.add_argument("path", help="Path to compacted file that contains nodes and edges JSONL files")
    parser.add_argument("nodes_path", help="Path to nodes JSONL file")
    parser.add_argument("edges_path", help="Path to edges JSONL file")
    parser.add_argument("out_path", help="Output RDF file path")
    parser.add_argument("schema_path", help="Path to schema.dgraph")
    parser.add_argument("version_prefix", help="Version prefix")
    parser.add_argument("--include-all-fields", action="store_true",
                        help="Include all remaining fields found in JSON records as versioned predicates")
    parser.add_argument('--stream', action='store_true', 
                        help='Output RDF to stdout instead of creating a file')
    args = parser.parse_args()

    # Validate version prefix (should be alphanumeric)
    if not re.match(r'^[A-Za-z0-9_]+$', args.version_prefix):
        print("Error: Version prefix should only contain letters, numbers, and underscores")
        sys.exit(1)

    # print("uncompressing '%s'", args.path)
    # uncompressall(args.path)
    # print("done uncompressing '%s'", args.path)

    # Use stdout if --stream is specified or out_path is "-"
    stream_output = args.stream or args.out_path == "-"

    success = convert_jsonl_to_rdf(
        nodes_path=args.nodes_path,
        edges_path=args.edges_path,
        out_path=args.out_path if not stream_output else "/dev/null",  # Ignore out_path when streaming
        schema_path=args.schema_path,
        version_prefix=args.version_prefix,
        include_all_fields=args.include_all_fields,
        stream_output=stream_output
    )
    if not success:
        sys.exit(1)

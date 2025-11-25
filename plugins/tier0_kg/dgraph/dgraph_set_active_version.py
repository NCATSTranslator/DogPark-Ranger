#!/usr/bin/env python3
# filepath: /home/erodolpho/dgraph/schema_v9.4/dgraph_set_active_version.py

# # Make the script executable
# chmod +x dgraph_set_active_version.py

# Just list all schema versions
# ./dgraph_set_active_version.py --list
# ./dgraph_set_active_version.py --list --endpoint http://localhost:8080

# # Set an existing version as active
# ./dgraph_set_active_version.py --version v2_small

# # Create a new version and set as active
# ./dgraph_set_active_version.py --version v3 --create

# # Specify a different endpoint
# ./dgraph_set_active_version.py --version v2_small --endpoint http://localhost:8080
# ./dgraph_set_active_version.py --version v2 --create --endpoint http://localhost:8080

import requests
import json
import sys
import argparse
import os
import base64
import msgpack


def setup_argument_parser():
    parser = argparse.ArgumentParser(description='Set active schema version in Dgraph')
    parser.add_argument('--version', '-v', required=False, help='Schema version to set as active')
    parser.add_argument('--endpoint', '-e', default='http://localhost:8080', 
                       help='Dgraph endpoint (default: http://localhost:8080)')
    parser.add_argument('--create', '-c', action='store_true', 
                       help='Create version if it does not exist')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List all schema versions without making changes')
    parser.add_argument('--mapping-dir', '-m', default='.',
                       help='Directory containing mapping files (default: current directory)')
    return parser


def load_mapping_file(version, mapping_dir='.'):
    """Load and serialize mapping file for the given version"""
    mapping_file = os.path.join(mapping_dir, f"mapping.dgraph.{version}")

    if not os.path.exists(mapping_file):
        print(f"Warning: Mapping file '{mapping_file}' not found. Using empty mapping.")
        return base64.b64encode(msgpack.packb({})).decode('utf-8')

    try:
        with open(mapping_file, 'r') as f:
            mapping_data = json.load(f)

        # Serialize with msgpack and encode as base64 for storage in Dgraph
        packed_data = msgpack.packb(mapping_data)
        encoded_data = base64.b64encode(packed_data).decode('utf-8')

        print(f"Loaded mapping from '{mapping_file}' ({len(mapping_data)} entries)")
        return encoded_data

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in mapping file '{mapping_file}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading mapping file '{mapping_file}': {e}")
        sys.exit(1)


def get_schema_versions(endpoint):
    """Get all schema versions from Dgraph using JSON query"""
    query = {
        "query": """
        {
          versions(func: type(SchemaMetadata)) {
            uid
            schema_metadata_version
            schema_metadata_is_active
            schema_metadata_mapping
          }
        }
        """
    }

    try:
        response = requests.post(
            f"{endpoint}/query",
            headers={"Content-Type": "application/json"},
            json=query,
            timeout=10
        )

        # print(f"Response status: {response.status_code}")
        # print(f"Response headers: {response.headers}")

        if response.status_code != 200:
            print(f"Error fetching schema versions: {response.text}")
            return None

        data = response.json()
        # print(f"Response data: {json.dumps(data, indent=2)}")

        return data.get('data', {}).get('versions', [])

    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

def set_active_version(endpoint, version, create=False, mapping_dir='.'):
    """Set the specified version as active using a single request when possible"""
    # Get existing versions
    versions = get_schema_versions(endpoint)
    if versions is None:
        return False

    # Find if our target version exists
    target_uid = None
    for v in versions:
        if v['schema_metadata_version'] == version:
            target_uid = v['uid']
            break

    # If version doesn't exist and create is False, show error
    if target_uid is None and not create:
        print(f"Error: Version '{version}' does not exist. Use --create to create it.")
        return False

    # Load mapping data for this version
    mapping_data = load_mapping_file(version, mapping_dir)

    if target_uid:
        # If the version exists, deactivate all, activate target, and update mapping
        update_json = {
            "query": """
            {
                versions as var(func: type(SchemaMetadata))
                target as var(func: type(SchemaMetadata)) @filter(eq(schema_metadata_version, "%s"))
            }
            """ % version,
            "set": [
                {
                    "uid": "uid(versions)",
                    "schema_metadata_is_active": "false"
                },
                {
                    "uid": "uid(target)",
                    "schema_metadata_is_active": "true",
                    "schema_metadata_mapping": mapping_data
                }
            ]
        }
    else:
        # Create new version with mapping
        update_json = {
            "query": """
            {
                versions as var(func: type(SchemaMetadata))
            }
            """,
            "set": [
                {
                    "uid": "uid(versions)",
                    "schema_metadata_is_active": "false"
                },
                {
                    "dgraph.type": "SchemaMetadata",
                    "schema_metadata_version": version,
                    "schema_metadata_is_active": "true",
                    "schema_metadata_mapping": mapping_data
                }
            ]
        }

    update_response = requests.post(
        f"{endpoint}/mutate?commitNow=true",
        headers={"Content-Type": "application/json"},
        json=update_json
    )

    if update_response.status_code != 200:
        print(f"Error updating versions: {update_response.text}")
        return False

    print(f"Activated schema version: {version}")

    return True


def decode_mapping_data(encoded_data):
    """Decode base64 + msgpack encoded mapping data"""
    if not encoded_data:
        return {}

    try:
        decoded_data = base64.b64decode(encoded_data.encode('utf-8'))
        mapping_data = msgpack.unpackb(decoded_data, raw=False)
        return mapping_data
    except Exception as e:
        print(f"Warning: Could not decode mapping data: {e}")
        return {}


def display_versions(endpoint):
    """Display all schema versions with mapping info"""
    versions = get_schema_versions(endpoint)
    if versions is None:
        return

    print("\nCurrent Schema Versions:")
    print("------------------------")
    for v in versions:
        active = "✓" if v.get('schema_metadata_is_active') else " "
        version_name = v.get('schema_metadata_version', 'Unknown')

        # Decode mapping data to show summary
        mapping_data = decode_mapping_data(v.get('schema_metadata_mapping', ''))
        mapping_info = f" ({len(mapping_data)} mappings)" if mapping_data else " (no mapping)"

        print(f" [{active}] {version_name}{mapping_info}")
    print()

def main():
    parser = setup_argument_parser()
    args = parser.parse_args()

    print(f"Connecting to Dgraph at {args.endpoint}")

    # Display current versions
    display_versions(args.endpoint)

    # If --list is provided, just show versions and exit
    if args.list:
        return

    # Version is required if not in list mode
    if not args.version:
        print("Error: --version is required when not using --list")
        parser.print_help()
        sys.exit(1)

    # Set active version
    if set_active_version(args.endpoint, args.version, args.create, args.mapping_dir):
        # Show updated versions
        display_versions(args.endpoint)
        print(f"Successfully set schema version '{args.version}' as active")
    else:
        print(f"Failed to set schema version '{args.version}' as active")
        sys.exit(1)

if __name__ == "__main__":
    main()

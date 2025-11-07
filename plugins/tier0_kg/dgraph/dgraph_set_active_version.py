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


def setup_argument_parser():
    parser = argparse.ArgumentParser(description='Set active schema version in Dgraph')
    parser.add_argument('--version', '-v', required=False, help='Schema version to set as active')
    parser.add_argument('--endpoint', '-e', default='http://localhost:8080', 
                       help='Dgraph endpoint (default: http://localhost:8080)')
    parser.add_argument('--create', '-c', action='store_true', 
                       help='Create version if it does not exist')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List all schema versions without making changes')
    return parser

def get_schema_versions(endpoint):
    """Get all schema versions from Dgraph using JSON query"""
    query = {
        "query": """
        {
          versions(func: type(SchemaMetadata)) {
            uid
            schema_metadata_version
            schema_metadata_is_active
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

def set_active_version(endpoint, version, create=False):
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
    
    if target_uid:
        # If the version exists, deactivate all and activate target in one request
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
                    "schema_metadata_is_active": "true"
                }
            ]
        }
    else:
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
                    "schema_metadata_is_active": "true"
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

def display_versions(endpoint):
    """Display all schema versions"""
    versions = get_schema_versions(endpoint)
    if versions is None:
        return
    
    print("\nCurrent Schema Versions:")
    print("------------------------")
    for v in versions:
        active = "✓" if v['schema_metadata_is_active'] == True else " "
        print(f" [{active}] {v['schema_metadata_version']}")
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
    if set_active_version(args.endpoint, args.version, args.create):
        # Show updated versions
        display_versions(args.endpoint)
        print(f"Successfully set schema version '{args.version}' as active")
    else:
        print(f"Failed to set schema version '{args.version}' as active")
        sys.exit(1)

if __name__ == "__main__":
    main()
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

# # Use token for authentication (if Dgraph ACL is enabled)
# ./dgraph_set_active_version.py --token $TOKEN --list
# ./dgraph_set_active_version.py --token $TOKEN --version v2 --create --endpoint http://localhost:8080

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
    parser.add_argument('--token', default=None,
                       help='Optional Dgraph access token (adds X-Dgraph-AccessToken header)')
    parser.add_argument('--debug', action='store_true',
                       help='Print request and response details for troubleshooting')
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


def build_headers(token: str | None = None) -> dict:
    """Build common headers for Dgraph requests, including optional auth."""
    headers = {"Content-Type": "application/json"}
    if token:
        # Prefer JWT via Authorization for ACL-enabled Dgraph
        # Include access token header for Dgraph HTTP API
        headers["X-Dgraph-AccessToken"] = token
    return headers


def _redact_token_value(value: str) -> str:
    """Redact a token string, showing only a few characters for debugging."""
    if not isinstance(value, str) or not value:
        return ""  # nothing to show
    if len(value) <= 10:
        return value[:3] + "..."
    return value[:6] + "..." + value[-4:]


def redact_headers_for_debug(headers: dict) -> dict:
    """Return a copy of headers with sensitive values redacted for debug output."""
    safe = dict(headers or {})
    # Redact Authorization: Bearer <token>
    auth = safe.get("Authorization")
    if isinstance(auth, str) and auth.lower().startswith("bearer "):
        token_part = auth.split(" ", 1)[1]
        safe["Authorization"] = "Bearer " + _redact_token_value(token_part)
    # Redact direct access token header
    if "X-Dgraph-AccessToken" in safe:
        safe["X-Dgraph-AccessToken"] = _redact_token_value(safe.get("X-Dgraph-AccessToken"))
    return safe


def decode_jwt_namespace(token: str | None) -> int | None:
    """Decode JWT payload to read the 'namespace' claim if present."""
    if not token or not isinstance(token, str):
        return None
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        # Fix padding for base64url
        padding = '=' * (-len(payload_b64) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(payload_bytes.decode('utf-8'))
        ns = payload.get('namespace')
        if isinstance(ns, int):
            return ns
        # fallback if some deployments use 'ns'
        ns2 = payload.get('ns')
        return ns2 if isinstance(ns2, int) else None
    except Exception:
        return None


def get_schema_versions(endpoint, token: str | None = None, debug: bool = False):
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
        headers = build_headers(token)
        if debug:
            try:
                print(f"[DEBUG] Query headers: {json.dumps(redact_headers_for_debug(headers))}")
            except Exception:
                print("[DEBUG] Query headers: <unprintable>")
        response = requests.post(
            f"{endpoint}/query",
            headers=headers,
            json=query,
            timeout=10
        )

        if debug:
            print(f"[DEBUG] Query response status: {response.status_code}")
            print(f"[DEBUG] Query response body: {response.text}")

        if response.status_code != 200:
            print(f"Error fetching schema versions: {response.text}")
            return None

        data = response.json()

        # Gracefully handle missing or null fields
        data_block = data.get('data') or {}
        versions = data_block.get('versions') or []
        if versions:
                return versions

        fallback_query = {
                "query": """
                {
                    versions(func: has(schema_metadata_version)) {
                        uid
                        schema_metadata_version
                        schema_metadata_is_active
                        schema_metadata_mapping
                    }
                }
                """
        }
        fb_headers = build_headers(token)
        if debug:
            try:
                print(f"[DEBUG] Fallback headers: {json.dumps(redact_headers_for_debug(fb_headers))}")
            except Exception:
                print("[DEBUG] Fallback headers: <unprintable>")
        fallback_resp = requests.post(
            f"{endpoint}/query",
            headers=fb_headers,
            json=fallback_query,
            timeout=10
        )
        if debug:
            print(f"[DEBUG] Fallback query status: {fallback_resp.status_code}")
            print(f"[DEBUG] Fallback query body: {fallback_resp.text}")
        if fallback_resp.status_code != 200:
                print(f"Error fetching schema versions (fallback): {fallback_resp.text}")
                return []
        fallback_data = fallback_resp.json()
        return (fallback_data.get('data') or {}).get('versions') or []

    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

def set_active_version(endpoint, version, create=False, mapping_dir='.', token: str | None = None, debug: bool = False):
    """Set the specified version as active using two transactions for stability."""
    # 1) Discover existing versions
    versions = get_schema_versions(endpoint, token)
    if versions is None:
        return False

    target_uid = None
    for v in versions:
        if v.get('schema_metadata_version') == version:
            target_uid = v.get('uid')
            break

    # 2) Create the version node if missing (create-only txn)
    if target_uid is None:
        if not create:
            print(f"Error: Version '{version}' does not exist. Use --create to create it.")
            return False

        create_json = {
            "set": [
                {
                    "dgraph.type": "SchemaMetadata",
                    "schema_metadata_version": version,
                    # Create as inactive; activation happens in step 3
                    "schema_metadata_is_active": "false"
                }
            ]
        }
        create_headers = build_headers(token)
        if debug:
            try:
                print(f"[DEBUG] Create headers: {json.dumps(redact_headers_for_debug(create_headers))}")
            except Exception:
                print("[DEBUG] Create headers: <unprintable>")
        create_resp = requests.post(
            f"{endpoint}/mutate?commitNow=true",
            headers=create_headers,
            json=create_json
        )

        if debug:
            try:
                print("\n[DEBUG] Create payload:")
                print(json.dumps(create_json, indent=2))
            except Exception:
                print("\n[DEBUG] Could not pretty-print create payload.")
            print(f"[DEBUG] Create response status: {create_resp.status_code}")
            print(f"[DEBUG] Create response body: {create_resp.text}\n")

        if create_resp.status_code != 200:
            print(f"Error creating version '{version}': {create_resp.text}")
            return False

        # Re-query to obtain the new UID
        versions = get_schema_versions(endpoint, token)
        if versions is None:
            print("Error: Could not fetch versions after creation.")
            return False
        for v in versions:
            if v.get('schema_metadata_version') == version:
                target_uid = v.get('uid')
                break
        if not target_uid:
            print("Error: Could not locate newly created version node.")
            return False

    # 3) Separate txn: deactivate others, activate target, and update mapping
    mapping_data = load_mapping_file(version, mapping_dir)
    update_json = {
        "query": f"""
        {{
            versions as var(func: type(SchemaMetadata))
            target as var(func: type(SchemaMetadata)) @filter(eq(schema_metadata_version, \"{version}\"))
        }}
        """,
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

    activation_headers = build_headers(token)
    if debug:
        try:
            print(f"[DEBUG] Activation headers: {json.dumps(redact_headers_for_debug(activation_headers))}")
        except Exception:
            print("[DEBUG] Activation headers: <unprintable>")
    update_resp = requests.post(
        f"{endpoint}/mutate?commitNow=true",
        headers=activation_headers,
        json=update_json
    )

    if debug:
        try:
            print("\n[DEBUG] Activation payload:")
            print(json.dumps(update_json, indent=2))
        except Exception:
            print("\n[DEBUG] Could not pretty-print activation payload.")
        print(f"[DEBUG] Activation response status: {update_resp.status_code}")
        print(f"[DEBUG] Activation response body: {update_resp.text}\n")

    if update_resp.status_code != 200:
        print(f"Error activating version '{version}': {update_resp.text}")
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


def display_versions(endpoint, token: str | None = None, debug: bool = False):
    """Display all schema versions with mapping info"""
    versions = get_schema_versions(endpoint, token, debug)
    if versions is None:
        return

    print("\nCurrent Schema Versions:")
    print("------------------------")
    # Filter out any null or non-dict entries to avoid attribute errors
    safe_versions = [v for v in versions if isinstance(v, dict)]
    if not safe_versions:
        print(" (no SchemaMetadata entries found)")
        print()
        return

    for v in safe_versions:
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
    if args.debug and args.token:
        token_preview = _redact_token_value(args.token)
        ns_claim = decode_jwt_namespace(args.token)
        ns_msg = f", namespace claim={ns_claim}" if ns_claim is not None else ""
        print(f"[DEBUG] Token preview: {token_preview}{ns_msg}")

    # Display current versions
    display_versions(args.endpoint, args.token, args.debug)

    # If --list is provided, just show versions and exit
    if args.list:
        return

    # Version is required if not in list mode
    if not args.version:
        print("Error: --version is required when not using --list")
        parser.print_help()
        sys.exit(1)

    # Set active version
    if set_active_version(args.endpoint, args.version, args.create, args.mapping_dir, args.token, args.debug):
        # Show updated versions
        display_versions(args.endpoint, args.token, args.debug)
        print(f"Successfully set schema version '{args.version}' as active")
    else:
        print(f"Failed to set schema version '{args.version}' as active")
        sys.exit(1)

if __name__ == "__main__":
    main()

import json
import requests
import sys

def main():
    version_prefix = sys.argv[1]
    endpoint = sys.argv[2]
    print(f"Dropping all schema elements with prefix: {version_prefix}")

    # Fetch schema
    print("Fetching schema...")
    schema_response = requests.post(
        f"{endpoint}/query",
        headers={"Content-Type": "application/dql"},
        data="schema {}"
    )
    
    if schema_response.status_code != 200:
        print(f"Error fetching schema: {schema_response.text}")
        sys.exit(1)
    
    schema_data = schema_response.json()
    
    # Find predicates with our prefix
    predicates = [p['predicate'] for p in schema_data['data']['schema'] 
                 if p['predicate'].startswith(version_prefix)]
    
    # Find types with our prefix
    types = [t['name'] for t in schema_data['data']['types'] 
             if t['name'].startswith(version_prefix)]
    
    print(f"Found {len(predicates)} predicates and {len(types)} types with prefix {version_prefix}")
    
    if not predicates and not types:
        print(f"No schema elements found with prefix {version_prefix}. Nothing to drop.")
        return
    
    # Print the schema elements we're going to drop
    if predicates:
        print("Predicates to drop:")
        for pred in predicates:
            print(f"  - {pred}")
    
    if types:
        print("Types to drop:")
        for typ in types:
            print(f"  - {typ}")
    
    # Confirm before proceeding
    confirm = input("Are you sure you want to drop these schema elements? (y/n): ")
    if confirm.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Drop each predicate
    if predicates:
        print("Starting to drop predicates...")
        for predicate in predicates:
            print(f"Dropping predicate {predicate}...")
            drop_response = requests.post(
                f"{endpoint}/alter",
                headers={"Content-Type": "application/dql"},
                json={"drop_attr": predicate}
            )
            print(f"Result: {drop_response.text}")
    
    # Drop each type
    if types:
        print("Starting to drop types...")
        for type_name in types:
            print(f"Dropping type {type_name}...")
            drop_response = requests.post(
                f"{endpoint}/alter",
                headers={"Content-Type": "application/dql"},
                json={"drop_op": "TYPE", "drop_value": type_name}
            )
            print(f"Result: {drop_response.text}")
    
    print(f"All {version_prefix} schema elements dropped!")

if __name__ == "__main__":
    main()

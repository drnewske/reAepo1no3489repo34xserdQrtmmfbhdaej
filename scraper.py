import json
import os
import hashlib

def generate_match_id(match_title, date, competition):
    """
    Generate a unique and deterministic ID for a match based on its core properties.
    This ensures that the same match will always have the same ID.
    """
    # Normalize the string to ensure consistency (lowercase, no leading/trailing whitespace)
    match_string = f"{match_title}|{date}|{competition}".lower().strip()
    return hashlib.md5(match_string.encode('utf-8')).hexdigest()

def process_existing_json(file_path="matches.json"):
    """
    Adds a unique 'match_id' to each match object in the specified JSON file
    and removes any duplicate entries. This is intended as a one-time processing script.
    """
    # 1. Check if the target file exists before proceeding.
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' was not found. Please ensure it is in the same directory as this script.")
        return

    # 2. Load the existing JSON data from the file.
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            existing_matches = json.load(f)
        if not isinstance(existing_matches, list):
            print(f"Error: The data in '{file_path}' is not a valid JSON list of matches.")
            return
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading or parsing '{file_path}': {e}")
        return

    print(f"Successfully loaded {len(existing_matches)} matches from '{file_path}'.")
    print("Processing matches to add unique IDs and remove duplicates...")

    unique_matches = []
    seen_ids = set()
    duplicates_found = 0
    malformed_entries = 0

    # 3. Iterate through each match, generate an ID, and filter out duplicates.
    for match in existing_matches:
        # Ensure the match object has the necessary keys to generate an ID.
        match_title = match.get('match')
        match_date = match.get('date')
        match_competition = match.get('competition')

        if not all([match_title, match_date, match_competition]):
            print(f"Warning: Skipping a malformed match object lacking required keys: {match}")
            malformed_entries += 1
            continue

        # Generate the unique ID for the current match.
        match_id = generate_match_id(match_title, match_date, match_competition)

        # Check if this ID has been seen before (i.e., it's a duplicate).
        if match_id in seen_ids:
            duplicates_found += 1
            print(f"Found duplicate, removing: '{match_title}' on '{match_date}'")
            continue
        
        # If it's a new, unique match:
        # - Add its ID to the set of seen IDs.
        # - Add the new 'match_id' key to the match object itself.
        # - Append the processed match to our list of unique matches.
        seen_ids.add(match_id)
        match['match_id'] = match_id
        unique_matches.append(match)

    # 4. Write the updated, clean data back to the original file.
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(unique_matches, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"Fatal Error: Could not write the updated data to '{file_path}': {e}")
        return

    # 5. Print a final summary of the operation.
    print("\n--- Processing Complete ---")
    print(f"Original matches count: {len(existing_matches)}")
    if malformed_entries > 0:
        print(f"Malformed entries skipped: {malformed_entries}")
    print(f"Duplicates found and removed: {duplicates_found}")
    print(f"Final unique matches count: {len(unique_matches)}")
    print(f"✓ The file '{file_path}' has been successfully updated.")

if __name__ == "__main__":
    # Define the file to be processed.
    json_file_to_update = "matches.json"
    process_existing_json(json_file_to_update)

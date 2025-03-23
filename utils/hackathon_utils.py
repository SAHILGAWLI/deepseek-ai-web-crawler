import csv
from typing import List, Set

from models.hackathon import Hackathon


def is_duplicate_hackathon(hackathon_name: str, seen_names: set) -> bool:
    """
    Check if a hackathon with the given name has already been processed.

    Args:
        hackathon_name (str): The name of the hackathon to check.
        seen_names (set): A set of hackathon names that have already been processed.

    Returns:
        bool: True if the hackathon is a duplicate, False otherwise.
    """
    return hackathon_name in seen_names


def is_complete_hackathon(hackathon: dict, required_keys: List[str]) -> bool:
    """
    Check if a hackathon has all the required keys.

    Args:
        hackathon (dict): The hackathon data to check.
        required_keys (List[str]): A list of required key names.

    Returns:
        bool: True if the hackathon has all required keys, False otherwise.
    """
    return all(key in hackathon and hackathon[key] is not None for key in required_keys)


def save_hackathons_to_csv(hackathons: List[dict], filename: str):
    """
    Save a list of hackathons to a CSV file.

    Args:
        hackathons (List[dict]): The list of hackathons to save.
        filename (str): The name of the CSV file to save to.
    """
    if not hackathons:
        print("No hackathons to save.")
        return

    # Use field names from the Hackathon model
    fieldnames = Hackathon.model_fields.keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hackathons)
    print(f"Saved {len(hackathons)} hackathons to '{filename}'.") 
import os
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv
import csv

load_dotenv()

def organization_search(api_key: str, payload: dict) -> dict:
    url = "https://api.apollo.io/api/v1/mixed_companies/search"

    dash = chr(45)
    headers = {
        "accept": "application/json",
        "content" + dash + "type": "application/json",
        "cache" + dash + "control": "no-cache",
        "x" + dash + "api" + dash + "key": api_key,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code == 401:
        raise RuntimeError(f"401 unauthorized. body: {response.text[:500]}")

    if response.status_code == 403:
        raise RuntimeError(f"403 forbidden. body: {response.text[:800]}")

    response.raise_for_status()
    return response.json()

def save_to_csv(data: list, filename: str) -> None:
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "Website", "LinkedIn", "Primary Phone", "Phone", "Languages"])
        for item in data:
            writer.writerow([
                item.get("name"),
                item.get("website_url"),
                item.get("linkedin_url"),
                item.get("primary_phone"),
                item.get("phone"),
                ", ".join(item.get("languages", []))
            ])

if __name__ == "__main__":
    api_key = os.getenv("APOLLO_API_KEY")

    payload = {
        "page": 1,
        "per_page": 100,
        "organization_locations": ["Brazil"],
        "organization_industries": ["agriculture", "logistics & supply chain", "retail"],
        "organization_num_employees_ranges": ["201,500", "501,1000", "1001,2000", "2001,5000", "5001,10000"],
    }

    data = organization_search(api_key, payload)
    organizations = data.get("organizations", [])
    
    save_to_csv(organizations, "apollo_organizations.csv")

        
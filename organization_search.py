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
    write_header = (not os.path.exists(filename)) or os.path.getsize(filename) == 0
    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(["Name", "Website", "LinkedIn", "Primary Phone", "Phone", "Languages", "Segment"])

        for item in data:
            org = item.get("organization") if isinstance(item, dict) else None
            if isinstance(org, dict):
                src = org
            else:
                src = item

            industries = src.get("industries")
            industry = src.get("industry")

            segment = industry
            if not segment and industries:
                if isinstance(industries, list):
                    names = []
                    for x in industries:
                        if isinstance(x, dict):
                            n = x.get("name") or x.get("label") or x.get("value")
                            if n:
                                names.append(n)
                        elif x:
                            names.append(str(x))
                    segment = ", ".join(names) if names else None
                else:
                    if isinstance(industries, dict):
                        segment = industries.get("name") or industries.get("label") or industries.get("value")
                    else:
                        segment = str(industries)

            langs = src.get("languages") or []
            if not isinstance(langs, list):
                langs = [str(langs)]

            writer.writerow([
                src.get("name"),
                src.get("website_url"),
                src.get("linkedin_url"),
                src.get("primary_phone"),
                src.get("phone"),
                ", ".join([str(x) for x in langs if x]),
                segment,
            ])

if __name__ == "__main__":
    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        raise RuntimeError("APOLLO_API_KEY nao encontrado no .env")

    page = 1
    per_page = 100
    total_salvo = 0

    while True:
        payload = {
            "page": page,
            "per_page": per_page,
            "organization_locations": ["Brazil"],
            "organization_industries": ["agriculture", "logistics & supply chain", "retail"],
            "organization_num_employees_ranges": ["201,500", "501,1000", "1001,2000", "2001,5000", "5001,10000"],
        }

        data = organization_search(api_key, payload)
        organizations = data.get("organizations", []) or []
        pagination = data.get("pagination", {}) or {}
        total_pages = pagination.get("total_pages")

        if not organizations:
            print(f"pagina={page} | sem resultados | encerrando")
            break

        save_to_csv(organizations, "apollo_organizations.csv")
        total_salvo += len(organizations)
        if total_pages:
            print(f"pagina={page}/{total_pages} | novos={len(organizations)} | total={total_salvo}")
        else:
            print(f"pagina={page} | novos={len(organizations)} | total={total_salvo}")

        if total_pages and page >= total_pages:
            break

        if len(organizations) < per_page:
            break

        page += 1

        

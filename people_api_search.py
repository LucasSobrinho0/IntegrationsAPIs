import os
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv
import csv
import time

load_dotenv()


# Pesquisa de usuários
def people_api_search(api_key: str, payload: dict) -> dict:
    url = "https://api.apollo.io/api/v1/mixed_people/api_search"

    dash = chr(45)
    headers = {
        "accept": "application/json",
        "content" + dash + "type": "application/json",
        "cache" + dash + "control": "no" + dash + "cache",
        "x" + dash + "api" + dash + "key": api_key,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        wait_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 10
        time.sleep(wait_s)
        return people_api_search(api_key, payload)

    if response.status_code in (401, 403, 422):
        raise RuntimeError(f"{response.status_code}. body: {response.text[:1200]}")

    response.raise_for_status()
    return response.json()

def domain_from_website(website_url: str) -> str | None:
    if not website_url:
        return None
    parsed = urlparse(website_url if "://" in website_url else "https://" + website_url)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None

def read_organizations_csv(path: str) -> list[dict]:
    orgs = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        _header = next(r, None)
        for row in r:
            if not row:
                continue
            name = (row[0] or "").strip() if len(row) > 0 else ""
            website = (row[1] or "").strip() if len(row) > 1 else ""
            if not name:
                continue
            domain = domain_from_website(website)
            orgs.append({"name": name, "website": website, "domain": domain})
    return orgs

def save_people_to_csv(rows: list[dict], filename: str) -> None:
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "organization_name",
            "organization_domain",
            "organization_website",
            "person_id",
            "first_name",
            "last_name_obfuscated",
            "title",
            "has_email",
            "has_direct_phone",
            "last_refreshed_at",
        ])
        for x in rows:
            w.writerow([
                x.get("organization_name"),
                x.get("organization_domain"),
                x.get("organization_website"),
                x.get("person_id"),
                x.get("first_name"),
                x.get("last_name_obfuscated"),
                x.get("title"),
                x.get("has_email"),
                x.get("has_direct_phone"),
                x.get("last_refreshed_at"),
            ])

def iter_people_pages(api_key: str, base_payload: dict, per_page: int = 25, max_pages: int = 10) -> list[dict]:
    all_people = []
    for page in range(1, max_pages + 1):
        payload = dict(base_payload)
        payload["per_page"] = per_page
        payload["page"] = page

        data = people_api_search(api_key, payload)
        people = data.get("people", []) or []
        all_people.extend(people)

        if len(people) < per_page:
            break

        time.sleep(1.2)
    return all_people

def build_payload_for_org(org_name: str, org_domain: str | None, person_titles: list[str]) -> dict:
    payload = {"person_titles": person_titles}
    if org_domain:
        payload["q_organization_domains_list"] = [org_domain]
    else:
        payload["q_organization_name"] = org_name
    return payload

if __name__ == "__main__":
    api_key = os.getenv("MASTER_API_KEY")
    if not api_key:
        raise RuntimeError("MASTER_API_KEY não encontrado no .env")

    person_titles = ["fp&a manager", "fpa manager", "fp and a manager", "Financial Supervisor", "IT Manager", ]

    orgs = read_organizations_csv("apollo_organizations.csv")

    out_rows = []
    for i, org in enumerate(orgs, start=1):
        org_name = org.get("name") or ""
        org_domain = org.get("domain")
        org_website = org.get("website") or ""

        base_payload = build_payload_for_org(org_name, org_domain, person_titles)

        people = iter_people_pages(api_key, base_payload, per_page=25, max_pages=10)

        for p in people:
            out_rows.append({
                "organization_name": org_name,
                "organization_domain": org_domain,
                "organization_website": org_website,
                "person_id": p.get("id"),
                "first_name": p.get("first_name"),
                "last_name_obfuscated": p.get("last_name_obfuscated"),
                "title": p.get("title"),
                "has_email": p.get("has_email"),
                "has_direct_phone": p.get("has_direct_phone"),
                "last_refreshed_at": p.get("last_refreshed_at"),
            })

        time.sleep(1.5)

    save_people_to_csv(out_rows, "apollo_people_fpa_manager.csv")
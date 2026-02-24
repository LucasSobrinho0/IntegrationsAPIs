import os
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()

def people_enrichment(api_key: str, payload: dict) -> dict:
    url = "https://api.apollo.io/api/v1/people/match"

    dash = chr(45)
    headers = {
        "accept": "application/json",
        "content" + dash + "type": "application/json",
        "cache" + dash + "control": "no" + dash + "cache",
        "x" + dash + "api" + dash + "key": api_key,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        wait_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 15
        time.sleep(wait_s)
        return people_enrichment(api_key, payload)

    if response.status_code in (401, 403, 422):
        raise RuntimeError(f"{response.status_code}. body: {response.text[:2000]}")

    response.raise_for_status()
    return response.json()

def read_first_n_people(path: str, n: int = 10) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            if i >= n:
                break
            rows.append(row)
    return rows

def pick_email(person: dict) -> str:
    for key in ("personal_emails", "emails"):
        v = person.get(key)
        if isinstance(v, list) and v:
            e0 = v[0]
            if isinstance(e0, dict):
                return e0.get("email") or ""
            if isinstance(e0, str):
                return e0
    for key in ("email", "work_email"):
        v = person.get(key)
        if isinstance(v, str) and v:
            return v
    return ""

def pick_phone(person: dict) -> str:
    for key in ("phone_numbers", "phones"):
        v = person.get(key)
        if isinstance(v, list):
            for ph in v:
                if not isinstance(ph, dict):
                    continue
                num = ph.get("sanitized_number") or ph.get("raw_number") or ph.get("number") or ""
                if num:
                    return num
    for key in ("direct_phone", "mobile_phone", "phone"):
        v = person.get(key)
        if isinstance(v, str) and v:
            return v
    return ""

def normalize_person(data: dict) -> dict:
    for k in ("person", "people", "match"):
        v = data.get(k)
        if isinstance(v, dict):
            return v
    return data if isinstance(data, dict) else {}

def save_enriched(rows: list[dict], filename: str) -> None:
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "organization_name",
            "person_id",
            "first_name",
            "last_name",
            "title",
            "linkedin_url",
            "email",
            "phone",
        ])
        for r in rows:
            w.writerow([
                r.get("organization_name"),
                r.get("person_id"),
                r.get("first_name"),
                r.get("last_name"),
                r.get("title"),
                r.get("linkedin_url"),
                r.get("email"),
                r.get("phone"),
            ])

if __name__ == "__main__":
    api_key = os.getenv("MASTER_API_KEY")
    if not api_key:
        raise RuntimeError("MASTER_API_KEY não encontrado no .env")

    base = read_first_n_people("apollo_people_fpa_manager.csv", n=10)

    enriched_rows = []

    for row in base:
        person_id = (row.get("person_id") or "").strip()
        if not person_id:
            continue

        payload = {
            "details": [{"id": person_id}],
            "reveal_personal_emails": True,
            "reveal_phone_number": True,
            # só mantenha os waterfall se você tiver webhook configurado
        }

        data = people_enrichment(api_key, payload)
        person = normalize_person(data)

        org_name = ""
        org = person.get("organization")
        if isinstance(org, dict):
            org_name = org.get("name") or ""

        enriched_rows.append({
            "organization_name": (row.get("organization_name") or org_name),
            "person_id": person.get("id") or person_id,
            "first_name": person.get("first_name") or (row.get("first_name") or ""),
            "last_name": person.get("last_name") or "",
            "title": person.get("title") or (row.get("title") or ""),
            "linkedin_url": person.get("linkedin_url") or "",
            "email": pick_email(person),
            "phone": pick_phone(person),
        })

        time.sleep(1.0)

    save_enriched(enriched_rows, "apollo_people_fpa_manager_enriched_10.csv")
    print("ok: apollo_people_fpa_manager_enriched_10.csv")
import csv
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()


def build_headers(api_key: str) -> dict:
    dash = chr(45)
    return {
        "accept": "application/json",
        "content" + dash + "type": "application/json",
        "cache" + dash + "control": "no" + dash + "cache",
        "x" + dash + "api" + dash + "key": api_key,
    }


def post_with_retry(
    url: str, headers: dict, payload: dict, params: dict | None = None, max_retries: int = 5
) -> dict:
    wait_s = 2.0
    for _ in range(max_retries):
        response = requests.post(url, headers=headers, params=params, json=payload, timeout=60)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            header_wait = int(retry_after) if (retry_after and retry_after.isdigit()) else None
            time.sleep(header_wait if header_wait is not None else wait_s)
            wait_s = min(wait_s * 2, 60)
            continue

        if response.status_code in (401, 403, 422):
            raise RuntimeError(f"{response.status_code}. body: {response.text[:2000]}")

        if response.status_code >= 400:
            raise RuntimeError(f"{response.status_code}. body: {response.text[:2000]}")

        return response.json()

    raise RuntimeError("429 persistente apos varias tentativas.")


def read_people(path: str, n: int | None = None) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            if n is not None and i >= n:
                break
            rows.append(row)
    return rows


def chunked(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


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


def normalize_people(data: dict) -> list[dict]:
    for key in ("people", "matches", "contacts"):
        v = data.get(key)
        if isinstance(v, list):
            return [p for p in v if isinstance(p, dict)]
    if isinstance(data.get("person"), dict):
        return [data["person"]]
    if isinstance(data, dict) and isinstance(data.get("id"), str):
        return [data]
    return []


def save_enriched(rows: list[dict], filename: str) -> None:
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_name", "company_name", "title", "email", "phone"])
        for r in rows:
            full_name = " ".join(
                [(r.get("first_name") or "").strip(), (r.get("last_name") or "").strip()]
            ).strip()
            w.writerow(
                [full_name, r.get("organization_name"), r.get("title"), r.get("email"), r.get("phone")]
            )


if __name__ == "__main__":
    api_key = os.getenv("MASTER_API_KEY")
    if not api_key:
        raise RuntimeError("MASTER_API_KEY nao encontrado no .env")

    input_csv = os.getenv("INPUT_CSV", "apollo_people_fpa_manager.csv")
    output_csv = os.getenv("OUTPUT_CSV", "apollo_people_fpa_manager_enriched.csv")
    max_rows_env = (os.getenv("MAX_ROWS") or "").strip()
    max_rows = int(max_rows_env) if max_rows_env.isdigit() else None
    delay_s_env = (os.getenv("BATCH_DELAY_SECONDS") or "").strip()
    delay_s = float(delay_s_env) if delay_s_env else 2.0
    webhook_url = (os.getenv("WEBHOOK_URL") or "").strip()

    base = read_people(input_csv, n=max_rows)
    unique_rows_by_id = {}
    for row in base:
        person_id = (row.get("person_id") or "").strip()
        if person_id and person_id not in unique_rows_by_id:
            unique_rows_by_id[person_id] = row
    base = list(unique_rows_by_id.values())

    headers = build_headers(api_key)
    url_bulk = "https://api.apollo.io/api/v1/people/bulk_match"
    enriched_rows = []

    for batch in chunked(base, 10):
        details = []
        source_by_id = {}
        for row in batch:
            person_id = (row.get("person_id") or "").strip()
            if not person_id:
                continue
            details.append({"id": person_id})
            source_by_id[person_id] = row

        if not details:
            continue

        payload = {"details": details}
        params = {"reveal_personal_emails": "true"}
        if webhook_url:
            params["reveal_phone_number"] = "true"
            params["webhook_url"] = webhook_url

        data = post_with_retry(url_bulk, headers, payload, params=params, max_retries=5)
        people = normalize_people(data)

        for person in people:
            person_id = (person.get("id") or "").strip()
            src = source_by_id.get(person_id, {})

            org_name = ""
            org = person.get("organization")
            if isinstance(org, dict):
                org_name = org.get("name") or ""

            enriched_rows.append(
                {
                    "organization_name": src.get("organization_name") or org_name,
                    "first_name": person.get("first_name") or src.get("first_name") or "",
                    "last_name": person.get("last_name") or "",
                    "title": person.get("title") or src.get("title") or "",
                    "email": pick_email(person),
                    "phone": pick_phone(person),
                }
            )

        print(f"lote processado: {len(details)} ids | retornados: {len(people)}")
        time.sleep(delay_s)

    save_enriched(enriched_rows, output_csv)
    print(f"ok: {output_csv} | pessoas enriquecidas: {len(enriched_rows)}")

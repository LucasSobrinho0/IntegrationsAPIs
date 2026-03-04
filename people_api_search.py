import os
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv
import csv
import time

load_dotenv()
DEBUG = (os.getenv("DEBUG_API_SEARCH") or "").strip().lower() in {"1", "true", "yes", "on"}

CSV_HEADERS = [
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
]


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

    if DEBUG:
        print(
            "[DEBUG] POST mixed_people/api_search | "
            f"page={payload.get('page')} | per_page={payload.get('per_page')} | "
            f"domain={payload.get('q_organization_domains_list')} | "
            f"name={payload.get('q_organization_name')} | "
            f"titles={payload.get('person_titles')}",
            flush=True,
        )

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if DEBUG:
        print(f"[DEBUG] HTTP status={response.status_code} | page={payload.get('page')}", flush=True)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        wait_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 10
        print(f"[INFO] 429 rate limit | aguardando {wait_s}s", flush=True)
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
        header_line = f.readline()
        f.seek(0)
        delimiter = "\t" if "\t" in header_line else ","
        r = csv.DictReader(f, delimiter=delimiter)
        for row in r:
            if not row:
                continue
            name = (row.get("Name") or row.get("name") or "").strip()
            website = (row.get("Website") or row.get("website") or "").strip()
            linkedin = (row.get("LinkedIn") or row.get("linkedin") or "").strip()
            if not name:
                continue
            domain = domain_from_website(website)
            orgs.append({
                "name": name,
                "website": website,
                "linkedin": linkedin,
                "domain": domain,
            })
    return orgs

def save_people_to_csv(rows: list[dict], filename: str) -> None:
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADERS)
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

def init_people_csv(filename: str) -> None:
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADERS)

def append_people_to_csv(rows: list[dict], filename: str) -> None:
    if not rows:
        return
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
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
        if DEBUG:
            pagination = data.get("pagination") or {}
            print(
                f"[DEBUG] page={page} | people_na_pagina={len(people)} | "
                f"total_acumulado={len(all_people)} | pagination={pagination}"
            )

        if len(people) < per_page:
            if DEBUG:
                print(f"[DEBUG] encerrando paginação na page={page} (retornou < {per_page})")
            break

        time.sleep(1.2)
    return all_people

def build_payload_for_org(org_name: str, org_domain: str | None, person_titles: list[str]) -> dict:
    payload = {"person_titles": person_titles, "contact_email_status": "verified"}
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
    output_file = "apollo_people_fpa_manager.csv"
    init_people_csv(output_file)
    total_empresas = len(orgs)
    total_registros_salvos = 0
    print(
        f"inicio | empresas={total_empresas} | output={output_file} | "
        f"debug={'on' if DEBUG else 'off'}",
        flush=True,
    )

    for i, org in enumerate(orgs, start=1):
        org_name = org.get("name") or ""
        org_domain = org.get("domain")
        org_website = org.get("website") or ""

        base_payload = build_payload_for_org(org_name, org_domain, person_titles)
        print(
            f"[{i}/{total_empresas}] consultando | empresa={org_name} | domain={org_domain or '-'}",
            flush=True,
        )
        if DEBUG:
            print(
                f"[DEBUG] empresa {i}/{total_empresas} | nome={org_name} | "
                f"domain={org_domain} | website={org_website}",
                flush=True,
            )

        try:
            people = iter_people_pages(api_key, base_payload, per_page=25, max_pages=10)
        except Exception as err:
            print(f"[ERRO] empresa={org_name} | detalhe={str(err)[:1200]}")
            time.sleep(1.5)
            continue
        org_rows = []

        for p in people:
            org_rows.append({
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

        append_people_to_csv(org_rows, output_file)
        total_registros_salvos += len(org_rows)
        status_busca = "ok" if org_rows else "sem_resultado"
        person_id_exemplo = org_rows[0].get("person_id") if org_rows else ""
        print(
            f"[{i}/{total_empresas}] salvo em {output_file} | "
            f"empresa={org_name} | novos={len(org_rows)} | total={total_registros_salvos} | "
            f"person_id={person_id_exemplo} | status={status_busca}",
            flush=True,
        )
        time.sleep(1.5)

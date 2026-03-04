from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

try:
    from hubspot.hubspot_company import (
        CompanyPayload,
        create_or_get_company,
        get_hubspot_api_key,
    )
    from hubspot.hubspot_contact import ContactPayload, create_or_get_contact
except ModuleNotFoundError:
    from hubspot_company import CompanyPayload, create_or_get_company, get_hubspot_api_key
    from hubspot_contact import ContactPayload, create_or_get_contact

load_dotenv()

HUBSPOT_API_BASE_URL = "https://api.hubapi.com"
DEALS_OBJECT_PATH = "/crm/v3/objects/deals"
DEAL_PIPELINES_PATH = "/crm/v3/pipelines/deals"
OWNERS_PATH = "/crm/v3/owners/"
DEFAULT_TIMEOUT_SECONDS = 30
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
DEFAULT_DEAL_CSV_PATH = PROJECT_ROOT / "deal.csv"

# HubSpot-defined associations according to CRM associations reference.
DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID = 3
DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID = 5


class HubSpotApiError(RuntimeError):
    """Raised when HubSpot returns a non-success response."""


@dataclass(slots=True, frozen=True)
class DealExecutionResult:
    """Summary for one CSV line processed."""

    company_id: str
    contact_id: str
    deal_id: str
    company_created: bool
    contact_created: bool
    pipeline_id: str
    stage_id: str
    owner_email: str | None = None
    owner_id: str | None = None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_headers(api_key: str) -> dict[str, str]:
    token = _clean_text(api_key)
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _request(
    method: str,
    path: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    response = requests.request(
        method=method,
        url=f"{HUBSPOT_API_BASE_URL}{path}",
        headers=_build_headers(api_key),
        json=payload,
        timeout=timeout_seconds,
    )
    if response.status_code == 401:
        raise HubSpotApiError(
            "HubSpot retornou 401 (nao autenticado). Verifique HUBSPOT_API_KEY "
            "com um Private App Token valido."
        )
    if response.status_code >= 400:
        raise HubSpotApiError(
            f"HubSpot erro {response.status_code} em {path}. body={response.text[:2000]}"
        )
    return response.json() if response.text else {}


def resolve_owner_id_by_email(api_key: str, owner_email: str) -> str:
    """
    Resolves a HubSpot owner ID from e-mail.

    HubSpot expects owner assignment through `hubspot_owner_id`, not e-mail.
    """
    email_clean = _clean_text(owner_email).lower()
    if not email_clean:
        raise ValueError("E-mail do owner vazio ao tentar resolver owner id.")

    response = requests.get(
        url=f"{HUBSPOT_API_BASE_URL}{OWNERS_PATH}",
        headers=_build_headers(api_key),
        params={"email": email_clean, "limit": 1},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        raise HubSpotApiError(
            "HubSpot retornou 401 ao consultar owners. Verifique HUBSPOT_API_KEY."
        )
    if response.status_code >= 400:
        raise HubSpotApiError(
            f"HubSpot erro {response.status_code} em {OWNERS_PATH}. body={response.text[:2000]}"
        )

    results = (response.json() or {}).get("results") or []
    if not results:
        raise RuntimeError(f"Nenhum owner encontrado no HubSpot para o e-mail: {email_clean}")

    owner = next(
        (item for item in results if _clean_text(item.get("email")).lower() == email_clean),
        results[0],
    )
    owner_id = _clean_text(owner.get("id"))
    if not owner_id:
        raise RuntimeError(f"Owner encontrado sem id para e-mail: {email_clean}")
    return owner_id


def resolve_deal_owner_email(row: dict[str, str]) -> str:
    """
    Owner e-mail source priority:
    1) CSV columns
    2) HUBSPOT_DEAL_OWNER_EMAIL in env
    """
    candidate_columns = [
        "deal_owner_email",
        "hubspot_owner_email",
        "owner_email",
        "responsavel_email",
        "responsible_email",
    ]
    for column in candidate_columns:
        value = _clean_text(row.get(column))
        if value:
            return value
    return _clean_text(os.getenv("HUBSPOT_DEAL_OWNER_EMAIL"))


def _detect_csv_delimiter(csv_path: str) -> str:
    with open(csv_path, mode="r", encoding="utf-8-sig", newline="") as file:
        sample = file.read(4096)
    return "\t" if sample.count("\t") >= sample.count(",") else ","


def resolve_csv_path(csv_path: str | os.PathLike[str] | None = None) -> Path:
    """
    Resolves CSV path robustly for different execution directories.
    Priority:
    1) absolute path
    2) current working directory
    3) directory of this file (hubspot/)
    4) project root (parent of hubspot/)
    """
    provided = str(csv_path).strip() if csv_path is not None else ""
    relative_name = provided or "deal.csv"
    path_obj = Path(relative_name)

    candidates: list[Path] = []
    if path_obj.is_absolute():
        candidates.append(path_obj)
    else:
        candidates.append(Path.cwd() / path_obj)
        candidates.append(CURRENT_DIR / path_obj)
        candidates.append(PROJECT_ROOT / path_obj)
        if not provided:
            candidates.append(DEFAULT_DEAL_CSV_PATH)

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()

    searched_paths = " | ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Arquivo CSV nao encontrado. Tentativas: {searched_paths}")


def read_first_valid_row(csv_path: str | os.PathLike[str] | None = None) -> dict[str, str]:
    """
    Reads only the first non-empty line of deal.csv.
    This guarantees "only one deal" per execution.
    """
    resolved_csv_path = resolve_csv_path(csv_path)
    delimiter = _detect_csv_delimiter(str(resolved_csv_path))
    with open(resolved_csv_path, mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, delimiter=delimiter)
        for row in reader:
            normalized = {
                _clean_text(column): _clean_text(value)
                for column, value in row.items()
                if _clean_text(column)
            }
            if any(normalized.values()):
                return normalized
    raise RuntimeError(f"Nenhuma linha valida encontrada em {resolved_csv_path}.")


def read_all_valid_rows(csv_path: str | os.PathLike[str] | None = None) -> list[dict[str, str]]:
    """Reads all non-empty lines from deal.csv."""
    resolved_csv_path = resolve_csv_path(csv_path)
    delimiter = _detect_csv_delimiter(str(resolved_csv_path))
    rows: list[dict[str, str]] = []

    with open(resolved_csv_path, mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, delimiter=delimiter)
        for row in reader:
            normalized = {
                _clean_text(column): _clean_text(value)
                for column, value in row.items()
                if _clean_text(column)
            }
            if any(normalized.values()):
                rows.append(normalized)

    if not rows:
        raise RuntimeError(f"Nenhuma linha valida encontrada em {resolved_csv_path}.")
    return rows


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def resolve_deal_pipeline_and_stage(api_key: str) -> tuple[str, str]:
    """
    Resolves pipeline/deal stage.
    Priority: env vars -> first available pipeline/stage from HubSpot.
    """
    pipeline_from_env = _clean_text(os.getenv("HUBSPOT_DEAL_PIPELINE_ID"))
    stage_from_env = _clean_text(os.getenv("HUBSPOT_DEAL_STAGE_ID"))

    if pipeline_from_env and stage_from_env:
        return pipeline_from_env, stage_from_env

    response = _request("GET", DEAL_PIPELINES_PATH, api_key)
    pipelines = response.get("results") or []
    if not pipelines:
        raise RuntimeError("Nenhum pipeline de deal retornado pela API do HubSpot.")

    selected_pipeline: dict[str, Any] | None = None
    if pipeline_from_env:
        selected_pipeline = next(
            (pipe for pipe in pipelines if _clean_text(pipe.get("id")) == pipeline_from_env),
            None,
        )
        if not selected_pipeline:
            raise RuntimeError(
                f"Pipeline informado em HUBSPOT_DEAL_PIPELINE_ID nao encontrado: {pipeline_from_env}"
            )
    else:
        selected_pipeline = next(
            (pipe for pipe in pipelines if pipe.get("stages")),
            pipelines[0],
        )

    pipeline_id = pipeline_from_env or _clean_text(selected_pipeline.get("id"))
    if not pipeline_id:
        raise RuntimeError("Pipeline selecionado sem id.")

    if stage_from_env:
        return pipeline_id, stage_from_env

    stages = selected_pipeline.get("stages") or []
    if not stages:
        raise RuntimeError(f"Pipeline {pipeline_id} nao possui stages.")

    stages_sorted = sorted(stages, key=lambda stage: _safe_int(stage.get("displayOrder")))
    stage_id = _clean_text(stages_sorted[0].get("id"))
    if not stage_id:
        raise RuntimeError(f"Nao foi possivel determinar um stage para o pipeline {pipeline_id}.")

    return pipeline_id, stage_id


def build_company_payload_from_row(row: dict[str, str]) -> CompanyPayload:
    company_name = _clean_text(row.get("empresa")) or "Empresa sem nome"
    website = _clean_text(row.get("website")) or None
    return CompanyPayload(name=company_name, website=website)


def build_contact_payload_from_row(row: dict[str, str]) -> ContactPayload:
    return ContactPayload(
        full_name=_clean_text(row.get("nome_pessoa")) or None,
        email=_clean_text(row.get("email")) or None,
        phone=_clean_text(row.get("telefone")) or None,
        job_title=_clean_text(row.get("cargo")) or None,
        company_name=_clean_text(row.get("empresa")) or None,
        website=_clean_text(row.get("website")) or None,
    )


def build_deal_properties(
    row: dict[str, str],
    pipeline_id: str,
    stage_id: str,
    owner_id: str = "",
) -> dict[str, Any]:
    company_name = _clean_text(row.get("empresa")) or "Empresa sem nome"
    person_name = _clean_text(row.get("nome_pessoa"))

    deal_name = _clean_text(row.get("dealname"))
    if not deal_name:
        deal_name = f"Deal - {company_name}"
        if person_name:
            deal_name = f"{deal_name} - {person_name}"

    properties: dict[str, Any] = {
        "dealname": deal_name,
        "pipeline": pipeline_id,
        "dealstage": stage_id,
    }
    owner_id_clean = _clean_text(owner_id)
    if owner_id_clean:
        properties["hubspot_owner_id"] = owner_id_clean

    amount = _clean_text(row.get("amount") or row.get("valor") or row.get("deal_amount"))
    if amount:
        properties["amount"] = amount

    description_parts = [
        f"Empresa: {company_name}",
        f"Contato: {person_name}" if person_name else "",
        f"Cargo: {_clean_text(row.get('cargo'))}" if _clean_text(row.get("cargo")) else "",
        f"E-mail: {_clean_text(row.get('email'))}" if _clean_text(row.get("email")) else "",
    ]
    description = " | ".join(part for part in description_parts if part)
    if description:
        properties["description"] = description

    return properties


def create_deal(
    api_key: str,
    properties: dict[str, Any],
    company_id: str,
    contact_id: str,
) -> dict[str, Any]:
    associations = [
        {
            "to": {"id": contact_id},
            "types": [
                {
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID,
                }
            ],
        },
        {
            "to": {"id": company_id},
            "types": [
                {
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID,
                }
            ],
        },
    ]
    payload = {"properties": properties, "associations": associations}
    return _request("POST", DEALS_OBJECT_PATH, api_key, payload)


def create_single_deal_from_row(
    row: dict[str, str],
    api_key: str,
    pipeline_id: str,
    stage_id: str,
) -> DealExecutionResult:
    """Creates company/contact/deal for one CSV row."""
    owner_email = resolve_deal_owner_email(row)
    owner_id = resolve_owner_id_by_email(api_key, owner_email) if owner_email else ""

    company_result = create_or_get_company(api_key, build_company_payload_from_row(row))
    contact_result = create_or_get_contact(
        api_key,
        build_contact_payload_from_row(row),
        company_id=company_result.id,
    )

    deal_properties = build_deal_properties(
        row=row,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        owner_id=owner_id,
    )
    deal_response = create_deal(
        api_key=api_key,
        properties=deal_properties,
        company_id=company_result.id,
        contact_id=contact_result.id,
    )

    deal_id = _clean_text(deal_response.get("id"))
    if not deal_id:
        raise HubSpotApiError("Resposta de criacao de deal sem 'id'.")

    return DealExecutionResult(
        company_id=company_result.id,
        contact_id=contact_result.id,
        deal_id=deal_id,
        company_created=company_result.created,
        contact_created=contact_result.created,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        owner_email=owner_email or None,
        owner_id=owner_id or None,
    )


def create_single_deal_from_csv(
    csv_path: str | os.PathLike[str] | None = None,
) -> DealExecutionResult:
    """
    Orchestrates 1-line flow:
    1) Create/get company
    2) Create/get contact
    3) Create one deal associated to both records
    """
    api_key = get_hubspot_api_key()
    row = read_first_valid_row(csv_path)
    pipeline_id, stage_id = resolve_deal_pipeline_and_stage(api_key)
    return create_single_deal_from_row(
        row=row,
        api_key=api_key,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
    )


def create_all_deals_from_csv(csv_path: str | os.PathLike[str] | None = None) -> list[DealExecutionResult]:
    """
    Processes all valid lines from CSV until the end.
    If one line fails, continues with the next.
    """
    api_key = get_hubspot_api_key()
    rows = read_all_valid_rows(csv_path)
    pipeline_id, stage_id = resolve_deal_pipeline_and_stage(api_key)
    results: list[DealExecutionResult] = []

    total = len(rows)
    for index, row in enumerate(rows, start=1):
        try:
            result = create_single_deal_from_row(
                row=row,
                api_key=api_key,
                pipeline_id=pipeline_id,
                stage_id=stage_id,
            )
            results.append(result)
            print(
                f"[{index}/{total}] ok | company_id={result.company_id} | "
                f"contact_id={result.contact_id} | deal_id={result.deal_id}"
            )
        except Exception as err:
            print(f"[{index}/{total}] erro | detalhe={str(err)[:1000]}")

    return results


if __name__ == "__main__":
    results = create_all_deals_from_csv()
    print(f"Fluxo concluido. Deals criados com sucesso: {len(results)}")

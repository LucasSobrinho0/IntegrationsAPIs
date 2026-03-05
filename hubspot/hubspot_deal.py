from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import unicodedata

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
HARDCODED_STAGE_ID = "appointmentscheduled"
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
    contact_ids: list[str]
    deal_id: str
    company_created: bool
    contacts_created_count: int
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
    Owner e-mail fixed from HUBSPOT_DEAL_OWNER_EMAIL in env.
    """
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


def _normalize_text(value: Any) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def resolve_deal_pipeline_and_stage(api_key: str) -> tuple[str, str]:
    """
    Resolves pipeline/stage by env IDs or hardcoded stage id.
    Hardcoded default stage: appointmentscheduled (label shown as Prospeção).
    """
    pipeline_from_env = _clean_text(os.getenv("HUBSPOT_DEAL_PIPELINE_ID"))
    stage_from_env = _clean_text(os.getenv("HUBSPOT_DEAL_STAGE_ID")) or HARDCODED_STAGE_ID
    pipeline_label_from_env = _clean_text(os.getenv("HUBSPOT_DEAL_PIPELINE_LABEL")) or "Prospeção"
    stage_label_from_env = _clean_text(os.getenv("HUBSPOT_DEAL_STAGE_LABEL")) or "Reaquecer"

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
    elif stage_from_env:
        selected_pipeline = next(
            (
                pipe
                for pipe in pipelines
                if any(_clean_text(stage.get("id")) == stage_from_env for stage in (pipe.get("stages") or []))
            ),
            None,
        )
        if not selected_pipeline:
            raise RuntimeError(
                "Nao foi encontrado pipeline para o stage hardcoded/configurado. "
                f"stage_id={stage_from_env}"
            )
    else:
        target_pipeline_normalized = _normalize_text(pipeline_label_from_env)
        selected_pipeline = next(
            (
                pipe
                for pipe in pipelines
                if _normalize_text(pipe.get("label")) == target_pipeline_normalized
                or _normalize_text(pipe.get("id")) == target_pipeline_normalized
            ),
            None,
        )
        if not selected_pipeline:
            raise RuntimeError(
                "Pipeline de deals nao encontrado por nome. "
                f"Esperado: {pipeline_label_from_env}"
            )

    pipeline_id = pipeline_from_env or _clean_text(selected_pipeline.get("id"))
    if not pipeline_id:
        raise RuntimeError("Pipeline selecionado sem id.")

    if stage_from_env:
        return pipeline_id, stage_from_env

    stages = selected_pipeline.get("stages") or []
    if not stages:
        raise RuntimeError(f"Pipeline {pipeline_id} nao possui stages.")

    target_stage_normalized = _normalize_text(stage_label_from_env)
    matched_stage = next(
        (
            stage
            for stage in stages
            if _normalize_text(stage.get("label")) == target_stage_normalized
            or _normalize_text(stage.get("id")) == target_stage_normalized
        ),
        None,
    )
    if not matched_stage:
        raise RuntimeError(
            "Stage de deal nao encontrado por nome no pipeline selecionado. "
            f"Pipeline={pipeline_id} | stage esperado={stage_label_from_env}"
        )

    stage_id = _clean_text(matched_stage.get("id"))
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
    contacts_rows: list[dict[str, str]],
    pipeline_id: str,
    stage_id: str,
    owner_id: str = "",
) -> dict[str, Any]:
    company_name = _clean_text(row.get("empresa")) or "Empresa sem nome"
    deal_name = company_name

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

    contacts_description: list[str] = []
    for contact_row in contacts_rows:
        contact_name = _clean_text(contact_row.get("nome_pessoa"))
        contact_email = _clean_text(contact_row.get("email"))
        contact_phone = _clean_text(contact_row.get("telefone"))

        detail_parts = [part for part in [contact_name, contact_email, contact_phone] if part]
        if detail_parts:
            contacts_description.append(" / ".join(detail_parts))

    description_parts = [f"Empresa: {company_name}"]
    if contacts_description:
        description_parts.append(f"Contatos: {' ; '.join(contacts_description)}")

    description = " | ".join(description_parts)
    if description:
        properties["description"] = description

    return properties


def create_deal(
    api_key: str,
    properties: dict[str, Any],
    company_id: str,
    contact_ids: list[str],
) -> dict[str, Any]:
    associations = []
    for contact_id in contact_ids:
        associations.append(
            {
                "to": {"id": contact_id},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID,
                    }
                ],
            }
        )
    associations.append(
        {
            "to": {"id": company_id},
            "types": [
                {
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID,
                }
            ],
        }
    )
    payload = {"properties": properties, "associations": associations}
    return _request("POST", DEALS_OBJECT_PATH, api_key, payload)


def create_single_deal_from_company_rows(
    company_rows: list[dict[str, str]],
    api_key: str,
    pipeline_id: str,
    stage_id: str,
) -> DealExecutionResult:
    """Creates company + all contacts + one deal for a single company."""
    if not company_rows:
        raise ValueError("Lista de linhas da empresa vazia.")

    row = company_rows[0]
    owner_email = resolve_deal_owner_email(row)
    owner_id = resolve_owner_id_by_email(api_key, owner_email) if owner_email else ""

    company_result = create_or_get_company(api_key, build_company_payload_from_row(row))

    contact_ids: list[str] = []
    contacts_created_count = 0
    for company_row in company_rows:
        contact_result = create_or_get_contact(
            api_key,
            build_contact_payload_from_row(company_row),
            company_id=company_result.id,
        )
        if contact_result.created:
            contacts_created_count += 1
        if contact_result.id not in contact_ids:
            contact_ids.append(contact_result.id)

    if not contact_ids:
        raise RuntimeError("Nenhum contato valido encontrado para associar ao deal.")

    deal_properties = build_deal_properties(
        row=row,
        contacts_rows=company_rows,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        owner_id=owner_id,
    )
    deal_response = create_deal(
        api_key=api_key,
        properties=deal_properties,
        company_id=company_result.id,
        contact_ids=contact_ids,
    )

    deal_id = _clean_text(deal_response.get("id"))
    if not deal_id:
        raise HubSpotApiError("Resposta de criacao de deal sem 'id'.")

    return DealExecutionResult(
        company_id=company_result.id,
        contact_ids=contact_ids,
        deal_id=deal_id,
        company_created=company_result.created,
        contacts_created_count=contacts_created_count,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        owner_email=owner_email or None,
        owner_id=owner_id or None,
    )


def _group_rows_by_company(rows: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        company_key = _clean_text(row.get("empresa")).lower() or "__empresa_sem_nome__"
        grouped.setdefault(company_key, []).append(row)
    return list(grouped.values())


def create_first_company_deal_from_csv(
    csv_path: str | os.PathLike[str] | None = None,
) -> DealExecutionResult:
    """
    Orchestrates 1-company flow:
    1) Create/get company
    2) Create/get all contacts from that company rows
    3) Create one deal associated to company + all contacts
    """
    api_key = get_hubspot_api_key()
    rows = read_all_valid_rows(csv_path)
    groups = _group_rows_by_company(rows)
    if not groups:
        raise RuntimeError("Nenhuma empresa valida encontrada no CSV.")

    pipeline_id, stage_id = resolve_deal_pipeline_and_stage(api_key)
    return create_single_deal_from_company_rows(
        company_rows=groups[0],
        api_key=api_key,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
    )


def create_all_deals_from_csv(csv_path: str | os.PathLike[str] | None = None) -> list[DealExecutionResult]:
    """
    Processes all companies from CSV (one deal per company).
    If one company fails, continues with the next.
    """
    api_key = get_hubspot_api_key()
    rows = read_all_valid_rows(csv_path)
    grouped_rows = _group_rows_by_company(rows)
    pipeline_id, stage_id = resolve_deal_pipeline_and_stage(api_key)
    results: list[DealExecutionResult] = []

    total = len(grouped_rows)
    for index, company_rows in enumerate(grouped_rows, start=1):
        try:
            result = create_single_deal_from_company_rows(
                company_rows=company_rows,
                api_key=api_key,
                pipeline_id=pipeline_id,
                stage_id=stage_id,
            )
            results.append(result)
            print(
                f"[{index}/{total}] ok | company_id={result.company_id} | "
                f"contacts={len(result.contact_ids)} | deal_id={result.deal_id}"
            )
        except Exception as err:
            print(f"[{index}/{total}] erro | detalhe={str(err)[:1000]}")

    return results


if __name__ == "__main__":
    results = create_all_deals_from_csv()
    print(f"Fluxo concluido. Deals criados com sucesso: {len(results)}")

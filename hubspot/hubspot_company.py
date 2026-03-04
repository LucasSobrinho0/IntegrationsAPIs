from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

HUBSPOT_API_BASE_URL = "https://api.hubapi.com"
COMPANIES_OBJECT_PATH = "/crm/v3/objects/companies"
COMPANIES_SEARCH_PATH = "/crm/v3/objects/companies/search"
DEFAULT_TIMEOUT_SECONDS = 30


class HubSpotApiError(RuntimeError):
    """Raised when HubSpot returns a non-success response."""


@dataclass(slots=True, frozen=True)
class CompanyPayload:
    """Normalized payload for company creation."""

    name: str
    website: str | None = None
    domain: str | None = None
    phone: str | None = None
    city: str | None = None
    state: str | None = None
    additional_properties: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class UpsertCompanyResult:
    """Result returned by the create-or-get flow."""

    id: str
    created: bool
    response: dict[str, Any]


def get_hubspot_api_key(env_var: str = "HUBSPOT_API_KEY") -> str:
    """
    Reads the HubSpot token from environment.

    The variable name requested by the project is HUBSPOT_API_KEY.
    """
    api_key = (os.getenv(env_var) or "").strip()
    if not api_key:
        raise RuntimeError(f"{env_var} nao encontrado no ambiente.")
    return api_key


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


def extract_domain_from_website(website: str | None) -> str:
    """Extracts a normalized domain from a website URL."""
    website_clean = _clean_text(website)
    if not website_clean:
        return ""

    parsed = urlparse(website_clean if "://" in website_clean else f"https://{website_clean}")
    host = (parsed.netloc or parsed.path).lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host.split("/")[0].strip()


def build_company_properties(company: CompanyPayload) -> dict[str, Any]:
    """Builds a valid HubSpot properties object for companies."""
    properties: dict[str, Any] = {}

    name = _clean_text(company.name)
    website = _clean_text(company.website)
    domain = _clean_text(company.domain).lower() or extract_domain_from_website(website)

    if name:
        properties["name"] = name
    if domain:
        properties["domain"] = domain
    if website:
        properties["website"] = website

    phone = _clean_text(company.phone)
    city = _clean_text(company.city)
    state = _clean_text(company.state)

    if phone:
        properties["phone"] = phone
    if city:
        properties["city"] = city
    if state:
        properties["state"] = state

    if company.additional_properties:
        for key, value in company.additional_properties.items():
            key_clean = _clean_text(key)
            value_clean = _clean_text(value)
            if key_clean and value_clean:
                properties[key_clean] = value_clean

    # HubSpot recomenda enviar ao menos name ou domain para evitar duplicidade.
    if "name" not in properties and "domain" not in properties:
        raise ValueError("CompanyPayload invalido: envie ao menos name ou domain.")

    return properties


def search_company_by_domain_or_name(
    api_key: str,
    *,
    domain: str = "",
    name: str = "",
) -> dict[str, Any] | None:
    """Searches an existing company to avoid creating duplicates."""
    search_filters: list[dict[str, str]] = []
    domain_clean = _clean_text(domain).lower()
    name_clean = _clean_text(name)

    if domain_clean:
        search_filters.append({"propertyName": "domain", "operator": "EQ", "value": domain_clean})
    if name_clean:
        search_filters.append({"propertyName": "name", "operator": "EQ", "value": name_clean})

    for filter_item in search_filters:
        payload = {
            "filterGroups": [{"filters": [filter_item]}],
            "properties": ["name", "domain", "website"],
            "limit": 1,
        }
        response = _request("POST", COMPANIES_SEARCH_PATH, api_key, payload)
        results = response.get("results") or []
        if results:
            return results[0]

    return None


def create_company(api_key: str, company: CompanyPayload) -> dict[str, Any]:
    """Creates a company on HubSpot CRM."""
    payload = {"properties": build_company_properties(company)}
    return _request("POST", COMPANIES_OBJECT_PATH, api_key, payload)


def create_or_get_company(api_key: str, company: CompanyPayload) -> UpsertCompanyResult:
    """
    Retrieves an existing company (by domain/name) or creates a new one.
    """
    properties = build_company_properties(company)
    existing = search_company_by_domain_or_name(
        api_key,
        domain=properties.get("domain", ""),
        name=properties.get("name", ""),
    )
    if existing:
        existing_id = _clean_text(existing.get("id"))
        if not existing_id:
            raise HubSpotApiError("Empresa encontrada sem 'id' na resposta da busca.")
        return UpsertCompanyResult(id=existing_id, created=False, response=existing)

    created = create_company(api_key, company)
    created_id = _clean_text(created.get("id"))
    if not created_id:
        raise HubSpotApiError("Resposta de criacao de empresa sem 'id'.")
    return UpsertCompanyResult(id=created_id, created=True, response=created)

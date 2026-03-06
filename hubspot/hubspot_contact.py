from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests

HUBSPOT_API_BASE_URL = "https://api.hubapi.com"
CONTACTS_OBJECT_PATH = "/crm/v3/objects/contacts"
CONTACTS_SEARCH_PATH = "/crm/v3/objects/contacts/search"
DEFAULT_TIMEOUT_SECONDS = 30

# HubSpot-defined association (Contact -> Company), conforme docs de CRM associations.
CONTACT_TO_COMPANY_ASSOCIATION_TYPE_ID = 279


class HubSpotApiError(RuntimeError):
    """Raised when HubSpot returns a non-success response."""


@dataclass(slots=True, frozen=True)
class ContactPayload:
    """Normalized payload for contact creation."""

    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    mobile_phone: str | None = None
    job_title: str | None = None
    company_name: str | None = None
    website: str | None = None
    additional_properties: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class UpsertContactResult:
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


def split_full_name(full_name: str | None) -> tuple[str, str]:
    """Converts full name into first name + last name."""
    normalized = _clean_text(full_name)
    if not normalized:
        return "", ""

    parts = [part for part in normalized.split() if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def split_phone_numbers(raw_phone: str | None) -> tuple[str, str]:
    """
    Splits phone string into (phone, mobilephone) when multiple numbers are present.
    Example: "A | B" -> ("A", "B")
    """
    normalized = _clean_text(raw_phone)
    if not normalized:
        return "", ""

    parts = [_clean_text(part) for part in normalized.split("|") if _clean_text(part)]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


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


def build_contact_properties(contact: ContactPayload) -> dict[str, Any]:
    """Builds a valid HubSpot properties object for contacts."""
    properties: dict[str, Any] = {}

    first_name = _clean_text(contact.first_name)
    last_name = _clean_text(contact.last_name)
    if not first_name and not last_name:
        first_name, last_name = split_full_name(contact.full_name)

    email = _clean_text(contact.email).lower()
    phone = _clean_text(contact.phone)
    mobile_phone = _clean_text(contact.mobile_phone)
    if not mobile_phone:
        phone, mobile_phone = split_phone_numbers(phone)
    job_title = _clean_text(contact.job_title)
    company_name = _clean_text(contact.company_name)
    website = _clean_text(contact.website)

    if first_name:
        properties["firstname"] = first_name
    if last_name:
        properties["lastname"] = last_name
    if email:
        properties["email"] = email
    if phone:
        properties["phone"] = phone
    if mobile_phone:
        properties["mobilephone"] = mobile_phone
    if job_title:
        properties["jobtitle"] = job_title
    if company_name:
        properties["company"] = company_name
    if website:
        properties["website"] = website

    if contact.additional_properties:
        for key, value in contact.additional_properties.items():
            key_clean = _clean_text(key)
            value_clean = _clean_text(value)
            if key_clean and value_clean:
                properties[key_clean] = value_clean

    # HubSpot exige ao menos email OU firstname OU lastname.
    if not (properties.get("email") or properties.get("firstname") or properties.get("lastname")):
        raise ValueError(
            "ContactPayload invalido: envie ao menos email, firstname ou lastname."
        )

    return properties


def _build_company_association(company_id: str) -> list[dict[str, Any]]:
    return [
        {
            "to": {"id": company_id},
            "types": [
                {
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": CONTACT_TO_COMPANY_ASSOCIATION_TYPE_ID,
                }
            ],
        }
    ]


def search_contact_by_email(api_key: str, email: str) -> dict[str, Any] | None:
    """Searches a contact by unique e-mail property."""
    email_clean = _clean_text(email).lower()
    if not email_clean:
        return None

    payload = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email_clean}]}
        ],
        "properties": ["firstname", "lastname", "email"],
        "limit": 1,
    }
    response = _request("POST", CONTACTS_SEARCH_PATH, api_key, payload)
    results = response.get("results") or []
    return results[0] if results else None


def create_contact(
    api_key: str,
    contact: ContactPayload,
    company_id: str | None = None,
) -> dict[str, Any]:
    """Creates a contact on HubSpot CRM."""
    payload: dict[str, Any] = {"properties": build_contact_properties(contact)}
    company_id_clean = _clean_text(company_id)
    if company_id_clean:
        payload["associations"] = _build_company_association(company_id_clean)
    return _request("POST", CONTACTS_OBJECT_PATH, api_key, payload)


def create_or_get_contact(
    api_key: str,
    contact: ContactPayload,
    company_id: str | None = None,
) -> UpsertContactResult:
    """Retrieves an existing contact by e-mail or creates a new one."""
    email = _clean_text(contact.email).lower()
    if email:
        existing = search_contact_by_email(api_key, email)
        if existing:
            existing_id = _clean_text(existing.get("id"))
            if not existing_id:
                raise HubSpotApiError("Contato encontrado sem 'id' na resposta da busca.")
            return UpsertContactResult(id=existing_id, created=False, response=existing)

    created = create_contact(api_key, contact, company_id=company_id)
    created_id = _clean_text(created.get("id"))
    if not created_id:
        raise HubSpotApiError("Resposta de criacao de contato sem 'id'.")
    return UpsertContactResult(id=created_id, created=True, response=created)

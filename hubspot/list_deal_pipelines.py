from __future__ import annotations

import os
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_API_BASE_URL = "https://api.hubapi.com"
DEAL_PIPELINES_PATH = "/crm/v3/pipelines/deals"
TIMEOUT_SECONDS = 30


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_headers(api_key: str) -> dict[str, str]:
    token = _clean_text(api_key)
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def get_hubspot_api_key() -> str:
    api_key = _clean_text(os.getenv("HUBSPOT_API_KEY"))
    if not api_key:
        raise RuntimeError("HUBSPOT_API_KEY nao encontrado no .env")
    return api_key


def list_deal_pipelines(api_key: str) -> None:
    response = requests.get(
        f"{HUBSPOT_API_BASE_URL}{DEAL_PIPELINES_PATH}",
        headers=_build_headers(api_key),
        timeout=TIMEOUT_SECONDS,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Erro HubSpot {response.status_code} ao listar pipelines: {response.text[:2000]}"
        )

    pipelines = (response.json() or {}).get("results") or []
    if not pipelines:
        print("Nenhum pipeline de deals encontrado.")
        return

    for pipeline in pipelines:
        pipeline_id = _clean_text(pipeline.get("id"))
        pipeline_label = _clean_text(pipeline.get("label"))
        print(f"PIPELINE | id={pipeline_id} | label={pipeline_label}")

        stages = pipeline.get("stages") or []
        for stage in stages:
            stage_id = _clean_text(stage.get("id"))
            stage_label = _clean_text(stage.get("label"))
            print(f"  STAGE  | id={stage_id} | label={stage_label}")
        print("-" * 80)


if __name__ == "__main__":
    list_deal_pipelines(get_hubspot_api_key())

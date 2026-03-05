import csv
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

COLUNAS_CSV_SAIDA = [
    "person_id",
    "empresa",
    "website",
    "nome_pessoa",
    "cargo",
    "email",
    "telefone",
    "status_api",
    "erro",
]


def construir_cabecalhos(api_key: str) -> dict:
    """Monta os cabecalhos de autenticacao para a API da Apollo."""
    separador = chr(45)
    return {
        "accept": "application/json",
        "content" + separador + "type": "application/json",
        "cache" + separador + "control": "no" + separador + "cache",
        "x" + separador + "api" + separador + "key": api_key,
    }


def post_com_tentativas(
    url: str,
    cabecalhos: dict,
    corpo: dict,
    parametros: dict | None = None,
    max_tentativas: int = 5,
) -> dict:
    """Executa POST com retry exponencial para tratar limitacao 429."""
    espera_segundos = 2.0
    for _ in range(max_tentativas):
        resposta = requests.post(
            url,
            headers=cabecalhos,
            params=parametros,
            json=corpo,
            timeout=60,
        )

        if resposta.status_code == 429:
            retry_after = resposta.headers.get("Retry-After")
            espera_header = int(retry_after) if (retry_after and retry_after.isdigit()) else None
            time.sleep(espera_header if espera_header is not None else espera_segundos)
            espera_segundos = min(espera_segundos * 2, 60)
            continue

        if resposta.status_code >= 400:
            raise RuntimeError(f"{resposta.status_code}. body: {resposta.text[:2000]}")

        return resposta.json()

    raise RuntimeError("429 persistente apos varias tentativas.")


def carregar_linhas_csv(caminho_csv: str, limite_linhas: int | None = None) -> list[dict]:
    """Le o CSV de entrada e devolve as linhas como dicionarios."""
    linhas = []
    with open(caminho_csv, newline="", encoding="utf-8") as arquivo:
        leitor = csv.DictReader(arquivo)
        for indice, linha in enumerate(leitor):
            if limite_linhas is not None and indice >= limite_linhas:
                break
            linhas.append(linha)
    return linhas


def obter_chave_empresa(linha: dict) -> str:
    """
    Define uma chave estavel da empresa para deduplicar.
    Prioridade: dominio -> website -> nome.
    """
    dominio = (linha.get("organization_domain") or "").strip().lower()
    website = (linha.get("organization_website") or "").strip().lower()
    nome = (linha.get("organization_name") or "").strip().lower()

    if dominio:
        return f"dominio:{dominio}"
    if website:
        return f"website:{website}"
    if nome:
        return f"nome:{nome}"
    return ""


def selecionar_uma_pessoa_por_empresa(linhas: list[dict]) -> list[dict]:
    """Mantem no maximo 3 pessoas por empresa."""
    max_por_empresa = 3
    contagem_por_empresa = {}
    linhas_filtradas = []

    for linha in linhas:
        person_id = (linha.get("person_id") or "").strip()
        if not person_id:
            continue

        chave_empresa = obter_chave_empresa(linha)
        if not chave_empresa:
            continue

        qtd_atual = contagem_por_empresa.get(chave_empresa, 0)
        if qtd_atual >= max_por_empresa:
            continue

        contagem_por_empresa[chave_empresa] = qtd_atual + 1
        linhas_filtradas.append(linha)

    return linhas_filtradas


def normalizar_pessoas_resposta(resposta_api: dict) -> list[dict]:
    """Normaliza formatos possiveis da resposta da Apollo para lista de pessoas."""
    for chave in ("people", "matches", "contacts"):
        valor = resposta_api.get(chave)
        if isinstance(valor, list):
            return [pessoa for pessoa in valor if isinstance(pessoa, dict)]

    if isinstance(resposta_api.get("person"), dict):
        return [resposta_api["person"]]

    if isinstance(resposta_api, dict) and isinstance(resposta_api.get("id"), str):
        return [resposta_api]

    return []


def extrair_email(pessoa_api: dict) -> str:
    """Extrai o melhor email disponivel na resposta da pessoa."""
    for chave in ("personal_emails", "emails"):
        valor = pessoa_api.get(chave)
        if isinstance(valor, list) and valor:
            primeiro = valor[0]
            if isinstance(primeiro, dict):
                return (primeiro.get("email") or "").strip()
            if isinstance(primeiro, str):
                return primeiro.strip()

    for chave in ("email", "work_email"):
        valor = pessoa_api.get(chave)
        if isinstance(valor, str) and valor:
            return valor.strip()

    return ""


def extrair_telefone(pessoa_api: dict) -> str:
    """Extrai o primeiro telefone disponivel na resposta da pessoa."""
    for chave in ("phone_numbers", "phones"):
        valor = pessoa_api.get(chave)
        if isinstance(valor, list):
            for telefone in valor:
                if not isinstance(telefone, dict):
                    continue
                numero = (
                    telefone.get("sanitized_number")
                    or telefone.get("raw_number")
                    or telefone.get("number")
                    or ""
                )
                if numero:
                    return str(numero).strip()

    for chave in ("direct_phone", "mobile_phone", "phone"):
        valor = pessoa_api.get(chave)
        if isinstance(valor, str) and valor:
            return valor.strip()

    return ""


def montar_nome_pessoa(pessoa_api: dict, linha_origem: dict) -> str:
    """Monta o nome da pessoa usando API e fallback do CSV de origem."""
    primeiro_nome = (pessoa_api.get("first_name") or linha_origem.get("first_name") or "").strip()
    sobrenome = (
        pessoa_api.get("last_name")
        or linha_origem.get("last_name")
        or linha_origem.get("last_name_obfuscated")
        or ""
    ).strip()
    return " ".join(parte for parte in [primeiro_nome, sobrenome] if parte).strip()


def montar_nome_empresa(pessoa_api: dict, linha_origem: dict) -> str:
    """Retorna o nome da empresa usando API com fallback para CSV."""
    empresa_api = ""
    organizacao = pessoa_api.get("organization")
    if isinstance(organizacao, dict):
        empresa_api = (organizacao.get("name") or "").strip()

    if empresa_api:
        return empresa_api
    return (linha_origem.get("organization_name") or "").strip()


def inicializar_csv_saida(caminho_csv_saida: str) -> None:
    """Cria/reescreve o CSV de saida com cabecalho."""
    with open(caminho_csv_saida, mode="w", newline="", encoding="utf-8") as arquivo_saida:
        escritor = csv.DictWriter(arquivo_saida, fieldnames=COLUNAS_CSV_SAIDA)
        escritor.writeheader()


def main() -> None:
    api_key = os.getenv("MASTER_API_KEY")
    if not api_key:
        raise RuntimeError("MASTER_API_KEY nao encontrado no .env")

    caminho_csv_entrada = os.getenv("INPUT_CSV", "apollo_people_fpa_manager.csv")
    caminho_csv_saida = os.getenv(
        "OUTPUT_CSV",
        "apollo_people_fpa_manager_um_por_empresa_enriched.csv",
    )
    webhook_url = (os.getenv("WEBHOOK_URL") or "").strip()

    max_rows_env = (os.getenv("MAX_ROWS") or "").strip()
    limite_linhas = int(max_rows_env) if max_rows_env.isdigit() else None

    delay_env = (os.getenv("BATCH_DELAY_SECONDS") or "").strip()
    atraso_entre_requisicoes = float(delay_env) if delay_env else 2.0

    linhas_entrada = carregar_linhas_csv(caminho_csv_entrada, limite_linhas)
    linhas_unicas_empresa = selecionar_uma_pessoa_por_empresa(linhas_entrada)
    if not linhas_unicas_empresa:
        raise RuntimeError("Nenhuma linha valida encontrada para processar.")

    inicializar_csv_saida(caminho_csv_saida)

    cabecalhos = construir_cabecalhos(api_key)
    url_bulk_match = "https://api.apollo.io/api/v1/people/bulk_match"
    total_empresas = len(linhas_unicas_empresa)

    with open(caminho_csv_saida, mode="a", newline="", encoding="utf-8") as arquivo_saida:
        escritor = csv.DictWriter(arquivo_saida, fieldnames=COLUNAS_CSV_SAIDA)

        for indice, linha_origem in enumerate(linhas_unicas_empresa, start=1):
            person_id = (linha_origem.get("person_id") or "").strip()

            linha_saida = {
                "person_id": person_id,
                "empresa": (linha_origem.get("organization_name") or "").strip(),
                "website": (linha_origem.get("organization_website") or "").strip(),
                "nome_pessoa": montar_nome_pessoa({}, linha_origem),
                "cargo": (linha_origem.get("title") or "").strip(),
                "email": "",
                "telefone": "",
                "status_api": "ok",
                "erro": "",
            }

            detalhe = {"id": person_id}
            if linha_saida["website"]:
                detalhe["organization_website"] = linha_saida["website"]
            corpo = {"details": [detalhe]}
            parametros = {"reveal_personal_emails": "true"}
            if webhook_url:
                parametros["reveal_phone_number"] = "true"
                parametros["webhook_url"] = webhook_url

            try:
                resposta_api = post_com_tentativas(
                    url=url_bulk_match,
                    cabecalhos=cabecalhos,
                    corpo=corpo,
                    parametros=parametros,
                    max_tentativas=5,
                )
                pessoas_api = normalizar_pessoas_resposta(resposta_api)
                pessoa_api = pessoas_api[0] if pessoas_api else {}

                if pessoa_api:
                    linha_saida["empresa"] = montar_nome_empresa(pessoa_api, linha_origem)
                    linha_saida["nome_pessoa"] = montar_nome_pessoa(pessoa_api, linha_origem)
                    linha_saida["cargo"] = (
                        pessoa_api.get("title") or linha_saida["cargo"] or ""
                    ).strip()
                    linha_saida["email"] = extrair_email(pessoa_api)
                    linha_saida["telefone"] = extrair_telefone(pessoa_api)
                else:
                    linha_saida["status_api"] = "sem_resultado"
                    linha_saida["erro"] = "API nao retornou pessoa para o person_id enviado."

            except Exception as erro:
                linha_saida["status_api"] = "erro"
                linha_saida["erro"] = str(erro)[:1000]

            # Persistencia por iteracao: grava e sincroniza no disco a cada pessoa.
            escritor.writerow(linha_saida)
            arquivo_saida.flush()
            os.fsync(arquivo_saida.fileno())

            print(
                f"[{indice}/{total_empresas}] salvo em {caminho_csv_saida} | "
                f"person_id={person_id} | status={linha_saida['status_api']}"
            )
            time.sleep(atraso_entre_requisicoes)

    print(f"ok: {caminho_csv_saida} | empresas processadas: {total_empresas}")


if __name__ == "__main__":
    main()

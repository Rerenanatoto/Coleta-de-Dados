from __future__ import annotations

import re
import unicodedata
import tempfile
import zipfile
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Optional, List
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sidrapy import get_table


# =========================================================
# CONFIGURAÇÃO GERAL
# =========================================================
START_YEAR = 2019
END_YEAR = datetime.today().year
OUTPUT_NAME = "replica_indicadores_publicos_brasil.xlsx"

# Se None, busca o RMD na web automaticamente.
# Se quiser forçar um arquivo local, preencha com o caminho.
RMD_FILE: Optional[str] = None

RMD_LOOKBACK_MONTHS = 18

PT_MESES = {
    "Jan": 1, "Fev": 2, "Mar": 3, "Abr": 4, "Mai": 5, "Jun": 6,
    "Jul": 7, "Ago": 8, "Set": 9, "Out": 10, "Nov": 11, "Dez": 12,
}
PT_MESES_MIN = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}
PAT_MES_ANO = re.compile(r"^(Jan|Fev|Mar|Abr|Mai|Jun|Jul|Ago|Set|Out|Nov|Dez)/\d{2}$")


# =========================================================
# MAPEAMENTO DE SÉRIES SGS
# =========================================================
SGS = {
    # PIB / anual
    "pib_nominal_brl": 1207,
    "pib_nominal_usd": 7324,
    "pib_real_growth": 7326,
    "pib_per_capita_usd": 21776,
    "cambio_fim": 3692,
    "cambio_medio_anual": 3694,
    "export_usd_bi": 23468,
    "import_usd_bi": 23469,
    "transacoes_correntes_usd_bi": 23461,
    "conta_capital_usd_bi": 23611,
    "conta_financeira_usd_bi": 23623,
    "ativo_reservas_usd_bi": 23803,
    "reservas_internacionais_usd_bi": 3545,
    "deflator_implicito": 1211,

    # Mensal / monetário
    "ipca_12m": 13522,
    "igp_di_mensal": 190,
    "selic_acum_mes": 4390,
    "cambio_medio_mensal": 3698,
    "cambio_diario": 1,
    "tjlp": 256,
    "tlp": 27572,
    "reservas_estoque": 3546,
    "inadimplencia_total": 21082,
    "inadimplencia_pf": 21112,
    "inadimplencia_pj": 21086,
    "inadimplencia_recursos_livres": 21085,

    # Crédito
    "credito_total_brl": 20539,
    "credito_total_pct_pib": 20622,

    # Setor externo mensal
    "transacoes_correntes_mensal": 22701,
    "transacoes_correntes_pct_pib": 23079,
    "conta_capital_mensal": 22851,
    "conta_financeira_mensal": 22863,
    "idp_mensal": 22885,
    "idp_pct_pib": 23080,

    # Fiscal mensal
    "resultado_primario_governo_central": 5497,
    "resultado_primario_consolidado": 5793,
    "resultado_nominal_gc_corrente": 4573,
    "resultado_primario_gc_corrente": 4639,
    "resultado_nominal_gc_12m": 5002,
    "resultado_primario_gc_12m": 5068,
    "resultado_nominal_spc_corrente": 4583,
    "resultado_primario_spc_corrente": 4649,
    "resultado_nominal_spc_12m": 5012,
    "resultado_primario_spc_12m": 5078,
    "dbgg_valor": 13761,
    "dbgg_pct_pib": 13762,
    "dlsp_valor": 4478,
    "dlsp_pct_pib": 4513,
}


# =========================================================
# HELPERS GERAIS
# =========================================================
def normalize_text(s: str) -> str:
    """
    Normaliza texto para busca robusta:
    - remove acentos
    - baixa caixa
    - remove espaços duplicados
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_text_strict(s: str) -> str:
    """
    Versão mais agressiva para matching de labels.
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_date_text(s: str) -> str:
    """
    Normalização específica para datas/meses.
    Preserva '/' para formatos como Dez/00, Jan/26, 01/2026.
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("-", "/")
    s = re.sub(r"[^a-z0-9/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fetch_sgs(code: int, start_year: str, end_year: str | None = None) -> pd.Series:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json&dataInicial=01/01/{start_year}"
    if end_year:
        url += f"&dataFinal=31/12/{end_year}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_json(StringIO(r.text))
    if df.empty:
        return pd.Series(dtype="float64")
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["valor"] = pd.to_numeric(
        df["valor"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    return df.set_index("data")["valor"].sort_index()


def _find_text_cols(raw: pd.DataFrame) -> List[str]:
    return [c for c in raw.columns if c.startswith("D") and (c.endswith("C") or c.endswith("N"))]


def _find_period_col(raw: pd.DataFrame) -> str:
    text_cols = _find_text_cols(raw)
    if not text_cols:
        raise ValueError(f"Não encontrei colunas D*C/D*N. Colunas: {list(raw.columns)}")
    return text_cols[0]


def _build_normalized_mask(
    raw: pd.DataFrame,
    search_terms: List[str],
    text_cols: Optional[List[str]] = None,
) -> pd.Series:
    """
    Procura os termos em todas as colunas textuais do raw,
    com normalização de acentos/caixa/espaços.
    """
    if text_cols is None:
        text_cols = _find_text_cols(raw)
    search_terms_norm = [normalize_text(x) for x in search_terms if x]
    mask = pd.Series(False, index=raw.index)
    for c in text_cols:
        col_norm = raw[c].astype(str).map(normalize_text)
        for term in search_terms_norm:
            mask = mask | col_norm.str.contains(term, regex=False, na=False)
    return mask


def _sidra_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def _detect_year_col(raw: pd.DataFrame) -> Optional[str]:
    text_cols = _find_text_cols(raw)
    for c in text_cols:
        vals = raw[c].astype(str).str.strip()
        if vals.str.fullmatch(r"\d{4}", na=False).any():
            return c
    return None


def _detect_quarter_period_col(raw: pd.DataFrame) -> str:
    text_cols = _find_text_cols(raw)
    quarter_patterns = [
        r"^\d{6}$",
        r"^\d{4}\.\d$",
        r"^\d{4}/\d$",
        r"^\d{4}\s+\d$",
        r"^[1-4].*trimestre.*\d{4}$",
        r"^\d{4}.*[1-4]$",
    ]
    for c in text_cols:
        vals = raw[c].astype(str).map(normalize_text)
        for pat in quarter_patterns:
            if vals.str.contains(pat, regex=True, na=False).any():
                return c
    return _find_period_col(raw)


def _parse_quarter_to_timestamp(s: str) -> pd.Timestamp:
    txt = normalize_text(s)

    m = re.fullmatch(r"(\d{4})(0[1-4]|[1-4])", txt)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    m = re.fullmatch(r"(\d{4})[./\s]+([1-4])", txt)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    m = re.search(r"([1-4]).*trimestre.*?(\d{4})", txt)
    if m:
        tri = int(m.group(1))
        ano = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    m = re.search(r"(\d{4}).*?t\s*([1-4])", txt)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    m1 = re.search(r"(\d{4}).*?([1-4])", txt)
    m2 = re.search(r"([1-4]).*?(\d{4})", txt)
    if m1:
        ano = int(m1.group(1))
        tri = int(m1.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)
    if m2:
        tri = int(m2.group(1))
        ano = int(m2.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    raise ValueError(f"Período trimestral inesperado: {s}")


# =========================================================
# HELPERS SIDRA
# =========================================================
def sidra_annual_named_series(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    period_col = _detect_year_col(raw)
    if period_col is None:
        period_col = _find_period_col(raw)

    text_cols = [c for c in _find_text_cols(raw) if c != period_col]
    search_terms = [target_text] + (aliases or [])
    mask = _build_normalized_mask(raw, search_terms, text_cols=text_cols)
    filtered = raw[mask].copy()

    if filtered.empty:
        raise ValueError(f"Não encontrei '{target_text}' na tabela {table_code}")

    df = filtered[[period_col, "V"]].copy()
    df.columns = ["ano", value_name]
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df[value_name] = _sidra_numeric(df[value_name])

    return (
        df.dropna(subset=["ano"])
        .groupby("ano", as_index=False)[value_name]
        .first()
        .sort_values("ano")
        .reset_index(drop=True)
    )


def sidra_annual_series_fallback(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    candidates = [target_text] + (aliases or [])
    last_exc: Optional[Exception] = None

    for candidate in candidates:
        try:
            extra_aliases = [x for x in candidates if x != candidate]
            return sidra_annual_named_series(
                table_code=table_code,
                target_text=candidate,
                value_name=value_name,
                period=period,
                aliases=extra_aliases,
            )
        except Exception as exc:
            last_exc = exc

    raise ValueError(
        f"Não encontrei a série anual '{target_text}' na tabela {table_code}. Último erro: {last_exc}"
    )


def sidra_quarterly_named_series(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    period_col = _detect_quarter_period_col(raw)
    text_cols = [c for c in _find_text_cols(raw) if c != period_col]

    search_terms = [target_text] + (aliases or [])
    mask = _build_normalized_mask(raw, search_terms, text_cols=text_cols)
    filtered = raw[mask].copy()

    if filtered.empty:
        raise ValueError(f"Não encontrei '{target_text}' na tabela trimestral {table_code}")

    df = filtered[[period_col, "V"]].copy()
    df.columns = ["periodo", value_name]
    df["periodo"] = df["periodo"].astype(str)
    df["data"] = df["periodo"].apply(_parse_quarter_to_timestamp)
    df[value_name] = _sidra_numeric(df[value_name])

    return (
        df[["data", value_name]]
        .dropna(subset=["data"])
        .sort_values("data")
        .groupby("data", as_index=False)[value_name]
        .first()
        .reset_index(drop=True)
    )


def sidra_quarterly_single_series_mean_by_year(
    table_code: str,
    value_name: str,
    period: str = "all",
) -> pd.DataFrame:
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    if raw.empty:
        raise ValueError(f"Tabela SIDRA {table_code} retornou vazia.")

    period_col = _detect_quarter_period_col(raw)
    if "V" not in raw.columns:
        raise ValueError(f"Tabela SIDRA {table_code} não possui coluna 'V'. Colunas: {list(raw.columns)}")

    df = raw[[period_col, "V"]].copy()
    df.columns = ["periodo", value_name]
    df["periodo"] = df["periodo"].astype(str)
    df["data"] = df["periodo"].apply(_parse_quarter_to_timestamp)
    df[value_name] = _sidra_numeric(df[value_name])

    df = (
        df[["data", value_name]]
        .dropna(subset=["data"])
        .sort_values("data")
        .groupby("data", as_index=False)[value_name]
        .first()
    )

    df["ano"] = df["data"].dt.year
    return (
        df.groupby("ano", as_index=False)[value_name]
        .mean()
        .sort_values("ano")
        .reset_index(drop=True)
    )


def sidra_unemployment_4099_mean_by_year(period: str = "all") -> pd.DataFrame:
    raw = get_table(
        table_code="4099",
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    if raw.empty:
        raise ValueError("Tabela SIDRA 4099 retornou vazia.")

    period_col = _detect_quarter_period_col(raw)
    text_cols = [c for c in _find_text_cols(raw) if c != period_col]

    search_terms = [
        "Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade",
        "Taxa de desocupação",
        "taxa de desocupacao",
    ]
    mask = _build_normalized_mask(raw, search_terms, text_cols=text_cols)
    filtered = raw[mask].copy()

    if filtered.empty:
        raise ValueError("Não encontrei a variável de taxa de desocupação na tabela 4099.")

    excl = pd.Series(False, index=filtered.index)
    for c in text_cols:
        col_norm = filtered[c].astype(str).map(normalize_text)
        excl = excl | col_norm.str.contains("coeficiente", regex=False, na=False)
        excl = excl | col_norm.str.contains("subutilizacao", regex=False, na=False)
        excl = excl | col_norm.str.contains("forca de trabalho potencial", regex=False, na=False)
        excl = excl | col_norm.str.contains("insuficiencia de horas", regex=False, na=False)
    filtered = filtered[~excl].copy()

    df = filtered[[period_col, "V"]].copy()
    df.columns = ["periodo", "unemployment_rate_pct_workforce"]
    df["periodo"] = df["periodo"].astype(str)
    df["data"] = df["periodo"].apply(_parse_quarter_to_timestamp)
    df["unemployment_rate_pct_workforce"] = _sidra_numeric(df["unemployment_rate_pct_workforce"])

    df = (
        df[["data", "unemployment_rate_pct_workforce"]]
        .dropna(subset=["data"])
        .sort_values("data")
        .groupby("data", as_index=False)["unemployment_rate_pct_workforce"]
        .first()
    )

    df["ano"] = df["data"].dt.year
    df = (
        df.groupby("ano", as_index=False)["unemployment_rate_pct_workforce"]
        .mean()
        .sort_values("ano")
        .reset_index(drop=True)
    )
    return df


# =========================================================
# RMD / WEB
# =========================================================
def build_rmd_page_url(year: int, month: int) -> str:
    return (
        "https://www.tesourotransparente.gov.br/publicacoes/"
        f"relatorio-mensal-da-divida-rmd/{year}/{month}"
    )


def build_rmd_base_url() -> str:
    return "https://www.tesourotransparente.gov.br/publicacoes/relatorio-mensal-da-divida-rmd/"


def iter_recent_year_months(max_lookback_months: int = 18):
    today = date.today()
    y, m = today.year, today.month

    for _ in range(max_lookback_months):
        yield y, m
        m -= 1
        if m == 0:
            m = 12
            y -= 1


def fetch_html(url: str, timeout: int = 60) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def collect_link_candidates_from_html(page_url: str, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    def add_candidate(raw_url: str, text: str, source_attr: str):
        if not raw_url:
            return
        full_url = urljoin(page_url, raw_url.strip())
        text = (text or "").strip()
        candidates.append(
            {
                "attachment_url": full_url,
                "anchor_text": text,
                "source_attr": source_attr,
            }
        )

    for a in soup.find_all("a"):
        text = a.get_text(" ", strip=True)
        for attr in ["href", "data-href", "data-url", "data-download", "download"]:
            raw = a.get(attr)
            if raw:
                add_candidate(raw, text, attr)

    for tag in soup.find_all(attrs={"data-href": True}):
        add_candidate(tag.get("data-href"), tag.get_text(" ", strip=True), "data-href")

    for tag in soup.find_all(attrs={"data-url": True}):
        add_candidate(tag.get("data-url"), tag.get_text(" ", strip=True), "data-url")

    return candidates


def score_attachment_candidate(candidate: dict, target_year: int | None = None, target_month: int | None = None) -> int:
    url_low = candidate["attachment_url"].lower()
    text_low = normalize_text(candidate.get("anchor_text", ""))

    score = 0

    if ".xlsx" in url_low:
        score += 8
    if ".zip" in url_low:
        score += 7
    if ".pdf" in url_low:
        score -= 5

    if "anexo" in url_low or "anexo" in text_low:
        score += 5
    if "rmd" in url_low or "rmd" in text_low:
        score += 5
    if "tabela" in url_low or "tabela" in text_low:
        score += 2

    if target_year is not None and target_month is not None:
        yy2 = str(target_year)[-2:]
        month_pt = normalize_text(month_number_to_pt_name(target_month))
        month_pt_ascii = normalize_text(month_number_to_pt_name_ascii(target_month))
        normalized_url = normalize_text(url_low)

        if str(target_year) in url_low or str(target_year) in text_low:
            score += 3
        if yy2 in url_low or yy2 in text_low:
            score += 2
        if month_pt in normalized_url or month_pt in text_low:
            score += 4
        if month_pt_ascii in normalized_url or month_pt_ascii in text_low:
            score += 4

    return score


def month_number_to_pt_name(month: int) -> str:
    mapping = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    return mapping[month]


def month_number_to_pt_name_ascii(month: int) -> str:
    mapping = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Marco",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    return mapping[month]


def find_rmd_attachment_in_page(page_url: str, target_year: int | None = None, target_month: int | None = None) -> dict:
    html = fetch_html(page_url, timeout=60)
    candidates = collect_link_candidates_from_html(page_url, html)

    if not candidates:
        raise FileNotFoundError(f"Nenhum link candidato foi encontrado na página: {page_url}")

    scored = []
    for c in candidates:
        score = score_attachment_candidate(c, target_year=target_year, target_month=target_month)
        scored.append({**c, "score": score})

    scored = [c for c in scored if c["score"] > 0]
    if not scored:
        raise FileNotFoundError(f"Encontrei links na página, mas nenhum parece ser anexo do RMD: {page_url}")

    scored.sort(key=lambda x: x["score"], reverse=True)
    best = scored[0]

    return {
        "page_url": page_url,
        "attachment_url": best["attachment_url"],
        "anchor_text": best["anchor_text"],
        "score": best["score"],
        "source_attr": best["source_attr"],
    }


def discover_latest_rmd_on_web(max_lookback_months: int = 18) -> dict:
    errors = []

    for year, month in iter_recent_year_months(max_lookback_months=max_lookback_months):
        monthly_url = build_rmd_page_url(year, month)
        base_url = build_rmd_base_url()

        for candidate_page in [monthly_url, base_url]:
            try:
                found = find_rmd_attachment_in_page(
                    candidate_page,
                    target_year=year,
                    target_month=month,
                )
                return {
                    "page_url": found["page_url"],
                    "attachment_url": found["attachment_url"],
                    "anchor_text": found["anchor_text"],
                    "reference_year": year,
                    "reference_month": month,
                    "score": found["score"],
                    "source_attr": found["source_attr"],
                }
            except Exception as exc:
                errors.append(f"{candidate_page} -> {exc}")

    raise FileNotFoundError(
        "Não foi possível localizar um anexo de RMD na web. "
        + " | ".join(errors[-10:])
    )


def download_file_to_temp(url: str, suffix: str | None = None) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    resp = requests.get(url, headers=headers, timeout=120, allow_redirects=True)
    resp.raise_for_status()

    if suffix is None:
        content_type = resp.headers.get("Content-Type", "").lower()
        final_url = resp.url.lower()

        if ".zip" in final_url or "zip" in content_type:
            suffix = ".zip"
        elif ".xlsx" in final_url or "spreadsheetml" in content_type or "excel" in content_type:
            suffix = ".xlsx"
        elif ".pdf" in final_url or "pdf" in content_type:
            suffix = ".pdf"
        else:
            suffix = ".bin"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(resp.content)
        return tmp.name


def extract_excel_from_zip(zip_path: str) -> tuple[str, str]:
    extract_dir = tempfile.mkdtemp(prefix="rmd_zip_")

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    excel_files = [p for p in Path(extract_dir).rglob("*.xlsx") if p.is_file()]
    if not excel_files:
        raise FileNotFoundError(f"Nenhum arquivo .xlsx foi encontrado dentro do ZIP: {zip_path}")

    def rank_excel_inside_zip(path: Path):
        name = normalize_text(path.name)
        return (
            "rmd" in name,
            "anexo" in name,
            "tabela" in name,
            name,
        )

    excel_files.sort(key=rank_excel_inside_zip, reverse=True)
    return str(excel_files[0]), extract_dir


def resolve_rmd_excel_path(rmd_file: Optional[str]) -> tuple[str | None, list[str], list[str]]:
    """
    Retorna:
      excel_path, temp_files, temp_dirs
    """
    temp_files: list[str] = []
    temp_dirs: list[str] = []

    if rmd_file:
        p = Path(rmd_file)
        if not p.exists():
            raise FileNotFoundError(f"RMD_FILE não encontrado: {p.resolve()}")
        return str(p), temp_files, temp_dirs

    found = discover_latest_rmd_on_web(max_lookback_months=RMD_LOOKBACK_MONTHS)
    downloaded_path = download_file_to_temp(found["attachment_url"])
    temp_files.append(downloaded_path)

    lower = downloaded_path.lower()
    if lower.endswith(".xlsx"):
        return downloaded_path, temp_files, temp_dirs

    if lower.endswith(".zip"):
        excel_path, extract_dir = extract_excel_from_zip(downloaded_path)
        temp_dirs.append(extract_dir)
        return excel_path, temp_files, temp_dirs

    raise ValueError(f"O arquivo baixado do RMD não é XLSX nem ZIP: {downloaded_path}")


# =========================================================
# RMD / HELPERS DE EXTRAÇÃO
# =========================================================
def month_token_to_datetime(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    if isinstance(x, (pd.Timestamp, datetime)):
        return datetime(x.year, x.month, 1)

    if isinstance(x, (int, float)) and not pd.isna(x):
        if 20000 <= float(x) <= 60000:
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=float(x))
            return datetime(dt.year, dt.month, 1)

    s = normalize_date_text(str(x))

    m = re.match(r"^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\s*/\s*(\d{2,4})$", s)
    if m:
        mes = PT_MESES_MIN[m.group(1)]
        ano_txt = m.group(2)
        ano = 2000 + int(ano_txt) if len(ano_txt) == 2 else int(ano_txt)
        return datetime(ano, mes, 1)

    m = re.match(r"^(\d{1,2})\s*/\s*(\d{2,4})$", s)
    if m:
        mes = int(m.group(1))
        ano_txt = m.group(2)
        if 1 <= mes <= 12:
            ano = 2000 + int(ano_txt) if len(ano_txt) == 2 else int(ano_txt)
            return datetime(ano, mes, 1)

    return None


def find_month_header_general(df, top_n_rows=60, min_required=3):
    best_row = None
    best_map = {}
    max_rows = min(top_n_rows, df.shape[0])

    for r in range(max_rows):
        current = {}
        for c in range(df.shape[1]):
            dt = month_token_to_datetime(df.iat[r, c])
            if dt is not None:
                current[dt] = c
        if len(current) > len(best_map) and len(current) >= min_required:
            best_map = current
            best_row = r

    if best_row is None or not best_map:
        raise ValueError("Não encontrei linha de meses na aba do RMD.")

    return best_row, dict(sorted(best_map.items(), key=lambda x: x[0]))


def row_text(df, r, ncols=6):
    vals = []
    for c in range(min(ncols, df.shape[1])):
        v = df.iat[r, c]
        if pd.notna(v):
            vals.append(str(v))
    return normalize_text_strict(" ".join(vals))


def find_row_by_label(df, target_label, min_row=0):
    if isinstance(target_label, (list, tuple, set)):
        targets = [normalize_text_strict(x) for x in target_label]
    else:
        targets = [normalize_text_strict(target_label)]

    for r in range(min_row, df.shape[0]):
        for c in range(min(4, df.shape[1])):
            cell = normalize_text_strict(df.iat[r, c])
            if not cell:
                continue
            for t in targets:
                if cell == t:
                    return r

    for r in range(min_row, df.shape[0]):
        txt = row_text(df, r, ncols=6)
        if not txt:
            continue
        for t in targets:
            if txt == t or t in txt:
                return r

    raise ValueError(f"Não encontrei a linha '{target_label}'.")


def find_col_by_label(df, target_label, max_scan_rows=80):
    if isinstance(target_label, (list, tuple, set)):
        targets = [normalize_text_strict(x) for x in target_label]
    else:
        targets = [normalize_text_strict(target_label)]

    best_col = None
    best_score = 0

    for c in range(df.shape[1]):
        textos = []
        for r in range(min(max_scan_rows, df.shape[0])):
            v = df.iat[r, c]
            if pd.notna(v):
                textos.append(str(v))
        txt = normalize_text_strict(" ".join(textos))
        if not txt:
            continue

        for t in targets:
            if txt == t or t in txt:
                return c

        txt_tokens = set(txt.split())
        for t in targets:
            t_tokens = set(t.split())
            score = len(txt_tokens.intersection(t_tokens))
            if score > best_score:
                best_score = score
                best_col = c

    if best_col is not None and best_score >= 1:
        return best_col

    raise ValueError(f"Não encontrei a coluna '{target_label}'.")


def extract_value(df, row_idx, col_idx):
    v = df.iat[row_idx, col_idx]
    if pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def extract_rmd_series_periods_in_columns(
    arquivo: str,
    aba: str,
    row_label: list[str] | str,
) -> pd.Series:
    df_raw = pd.read_excel(arquivo, sheet_name=aba, header=None, engine="openpyxl")
    header_row, month_cols = find_month_header_general(df_raw, top_n_rows=60, min_required=6)
    row_idx = find_row_by_label(df_raw, row_label, min_row=header_row + 1)

    out = {}
    for dt, col_idx in month_cols.items():
        val = extract_value(df_raw, row_idx, col_idx)
        out[dt] = val

    s = pd.Series(out).sort_index()
    s = pd.to_numeric(s, errors="coerce")
    return s


def extract_rmd_series_periods_in_rows(
    arquivo: str,
    aba: str,
    col_label: list[str] | str,
) -> pd.Series:
    df_raw = pd.read_excel(arquivo, sheet_name=aba, header=None, engine="openpyxl")
    col_idx = find_col_by_label(df_raw, col_label)

    out = {}
    for r in range(df_raw.shape[0]):
        dt = None
        for c in range(min(4, df_raw.shape[1])):
            dt = month_token_to_datetime(df_raw.iat[r, c])
            if dt is not None:
                break
        if dt is not None:
            out[dt] = extract_value(df_raw, r, col_idx)

    s = pd.Series(out).sort_index()
    s = pd.to_numeric(s, errors="coerce")
    return s


def last_available_by_year(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="float64")
    df = pd.DataFrame({"data": series.index, "valor": series.values})
    df = df.dropna(subset=["data"]).sort_values("data")
    df["ano"] = df["data"].dt.year
    return df.groupby("ano")["valor"].last().sort_index()


# =========================================================
# RMD / TESOURO
# =========================================================
def build_rmd_debt_block(rmd_file: Optional[str]) -> pd.DataFrame:
    cols = [
        "ano",
        "gross_lt_commercial_borrowing_usd_bi",
        "commercial_debt_stock_year_end_usd_bi",
        "st_debt_usd_bi",
        "bi_multilateral_debt_pct_total",
        "st_debt_pct_total",
        "fc_debt_pct_total",
        "lt_fixed_rate_debt_pct_total",
        "roll_over_ratio_pct_debt",
        "roll_over_ratio_pct_gdp",
    ]

    temp_files: list[str] = []
    temp_dirs: list[str] = []

    try:
        excel_path, temp_files, temp_dirs = resolve_rmd_excel_path(rmd_file)
        if excel_path is None:
            return pd.DataFrame(columns=cols)

        # Séries do RMD
        total_debt = extract_rmd_series_periods_in_columns(
            excel_path,
            "2.1",
            ["DPF EM PODER DO PUBLICO", "DPF EM PODER DO PÚBLICO", "DPF"],
        )
        external = extract_rmd_series_periods_in_columns(
            excel_path,
            "2.1",
            ["DPFe", "DPFe "],
        )
        fixed_rate = extract_rmd_series_periods_in_rows(
            excel_path,
            "2.5",
            ["Prefixado", "Fixed-rate", "Fixed rate"],
        )
        fx = extract_rmd_series_periods_in_rows(
            excel_path,
            "2.5",
            ["Cambio", "Câmbio", "'Câmbio", "FX"],
        )
        maturing_12m = extract_rmd_series_periods_in_rows(
            excel_path,
            "3.1",
            ["Ate 12 meses", "Até 12 meses", "Maturing in 12 months"],
        )

        # Consolidação anual: último mês disponível de cada ano
        total_y = last_available_by_year(total_debt)
        ext_y = last_available_by_year(external)
        fixed_y = last_available_by_year(fixed_rate)
        fx_y = last_available_by_year(fx)
        mat12_y = last_available_by_year(maturing_12m)

        # Câmbio fim de período para converter BRL -> USD
        cambio_fim = fetch_sgs(SGS["cambio_fim"], str(START_YEAR), str(END_YEAR))
        cambio_fim_y = (
            pd.DataFrame({"ano": cambio_fim.index.year, "cambio_fim": cambio_fim.values})
            .groupby("ano", as_index=False)
            .last()
            .set_index("ano")["cambio_fim"]
        )

        anos = sorted(
            set(total_y.index)
            | set(ext_y.index)
            | set(fixed_y.index)
            | set(fx_y.index)
            | set(mat12_y.index)
            | set(cambio_fim_y.index)
        )

        rows = []
        for ano in anos:
            total = total_y.get(ano, np.nan)
            ext = ext_y.get(ano, np.nan)
            fixed_pct = fixed_y.get(ano, np.nan)
            fx_pct = fx_y.get(ano, np.nan)
            mat12 = mat12_y.get(ano, np.nan)
            cambio = cambio_fim_y.get(ano, np.nan)

            commercial_debt_stock_year_end_usd_bi = np.nan
            st_debt_usd_bi = np.nan
            st_debt_pct_total = np.nan

            # Conversão para USD bi usando câmbio fim do ano
            if pd.notna(ext) and pd.notna(cambio) and cambio not in (0, None):
                commercial_debt_stock_year_end_usd_bi = ext / cambio

            if pd.notna(mat12) and pd.notna(cambio) and cambio not in (0, None):
                st_debt_usd_bi = mat12 / cambio

            if pd.notna(mat12) and pd.notna(total) and total not in (0, None):
                st_debt_pct_total = (mat12 / total) * 100

            rows.append(
                {
                    "ano": ano,
                    # Não identificado diretamente no recorte atual do RMD
                    "gross_lt_commercial_borrowing_usd_bi": np.nan,
                    # Proxy: DPFe convertido em USD bi
                    "commercial_debt_stock_year_end_usd_bi": commercial_debt_stock_year_end_usd_bi,
                    # Proxy: vencendo em 12 meses convertido em USD bi
                    "st_debt_usd_bi": st_debt_usd_bi,
                    # Não identificado diretamente no recorte atual do RMD
                    "bi_multilateral_debt_pct_total": np.nan,
                    # Proxy: maturing in 12 months / total debt
                    "st_debt_pct_total": st_debt_pct_total,
                    # Proxy: FX (%) da composição
                    "fc_debt_pct_total": fx_pct,
                    # Proxy: Fixed-rate (%) da composição
                    "lt_fixed_rate_debt_pct_total": fixed_pct,
                    # Não identificado diretamente no recorte atual do RMD
                    "roll_over_ratio_pct_debt": np.nan,
                    "roll_over_ratio_pct_gdp": np.nan,
                }
            )

        out = pd.DataFrame(rows).sort_values("ano").reset_index(drop=True)

        for col in cols:
            if col not in out.columns:
                out[col] = np.nan

        return out[cols]

    except Exception as exc:
        print(f"[aviso] Falha ao montar bloco RMD para 'Central Gov Debt and Borrowing': {exc}")
        return pd.DataFrame(columns=cols)

    finally:
        for f in temp_files:
            try:
                if f and Path(f).exists():
                    Path(f).unlink()
            except Exception:
                pass

        for d in temp_dirs:
            try:
                if d and Path(d).exists():
                    for p in Path(d).rglob("*"):
                        try:
                            if p.is_file():
                                p.unlink()
                        except Exception:
                            pass
                    Path(d).rmdir()
            except Exception:
                pass


# =========================================================
# ABA 1 - ECONOMIC DATA
# =========================================================
def build_economic_data() -> pd.DataFrame:
    pib_brl = fetch_sgs(SGS["pib_nominal_brl"], str(START_YEAR), str(END_YEAR))
    pib_usd = fetch_sgs(SGS["pib_nominal_usd"], str(START_YEAR), str(END_YEAR))
    pib_real = fetch_sgs(SGS["pib_real_growth"], str(START_YEAR), str(END_YEAR))
    exp_usd = fetch_sgs(SGS["export_usd_bi"], str(START_YEAR), str(END_YEAR))
    import_usd = fetch_sgs(SGS["import_usd_bi"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({
        "ano": pib_brl.index.year,
        "nominal_gdp_bil_lc": pd.to_numeric(pib_brl.values, errors="coerce") / 1e9,
        "nominal_gdp_bil_usd": pd.to_numeric(pib_usd.values, errors="coerce") / 1000.0,
        "real_gdp_growth_pct": pd.to_numeric(pib_real.values, errors="coerce"),
    }).drop_duplicates("ano").sort_values("ano").reset_index(drop=True)

    pib_pc_usd = fetch_sgs(SGS["pib_per_capita_usd"], str(START_YEAR), str(END_YEAR))
    pib_pc_df = pd.DataFrame({
        "ano": pib_pc_usd.index.year,
        "gdp_per_capita_000s_usd": pd.to_numeric(pib_pc_usd.values, errors="coerce") / 1000.0,
    }).drop_duplicates("ano")
    df = df.merge(pib_pc_df, on="ano", how="left")

    try:
        real_pc = sidra_annual_series_fallback(
            table_code="6601",
            target_text="Taxa de crescimento real do PIB per capita",
            value_name="real_gdp_per_capita_growth_pct",
            aliases=[
                "crescimento real do pib per capita",
                "taxa de crescimento real do produto interno bruto per capita",
                "pib per capita",
            ],
        )
        df = df.merge(real_pc, on="ano", how="left")
    except Exception:
        df["real_gdp_per_capita_growth_pct"] = np.nan

    exp_df = pd.DataFrame({
        "ano": exp_usd.index.year,
        "exports_usd_bi": exp_usd.values,
    }).drop_duplicates("ano")
    imp_df = pd.DataFrame({
        "ano": import_usd.index.year,
        "imports_usd_bi": import_usd.values,
    }).drop_duplicates("ano")

    df = df.merge(exp_df, on="ano", how="left").merge(imp_df, on="ano", how="left")
    df["exports_gdp_pct"] = ((df["exports_usd_bi"] / df["nominal_gdp_bil_usd"]) * 100) / 1000.0

    try:
        inv = sidra_quarterly_single_series_mean_by_year(
            table_code="6727",
            value_name="investment_gdp_pct",
            period="all",
        )
        df = df.merge(inv, on="ano", how="left")
    except Exception as e:
        print(f"[aviso] Falha ao usar SIDRA 6727 para investment_gdp_pct: {e}")
        df["investment_gdp_pct"] = np.nan

    try:
        fbc_real_tri = sidra_quarterly_named_series(
            table_code="5932",
            target_text="Formação bruta de capital",
            value_name="fbc_var_volume_tri",
            aliases=["formacao bruta de capital", "fbcf", "formação bruta de capital fixo"],
        )
        fbc_real_tri["ano"] = fbc_real_tri["data"].dt.year
        real_inv = (
            fbc_real_tri.groupby("ano", as_index=False)["fbc_var_volume_tri"]
            .mean()
            .rename(columns={"fbc_var_volume_tri": "real_investment_growth_pct"})
        )
        df = df.merge(real_inv, on="ano", how="left")
    except Exception:
        df["real_investment_growth_pct"] = np.nan

    try:
        savings = sidra_quarterly_single_series_mean_by_year(
            table_code="6726",
            value_name="savings_gdp_pct",
            period="all",
        )
        df = df.merge(savings, on="ano", how="left")
    except Exception as e:
        print(f"[aviso] Falha ao usar SIDRA 6726 para taxa de poupança: {e}")
        df["savings_gdp_pct"] = np.nan

    try:
        desemp_4099 = sidra_unemployment_4099_mean_by_year("all")
        df = df.merge(desemp_4099, on="ano", how="left")
    except Exception as e:
        print(f"[aviso] Falha ao usar SIDRA 4099 para unemployment_rate_pct_workforce: {e}")
        df["unemployment_rate_pct_workforce"] = np.nan

    for col in [
        "gdp_per_capita_000s_usd",
        "real_gdp_per_capita_growth_pct",
        "real_investment_growth_pct",
        "investment_gdp_pct",
        "savings_gdp_pct",
        "exports_gdp_pct",
        "unemployment_rate_pct_workforce",
    ]:
        if col not in df.columns:
            df[col] = np.nan

    cols = [
        "ano",
        "nominal_gdp_bil_lc",
        "nominal_gdp_bil_usd",
        "gdp_per_capita_000s_usd",
        "real_gdp_growth_pct",
        "real_gdp_per_capita_growth_pct",
        "real_investment_growth_pct",
        "investment_gdp_pct",
        "savings_gdp_pct",
        "exports_gdp_pct",
        "unemployment_rate_pct_workforce",
    ]
    return df[cols]


# =========================================================
# ABA 2 - MONETARY DATA
# =========================================================
def build_monetary_data() -> pd.DataFrame:
    ipca = fetch_sgs(SGS["ipca_12m"], str(START_YEAR), str(END_YEAR))
    cambio_fim = fetch_sgs(SGS["cambio_fim"], str(START_YEAR), str(END_YEAR))
    credito_total = fetch_sgs(SGS["credito_total_brl"], str(START_YEAR), str(END_YEAR))
    credito_pct_pib = fetch_sgs(SGS["credito_total_pct_pib"], str(START_YEAR), str(END_YEAR))
    deflator_1211 = fetch_sgs(SGS["deflator_implicito"], str(START_YEAR), str(END_YEAR))

    anos = sorted(
        set(ipca.index.year)
        | set(cambio_fim.index.year)
        | set(credito_total.index.year)
        | set(credito_pct_pib.index.year)
        | set(deflator_1211.index.year)
    )
    df = pd.DataFrame({"ano": anos})

    df = df.merge(
        pd.DataFrame({
            "ano": ipca.index.year,
            "cpi_growth_pct": ipca.values
        }).groupby("ano", as_index=False).last(),
        on="ano",
        how="left",
    )

    df = df.merge(
        pd.DataFrame({
            "ano": deflator_1211.index.year,
            "gdp_deflator_growth_pct": pd.to_numeric(deflator_1211.values, errors="coerce"),
        }).drop_duplicates("ano"),
        on="ano",
        how="left",
    )

    df = df.merge(
        pd.DataFrame({
            "ano": cambio_fim.index.year,
            "exchange_rate_year_end_lc_per_usd": cambio_fim.values
        }).groupby("ano", as_index=False).last(),
        on="ano",
        how="left",
    )

    credito_df = (
        pd.DataFrame({
            "ano": credito_total.index.year,
            "credito_total_brl": credito_total.values
        })
        .groupby("ano", as_index=False)
        .last()
        .sort_values("ano")
    )
    credito_df["banks_claims_growth_pct"] = credito_df["credito_total_brl"].pct_change() * 100

    credito_pib_df = (
        pd.DataFrame({
            "ano": credito_pct_pib.index.year,
            "banks_claims_gdp_pct": credito_pct_pib.values
        })
        .groupby("ano", as_index=False)
        .last()
    )

    df = df.merge(credito_df[["ano", "banks_claims_growth_pct"]], on="ano", how="left")
    df = df.merge(credito_pib_df, on="ano", how="left")

    df["fx_share_claims_pct"] = np.nan
    df["fx_share_deposits_pct"] = np.nan
    df["reer_growth_pct"] = np.nan

    cols = [
        "ano",
        "cpi_growth_pct",
        "gdp_deflator_growth_pct",
        "exchange_rate_year_end_lc_per_usd",
        "banks_claims_growth_pct",
        "banks_claims_gdp_pct",
        "fx_share_claims_pct",
        "fx_share_deposits_pct",
        "reer_growth_pct",
    ]
    return df[cols]


# =========================================================
# ABA 3 - GENERAL GOVERNMENT DATA
# =========================================================
def build_general_government_data() -> pd.DataFrame:
    dbgg = fetch_sgs(SGS["dbgg_pct_pib"], str(START_YEAR), str(END_YEAR))
    dlsp = fetch_sgs(SGS["dlsp_pct_pib"], str(START_YEAR), str(END_YEAR))
    primario = fetch_sgs(SGS["resultado_primario_consolidado"], str(START_YEAR), str(END_YEAR))
    nominal_12m = fetch_sgs(SGS["resultado_nominal_spc_12m"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({"ano": sorted(set(dbgg.index.year) | set(dlsp.index.year))})

    df = df.merge(
        pd.DataFrame({"ano": dbgg.index.year, "gross_gg_debt_gdp_pct": dbgg.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": dlsp.index.year, "net_gg_debt_gdp_pct": dlsp.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": primario.index.year, "primary_gg_balance_gdp_pct": primario.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": nominal_12m.index.year, "gg_balance_gdp_pct": nominal_12m.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )

    df = df.sort_values("ano").reset_index(drop=True)
    df["change_in_net_gg_debt_gdp_pct"] = df["net_gg_debt_gdp_pct"].diff()
    df["liquid_assets_gdp_pct"] = df["gross_gg_debt_gdp_pct"] - df["net_gg_debt_gdp_pct"]

    df["gg_revenues_gdp_pct"] = np.nan
    df["gg_expenditures_gdp_pct"] = np.nan
    df["gg_interest_expenditure_revenues_pct"] = np.nan
    df["debt_revenues_pct"] = np.nan

    cols = [
        "ano",
        "gg_balance_gdp_pct",
        "change_in_net_gg_debt_gdp_pct",
        "primary_gg_balance_gdp_pct",
        "gg_revenues_gdp_pct",
        "gg_expenditures_gdp_pct",
        "gg_interest_expenditure_revenues_pct",
        "gross_gg_debt_gdp_pct",
        "debt_revenues_pct",
        "net_gg_debt_gdp_pct",
        "liquid_assets_gdp_pct",
    ]
    return df[cols]


# =========================================================
# ABA 4 - BALANCE OF PAYMENTS DATA
# =========================================================
def build_balance_of_payments_data() -> pd.DataFrame:
    tc_pct = fetch_sgs(SGS["transacoes_correntes_pct_pib"], str(START_YEAR), str(END_YEAR))
    idp_pct = fetch_sgs(SGS["idp_pct_pib"], str(START_YEAR), str(END_YEAR))
    exp = fetch_sgs(SGS["export_usd_bi"], str(START_YEAR), str(END_YEAR))
    imp = fetch_sgs(SGS["import_usd_bi"], str(START_YEAR), str(END_YEAR))
    pib_usd = fetch_sgs(SGS["pib_nominal_usd"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({"ano": sorted(set(tc_pct.index.year) | set(idp_pct.index.year))})

    df = df.merge(
        pd.DataFrame({"ano": tc_pct.index.year, "current_account_balance_gdp_pct": tc_pct.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": idp_pct.index.year, "net_fdi_gdp_pct": idp_pct.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )

    exp_df = pd.DataFrame({"ano": exp.index.year, "exports_usd_bi": exp.values}).drop_duplicates("ano")
    imp_df = pd.DataFrame({"ano": imp.index.year, "imports_usd_bi": imp.values}).drop_duplicates("ano")
    pib_df = pd.DataFrame({"ano": pib_usd.index.year, "nominal_gdp_bil_usd": pib_usd.values / 1000.0}).drop_duplicates("ano")

    df = df.merge(exp_df, on="ano", how="left").merge(imp_df, on="ano", how="left").merge(pib_df, on="ano", how="left")

    df["trade_balance_gdp_pct"] = ((df["exports_usd_bi"] - df["imports_usd_bi"]) / df["nominal_gdp_bil_usd"]) * 100
    df["real_exports_growth_pct"] = df["exports_usd_bi"].pct_change() * 100
    df["cars_gdp_pct"] = np.nan
    df["current_account_balance_cars_pct"] = np.nan
    df["usable_reserves_caps_months"] = np.nan
    df["gross_ext_fin_needs_over_car_plus_res_pct"] = np.nan
    df["net_portfolio_equity_inflow_gdp_pct"] = np.nan

    cols = [
        "ano",
        "cars_gdp_pct",
        "real_exports_growth_pct",
        "current_account_balance_gdp_pct",
        "current_account_balance_cars_pct",
        "usable_reserves_caps_months",
        "gross_ext_fin_needs_over_car_plus_res_pct",
        "net_fdi_gdp_pct",
        "trade_balance_gdp_pct",
        "net_portfolio_equity_inflow_gdp_pct",
    ]
    return df[cols]


# =========================================================
# ABA 5 - EXTERNAL BALANCE SHEET
# =========================================================
def build_external_balance_sheet() -> pd.DataFrame:
    reservas = fetch_sgs(SGS["reservas_internacionais_usd_bi"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({"ano": sorted(set(reservas.index.year))})

    res_df = pd.DataFrame({
        "ano": reservas.index.year,
        "usable_reserves_usd_bi": reservas.values,
    }).drop_duplicates("ano")
    df = df.merge(res_df, on="ano", how="left")

    df["usable_reserves_usd_mil"] = df["usable_reserves_usd_bi"] * 1000
    df["narrow_net_ext_debt_cars_pct"] = np.nan
    df["narrow_net_ext_debt_caps_pct"] = np.nan
    df["net_ext_liabilities_cars_pct"] = np.nan
    df["st_external_debt_remaining_maturity_cars_pct"] = np.nan

    cols = [
        "ano",
        "narrow_net_ext_debt_cars_pct",
        "narrow_net_ext_debt_caps_pct",
        "net_ext_liabilities_cars_pct",
        "st_external_debt_remaining_maturity_cars_pct",
        "usable_reserves_usd_mil",
    ]
    return df[cols]


# =========================================================
# ABA 6 - CENTRAL GOV DEBT AND BORROWING DATA
# =========================================================
def build_central_government_debt_and_borrowing_data() -> pd.DataFrame:
    return build_rmd_debt_block(RMD_FILE)


# =========================================================
# EXPORTAÇÃO
# =========================================================
def export_to_excel(tabelas: dict[str, pd.DataFrame], output_name: str):
    output = Path(output_name)

    with pd.ExcelWriter(
        output,
        engine="openpyxl",
        datetime_format="YYYY-MM-DD",
        date_format="YYYY-MM-DD",
    ) as writer:
        for nome_aba, df in tabelas.items():
            sheet = nome_aba[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
            ws = writer.sheets[sheet]
            ws.freeze_panes = "B2"
            for idx, col in enumerate(df.columns, start=1):
                values = df[col].head(1000).fillna("").astype(str).tolist()
                max_len = max([len(str(col))] + [len(v) for v in values])
                ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 28)

    print(f"Arquivo exportado com sucesso: {output.resolve()}")


# =========================================================
# MAIN
# =========================================================
def main():
    economic = build_economic_data()
    monetary = build_monetary_data()
    fiscal = build_general_government_data()
    bop = build_balance_of_payments_data()
    ebs = build_external_balance_sheet()
    debt = build_central_government_debt_and_borrowing_data()

    tabelas = {
        "Economic Data": economic,
        "Monetary Data": monetary,
        "General Government Data": fiscal,
        "Balance-Of-Payments Data": bop,
        "External Balance Sheet": ebs,
        "Central Gov Debt and Borrowing": debt,
    }

    export_to_excel(tabelas, OUTPUT_NAME)


if __name__ == "__main__":
    main()

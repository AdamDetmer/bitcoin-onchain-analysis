"""Ton i liczba artykułów o Bitcoinie z GDELT DOC 2.0 API, agregowane do dni.

GDELT pozwala na 1 zapytanie na 5 sekund, więc pobieramy miesiąc po miesiącu
z przerwą między zapytaniami. Każdy miesiąc jest zapisywany w cache, więc
przerwane pobieranie można wznowić bez powtarzania zapytań.

Uruchomienie z katalogu głównego repo (pełna historia trwa ok. 15-20 min):
    python -m src.text.gdelt
    python -m src.text.gdelt --start 2024-01 --end 2024-12
"""
import argparse
import json
import os
import time
from datetime import date

import pandas as pd
import requests

API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_QUERY = "(bitcoin OR btc)"
CACHE_DIR = os.path.join("data", "raw", "sentiment", "gdelt_cache")
OUT_PATH = os.path.join("data", "raw", "sentiment", "gdelt_daily.parquet")

MIN_INTERVAL_S = 6       # limit GDELT: 1 zapytanie / 5 s, bierzemy zapas
BACKOFF_S = 60           # przerwa po odpowiedzi 429
MAX_RETRIES = 5
MODES = {"timelinetone": "tone", "timelinevolraw": "volume"}

_last_request = 0.0


def _get(params: dict) -> dict:
    """Jedno zapytanie z przestrzeganiem limitu i ponawianiem po 429."""
    global _last_request
    for attempt in range(1, MAX_RETRIES + 1):
        wait = MIN_INTERVAL_S - (time.time() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.time()
        resp = requests.get(API_URL, params=params, timeout=90)
        if resp.status_code == 429 or resp.text.startswith("Please limit"):
            print(f"  limit GDELT, czekam {BACKOFF_S * attempt}s (próba {attempt}/{MAX_RETRIES})")
            time.sleep(BACKOFF_S * attempt)
            continue
        resp.raise_for_status()
        text = resp.text.strip()
        return json.loads(text) if text else {}
    raise RuntimeError("GDELT ciągle zwraca limit zapytań - spróbuj później.")


def _months(start: str, end: str):
    """Zwraca kolejne miesiące (pierwszy dzień, pierwszy dzień następnego)."""
    for p in pd.period_range(start, end, freq="M"):
        yield p.start_time.date(), (p + 1).start_time.date()


def _fetch_month(query: str, mode: str, m_start: date, m_end: date) -> dict:
    tag = MODES[mode]
    path = os.path.join(CACHE_DIR, f"{tag}_{m_start:%Y-%m}.json")
    is_current = m_end > date.today()
    if os.path.exists(path) and not is_current:   # bieżący miesiąc zawsze odświeżamy
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    data = _get({
        "query": query,
        "mode": mode,
        "format": "json",
        "startdatetime": f"{m_start:%Y%m%d}000000",
        "enddatetime": f"{m_end:%Y%m%d}000000",
    })
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return data


def _to_frame(raw: dict, value_col: str) -> pd.DataFrame:
    """Spłaszcza odpowiedź timeline do (ts, value[, norm])."""
    rows = []
    for series in raw.get("timeline", []):
        for point in series.get("data", []):
            row = {"ts": point["date"], value_col: point.get("value")}
            if "norm" in point:
                row["total_articles"] = point["norm"]
            rows.append(row)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], format="%Y%m%dT%H%M%SZ")
    return df


def fetch_gdelt(start: str, end: str, query: str = DEFAULT_QUERY) -> pd.DataFrame:
    tone_parts, vol_parts = [], []
    for m_start, m_end in _months(start, end):
        print(f"GDELT {m_start:%Y-%m} ...")
        tone_parts.append(_to_frame(_fetch_month(query, "timelinetone", m_start, m_end), "tone"))
        vol_parts.append(_to_frame(_fetch_month(query, "timelinevolraw", m_start, m_end), "articles"))

    tone = pd.concat(tone_parts, ignore_index=True)
    vol = pd.concat(vol_parts, ignore_index=True)
    if tone.empty or vol.empty:
        raise RuntimeError("GDELT nie zwrócił danych - sprawdź zapytanie i zakres dat.")

    # API może zwrócić dane gęstsze niż dzienne; ton ważymy liczbą artykułów.
    df = vol.merge(tone, on="ts", how="outer")
    df["date"] = df["ts"].dt.normalize()
    df["w"] = df["articles"].fillna(0)
    df["tone_x_w"] = df["tone"] * df["w"]
    daily = df.groupby("date").agg(
        articles=("articles", "sum"),
        total_articles=("total_articles", "sum"),
        tone_x_w=("tone_x_w", "sum"),
        w=("w", "sum"),
        tone_mean=("tone", "mean"),
    )
    daily["gdelt_tone"] = (daily["tone_x_w"] / daily["w"]).where(daily["w"] > 0, daily["tone_mean"])
    daily["gdelt_articles"] = daily["articles"]
    # udział artykułów o BTC we wszystkich artykułach GDELT = miara uwagi
    daily["gdelt_attention"] = daily["articles"] / daily["total_articles"].where(daily["total_articles"] > 0)
    return daily[["gdelt_tone", "gdelt_articles", "gdelt_attention"]].reset_index()


def main() -> None:
    parser = argparse.ArgumentParser(description="Pobieranie tonu i uwagi z GDELT")
    parser.add_argument("--start", default="2017-01", help="pierwszy miesiąc, RRRR-MM")
    parser.add_argument("--end", default=f"{date.today():%Y-%m}", help="ostatni miesiąc, RRRR-MM")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    args = parser.parse_args()

    df = fetch_gdelt(args.start, args.end, args.query)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"Zapisano {len(df)} dni ({df['date'].min():%Y-%m-%d} - {df['date'].max():%Y-%m-%d}) -> {OUT_PATH}")


if __name__ == "__main__":
    main()

"""Crypto Fear & Greed Index (alternative.me), dzienny, od 2018-02-01.

Uruchomienie z katalogu głównego repo:
    python -m src.market.fear_greed
"""
import os

import pandas as pd
import requests

API_URL = "https://api.alternative.me/fng/"
OUT_PATH = os.path.join("data", "raw", "sentiment", "fear_greed.parquet")


def fetch_fear_greed() -> pd.DataFrame:
    """Pobiera całą historię indeksu (limit=0) i zwraca ramkę posortowaną po dacie."""
    resp = requests.get(API_URL, params={"limit": 0, "format": "json"}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("metadata", {}).get("error"):
        raise RuntimeError(f"Błąd API alternative.me: {payload['metadata']['error']}")

    df = pd.DataFrame(payload["data"])
    df["date"] = pd.to_datetime(df["timestamp"].astype(int), unit="s").dt.normalize()
    df = df.rename(columns={"value": "fng_value", "value_classification": "fng_class"})
    df["fng_value"] = df["fng_value"].astype(int)
    df = df[["date", "fng_value", "fng_class"]]
    return df.drop_duplicates("date").sort_values("date").reset_index(drop=True)


def main() -> None:
    df = fetch_fear_greed()
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    full_range = pd.date_range(df["date"].min(), df["date"].max(), freq="D")
    missing = full_range.difference(df["date"])
    print(f"Zapisano {len(df)} dni ({df['date'].min():%Y-%m-%d} - {df['date'].max():%Y-%m-%d}) -> {OUT_PATH}")
    print(f"Brakujące dni: {len(missing)}")


if __name__ == "__main__":
    main()

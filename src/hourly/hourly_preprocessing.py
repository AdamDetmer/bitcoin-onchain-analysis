import os
import numpy as np
import pandas as pd


class HourlyDataPreprocessor:

    def __init__(
        self,
        raw_dir="data/hourly/raw",
        processed_dir="data/hourly/processed",
        years_back=5,
    ):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.years_back = years_back
        os.makedirs(self.processed_dir, exist_ok=True)

    def _clean_datetime_series(self, series: pd.Series) -> pd.Series:
        dt = pd.to_datetime(series, format="mixed", utc=True)
        return dt.dt.tz_localize(None)

    def merge_and_clean(self):
        print("=== ROZPOCZYNAM INTEGRACJĘ DANYCH (SZTYWNE 5 LAT) ===")

        # 1. Główny plik godzinowy (Binance Spot)
        market_path = os.path.join(
            self.raw_dir, "bitcoin_hourly_market_data.csv"
        )
        if not os.path.exists(market_path):
            print(f"[BŁĄD] Brak pliku: {market_path}")
            return None

        master_df = pd.read_csv(market_path)

        time_col = "Date" if "Date" in master_df.columns else "timestamp"
        master_df["timestamp"] = self._clean_datetime_series(
            master_df[time_col]
        )
        if time_col != "timestamp":
            master_df.drop(columns=[time_col], inplace=True)

        master_df = master_df.sort_values("timestamp").reset_index(drop=True)

        # --- KLUCZOWA POPRAWKA 1: Ograniczenie danych sztywno do ostatnich 5 lat ---
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(
            days=self.years_back * 365
        )
        master_df = master_df[master_df["timestamp"] >= cutoff_date].copy()
        print(
            f"  -> Przycięto zakres danych rynkowych do ostatnich 5 lat (od {cutoff_date.date()})"
        )

        # Kluczowa kolumna spójności dla danych dziennych
        master_df["date_key"] = master_df["timestamp"].dt.normalize()

        # 2. Dołączanie danych GODZINOWYCH (Deribit Options, Dune Whale, Dune Stablecoins)
        hourly_files = [
            ("bitcoin_hourly_options_data.csv", "Deribit Options"),
            ("bitcoin_hourly_whale_data.csv", "Dune Whale Txs"),
            ("bitcoin_hourly_stablecoin_data.csv", "Dune Stablecoins"),

        ]

        for file_name, label in hourly_files:
            file_path = os.path.join(self.raw_dir, file_name)
            if os.path.exists(file_path):
                hdf = pd.read_csv(file_path)
                h_col = "timestamp" if "timestamp" in hdf.columns else "Date"
                hdf["timestamp"] = self._clean_datetime_series(hdf[h_col])

                cols_to_drop = [
                    c
                    for c in hdf.columns
                    if c in master_df.columns and c != "timestamp"
                ]
                hdf.drop(columns=cols_to_drop, inplace=True, errors="ignore")

                master_df = pd.merge(
                    master_df, hdf, on="timestamp", how="left"
                )
                print(f"  + [1H] {label} dołączono.")

        # 3. Dołączanie danych DZIENNYCH (Active Addresses, LN)
        daily_files = [
            ("active_addresses_data.csv", "CoinMetrics Aktywne Adresy"),
            ("lightning_network_data.csv", "Mempool Lightning Network"),
            ("btc_total_supply.csv", "Blockchain.com Total Supply"),
        ]

        for file_name, label in daily_files:
            file_path = os.path.join(self.raw_dir, file_name)
            if os.path.exists(file_path):
                ddf = pd.read_csv(file_path)
                d_col = "date" if "date" in ddf.columns else "time"

                ddf["date_key"] = self._clean_datetime_series(
                    ddf[d_col]
                ).dt.normalize()

                # Usuwanie starych kolumn tekstowych dat z pliku dziennego
                ddf.drop(
                    columns=[c for c in [d_col, "time", "date"] if c in ddf.columns],
                    inplace=True,
                    errors="ignore",
                )

                master_df = pd.merge(
                    master_df, ddf, on="date_key", how="left"
                )
                print(f"  + [1D -> 1H] {label} dołączono.")

        # Usuwamy pomocniczy klucz daty
        master_df.drop(columns=["date_key"], inplace=True)

        # 4. Wypełnianie braków w obrębie rzeczywistych 5 lat (forward fill)
        numeric_cols = master_df.select_dtypes(include=[np.number]).columns
        master_df[numeric_cols] = master_df[numeric_cols].ffill().bfill()

        output_path = os.path.join(
            self.processed_dir, "bitcoin_hourly_master.csv"
        )
        master_df.to_csv(output_path, index=False)

        print(f"\n✅ SUKCES! Zbudowano Master Dataset: {output_path}")
        print(
            f"Liczba wierszy: {len(master_df)} | Liczba kolumn: {len(master_df.columns)}"
        )
        return master_df
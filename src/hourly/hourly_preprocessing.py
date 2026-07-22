import os
import numpy as np
import pandas as pd


class HourlyDataPreprocessor:

    def __init__(
        self,
        raw_dir="data/hourly/raw",
        processed_dir="data/hourly/processed",
    ):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        os.makedirs(self.processed_dir, exist_ok=True)

    def _load_and_standardize_timestamp(self, filepath: str) -> pd.DataFrame:
        """Służy do bezpiecznego wczytywania plików CSV i standaryzowania kolumny timestamp.

        Obsługuje różne nazwy kolumn czasowych (Date, timestamp) oraz brak
        nagłówka indeksu.
        """
        if not os.path.exists(filepath):
            return None

        # Wczytujemy dane
        df = pd.read_csv(filepath)

        if df.empty:
            return None

        # 1. Poszukiwanie kolumny z czasem w różnych wariantach
        time_col = None
        for col in df.columns:
            if str(col).strip().lower() in [
                "timestamp",
                "date",
                "datetime",
                "time",
                "unnamed: 0",
            ]:
                time_col = col
                break

        # Jeśli timestamp był w indeksie pliku CSV
        if time_col is None:
            df = pd.read_csv(filepath, index_col=0).reset_index()
            time_col = df.columns[0]

        # 2. Konwersja na datetime
        df["timestamp"] = pd.to_datetime(df[time_col])

        if time_col != "timestamp":
            df.drop(columns=[time_col], inplace=True)

        # 3. Usuwanie strefy czasowej (tz-naive), aby uniknąć błędów join()
        if df["timestamp"].dt.tz is not None:
            df["timestamp"] = df["timestamp"].dt.tz_localize(None)

        # Zaokrąglanie do pełnych godzin (1h)
        df["timestamp"] = df["timestamp"].dt.floor("h")

        # Usuwanie ewentualnych duplikatów w tym samym punkcie czasowym
        df = df.drop_duplicates(subset=["timestamp"])

        return df

    def merge_and_clean(self):
        print("=== ROZPOCZYNAM INTEGRACJĘ DANYCH GODZINOWYCH ===")

        # ---------------------------------------------------------
        # 1. DANE RYNKOWE (Anchor Dataset)
        # ---------------------------------------------------------
        market_path = os.path.join(
            self.raw_dir, "bitcoin_hourly_market_data.csv"
        )
        market_df = self._load_and_standardize_timestamp(market_path)

        if market_df is None:
            print(
                f"[BŁĄD CRITICAL] Nie znaleziono pliku rynkowego: {market_path}"
            )
            print("Uruchom najpierw pobieranie w HourlyDataIngestor.")
            return None

        # Standaryzacja nazw kolumn rynkowych
        rename_market = {
            "Volume USDT": "volume_usdt",
            "Volume BTC": "volume_btc",
            "tradecount": "trade_count",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
        }
        market_df.rename(columns=rename_market, inplace=True)

        # Feature Engineering: Dodatkowe metryki rynkowe
        if "volume_usdt" in market_df.columns and "trade_count" in market_df.columns:
            # Średnia wartość transakcji USD (Proxy wielorybów)
            market_df["avg_trade_size_usd"] = market_df[
                "volume_usdt"
            ] / market_df["trade_count"].replace(0, np.nan)

        if "close" in market_df.columns:
            # Godzinowa zmiana ceny w %
            market_df["returns_1h_pct"] = market_df["close"].pct_change() * 100

        if all(k in market_df.columns for k in ["high", "low", "open"]):
            # Godzinowa zmienność w % (Range)
            market_df["hourly_volatility_pct"] = (
                (market_df["high"] - market_df["low"]) / market_df["open"]
            ) * 100

        master_df = market_df.copy()
        print(
            f"  + Dane Rynkowe (Spot): {master_df['timestamp'].min()} → {master_df['timestamp'].max()} ({len(master_df)} wierszy)"
        )

        # ---------------------------------------------------------
        # 2. DANE OPCYJNE (Deribit DVOL & Greki/Delta)
        # ---------------------------------------------------------
        options_path = os.path.join(
            self.raw_dir, "bitcoin_hourly_options_data.csv"
        )
        opt_df = self._load_and_standardize_timestamp(options_path)
        if opt_df is not None:
            # Usuwamy z opcji duplicate ceny btc_price jeśli już ją mamy ze spotu
            if "btc_price" in opt_df.columns:
                opt_df.drop(columns=["btc_price"], inplace=True)

            master_df = pd.merge(master_df, opt_df, on="timestamp", how="left")
            print(f"  + Rynek Opcji & Delta (Deribit): dołączono {len(opt_df.columns)-1} kolumn")

        # ---------------------------------------------------------
        # 3. DANE ON-CHAIN WIELORYBÓW (Dune Analytics)
        # ---------------------------------------------------------
        whale_path = os.path.join(
            self.raw_dir, "bitcoin_hourly_whale_data.csv"
        )
        whale_df = self._load_and_standardize_timestamp(whale_path)
        if whale_df is not None:
            master_df = pd.merge(
                master_df, whale_df, on="timestamp", how="left"
            )
            print(
                f"  + Transakcje Wielorybów (Dune): dołączono {len(whale_df.columns)-1} kolumn"
            )

        # ---------------------------------------------------------
        # 4. DANE STABLECOINÓW (Dune Analytics)
        # ---------------------------------------------------------
        stable_path = os.path.join(
            self.raw_dir, "bitcoin_hourly_stablecoin_data.csv"
        )
        stable_df = self._load_and_standardize_timestamp(stable_path)
        if stable_df is not None:
            master_df = pd.merge(
                master_df, stable_df, on="timestamp", how="left"
            )
            print(
                f"  + Przepływy Stablecoinów (Dune): dołączono {len(stable_df.columns)-1} kolumn"
            )

        # ---------------------------------------------------------
        # 5. DODATKOWE PLIKI (np. Lightning Network, Mempool if available)
        # ---------------------------------------------------------
        extra_files = [
            ("lightning_converted_to_hourly.csv", "Lightning Network"),
            ("bitcoin_hourly_active_addresses.csv", "Aktywne Adresy"),
        ]
        for file_name, label in extra_files:
            extra_path = os.path.join(self.raw_dir, file_name)
            extra_df = self._load_and_standardize_timestamp(extra_path)
            if extra_df is not None:
                master_df = pd.merge(
                    master_df, extra_df, on="timestamp", how="left"
                )
                print(f"  + {label}: dołączono dane.")

        # ---------------------------------------------------------
        # SORTOWANIE I OBSŁUGA BRAKÓW (Interpolacja Szeregów Czasowych)
        # ---------------------------------------------------------
        master_df = master_df.sort_values("timestamp").reset_index(drop=True)

        # Interpolacja pojedynczych brakujących godzin
        if master_df.isnull().sum().sum() > 0:
            print(
                "Wykryto braki w danych. Stosuję interpolację liniową i wypełnianie krawędzi..."
            )
            # Najpierw interpolujemy liniowo małe dziury czasowe
            numeric_cols = master_df.select_dtypes(include=[np.number]).columns
            master_df[numeric_cols] = master_df[numeric_cols].interpolate(
                method="linear", limit_direction="both"
            )
            # Dopełniamy krawędzie początku/końca zakresu
            master_df = master_df.ffill().bfill()

        # Zapis do pliku końcowego
        output_path = os.path.join(
            self.processed_dir, "bitcoin_hourly_master.csv"
        )
        master_df.to_csv(output_path, index=False)

        print("\n=== SUKCES! GODZINOWY MASTER DATASET UTWORZONY ===")
        print(f"Plik: {output_path}")
        print(
            f"Zakres czasowy: {master_df['timestamp'].min()} → {master_df['timestamp'].max()}"
        )
        print(f"Liczba rekordów (godzin): {len(master_df)}")
        print(f"Liczba kolumn: {len(master_df.columns)}")

        return master_df


if __name__ == "__main__":
    preprocessor = HourlyDataPreprocessor()
    df = preprocessor.merge_and_clean()
    if df is not None:
        print("\nOstatnie 5 godzin w zbiorze:")
        print(df.tail())
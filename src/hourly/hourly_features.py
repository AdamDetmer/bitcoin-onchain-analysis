import os
import numpy as np
import pandas as pd


class HourlyFeatureEngineer:

    def __init__(
        self,
        file_path="data/hourly/processed/bitcoin_hourly_master.csv",
        output_path="data/hourly/processed/bitcoin_hourly_final.csv",
    ):
        self.file_path = file_path
        self.output_path = output_path

        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Nie znaleziono pliku: {file_path}. Uruchom najpierw HourlyDataPreprocessor!"
            )

        # 1. low_memory=False zapobiega ostrzeżeniom DtypeWarning przy wczytywaniu
        self.df = pd.read_csv(file_path, low_memory=False)

        # 2. Standaryzacja kolumny czasu z format="mixed" oraz utc=True
        time_col = "timestamp" if "timestamp" in self.df.columns else "date"
        if time_col not in self.df.columns:
            time_col = self.df.columns[0]

        self.df["timestamp"] = pd.to_datetime(
            self.df[time_col], format="mixed", utc=True
        ).dt.tz_localize(None)

        if time_col != "timestamp":
            self.df.drop(columns=[time_col], inplace=True)

        self.df = self.df.sort_values("timestamp").reset_index(drop=True)

    def generate_features(self):
        print("=== GENEROWANIE CECH GODZINOWYCH (FEATURE ENGINEERING) ===")
        df = self.df.copy()

        # Szukamy właściwej kolumny cenowej (Close / close / price)
        price_col = None
        for col in ["Close", "close", "price"]:
            if col in df.columns:
                price_col = col
                break

        if price_col is None:
            raise KeyError(
                f"Brak kolumny cenowej ('Close', 'close' lub 'price') w DataFrame! Dostępne: {list(df.columns)}"
            )

        # Konwersja kolumny cenowej na float w razie gdyby była obiektem
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")

        # ---------------------------------------------------------------------
        # 1. STOPY ZWROTU I ZMIENNOŚĆ (Price Returns & Volatility)
        # ---------------------------------------------------------------------
        print("  [1/5] Obliczanie stóp zwrotu i wskaźników technicznych...")

        # Logarytmiczne stopy zwrotu (1h oraz dobowe 24h)
        df["log_return_1h"] = np.log(df[price_col] / df[price_col].shift(1))
        df["log_return_24h"] = np.log(df[price_col] / df[price_col].shift(24))

        # Średnie kroczące SMA dla interwałów godzinowych: 24h (1d), 168h (7d), 720h (30d)
        df["sma_24h"] = df[price_col].rolling(window=24).mean()
        df["sma_168h"] = df[price_col].rolling(window=168).mean()
        df["sma_720h"] = df[price_col].rolling(window=720).mean()

        # RSI - Relative Strength Index (14 godzin oraz 168 godzin / 7 dni)
        delta = df[price_col].diff()
        gain_14 = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss_14 = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs_14 = gain_14 / (loss_14 + 1e-9)
        df["rsi_14h"] = 100 - (100 / (1 + rs_14))

        # MACD (Moving Average Convergence Divergence - 12, 26, 9 godzin)
        ema_12 = df[price_col].ewm(span=12, adjust=False).mean()
        ema_26 = df[price_col].ewm(span=26, adjust=False).mean()
        df["macd_line"] = ema_12 - ema_26
        df["macd_signal"] = df["macd_line"].ewm(span=9, adjust=False).mean()
        df["macd_hist"] = df["macd_line"] - df["macd_signal"]

        # Wstęgi Bollingera (20 godzin)
        sma_20 = df[price_col].rolling(window=20).mean()
        std_20 = df[price_col].rolling(window=20).std()
        df["bollinger_upper"] = sma_20 + (std_20 * 2)
        df["bollinger_lower"] = sma_20 - (std_20 * 2)
        df["bollinger_bandwidth"] = (
            df["bollinger_upper"] - df["bollinger_lower"]
        ) / (sma_20 + 1e-9)

        # ---------------------------------------------------------------------
        # 2. RYNEK OPCJI I GREKI (Deribit Options & Volatility)
        # ---------------------------------------------------------------------
        if "dvol_close" in df.columns:
            print(
                "  [2/5] Obliczanie wskaźników rynku opcji (Deribit DVOL/Delta)..."
            )
            df["dvol_close"] = pd.to_numeric(df["dvol_close"], errors="coerce")

            # 24-godzinna zmiana indeksu DVOL
            df["dvol_pct_change_24h"] = df["dvol_close"].pct_change(24)

            # Z-score DVOL z okna 7-dniowego (168h)
            dvol_mean_168 = df["dvol_close"].rolling(168).mean()
            dvol_std_168 = df["dvol_close"].rolling(168).std()
            df["dvol_zscore_168h"] = (df["dvol_close"] - dvol_mean_168) / (
                dvol_std_168 + 1e-9
            )

            # DVOL=60 -> 60% rocznie -> ~3.14% oczekiwanego ruchu dziennie

            implied_daily_move_pct = (df["dvol_close"] / 100.0) / np.sqrt(365)
            implied_upper_bound = df[price_col] * (1 + implied_daily_move_pct)
            implied_lower_bound = df[price_col] * (1 - implied_daily_move_pct)

            # 3. Flagi binarne: Wybicia z widełek wycenionych 24h wcześniej (shift 24h)
            df["price_above_options_upper"] = (
                    df[price_col] > implied_upper_bound.shift(24)
            ).astype(int)
            df["price_below_options_lower"] = (
                    df[price_col] < implied_lower_bound.shift(24)
            ).astype(int)

            # 4. Delta Spread
            if (
                "delta_call_atm_30d" in df.columns
                and "delta_put_atm_30d" in df.columns
            ):
                df["delta_call_atm_30d"] = pd.to_numeric(
                    df["delta_call_atm_30d"], errors="coerce"
                )

                # 1. Odchylenie Delty ATM od poziomu neutralnego 0.5 (Zmienia się w czasie!)
                df["delta_call_bias_30d"] = df["delta_call_atm_30d"] - 0.5

                # 2. Pęd Delty Call (Zmiana 24h)
                df["delta_call_momentum_24h"] = df[
                    "delta_call_atm_30d"
                ].diff(24)

        # ---------------------------------------------------------------------
        # 3. TRANSAKCJE WIELORYBÓW (Whale Metrics - Dune & Market)
        # ---------------------------------------------------------------------
        print("  [3/5] Obliczanie wskaźników transakcji wielorybów...")

        if "whale_tx_count" in df.columns:
            df["whale_tx_count"] = pd.to_numeric(
                df["whale_tx_count"], errors="coerce"
            )
            w_mean = df["whale_tx_count"].rolling(168).mean()
            w_std = df["whale_tx_count"].rolling(168).std()
            df["whale_tx_count_zscore_168h"] = (
                df["whale_tx_count"] - w_mean
            ) / (w_std + 1e-9)

        if "total_whale_volume_btc" in df.columns:
            df["total_whale_volume_btc"] = pd.to_numeric(
                df["total_whale_volume_btc"], errors="coerce"
            )
            df["whale_vol_sum_24h"] = (
                df["total_whale_volume_btc"].rolling(24).sum()
            )
            df["whale_vol_sum_168h"] = (
                df["total_whale_volume_btc"].rolling(168).sum()
            )

        # ---------------------------------------------------------------------
        # 4. PRZEPŁYWY STABLECOINÓW (Stablecoin Liquidity - Dune)
        # ---------------------------------------------------------------------
        if "usdt_volume_usd" in df.columns and "usdc_volume_usd" in df.columns:
            print("  [4/5] Obliczanie wskaźników płynności stablecoinów...")
            df["usdt_volume_usd"] = pd.to_numeric(
                df["usdt_volume_usd"], errors="coerce"
            )
            df["usdc_volume_usd"] = pd.to_numeric(
                df["usdc_volume_usd"], errors="coerce"
            )

            df["total_stablecoin_vol_usd"] = (
                df["usdt_volume_usd"].fillna(0)
                + df["usdc_volume_usd"].fillna(0)
            )
            df["stablecoin_vol_sma_24h"] = (
                df["total_stablecoin_vol_usd"].rolling(24).mean()
            )
            df["stablecoin_vol_momentum_24h"] = df[
                "total_stablecoin_vol_usd"
            ].pct_change(24)

        if "ln_total_capacity_btc" in df.columns :
            print("  Obliczanie dynamicznych wskaźników Lightning Network...")

            # 1. Zmiana pojemności LN (24h i 7d)
            df["ln_capacity_btc_change_24h"] = df[
                "ln_total_capacity_btc"
            ].diff(24)
            df["ln_capacity_btc_change_7d"] = df["ln_total_capacity_btc"].diff(
                168
            )

            # 2. Dynamiczny % udział LN w całkowitej podaży BTC
            if "btc_total_supply" in df.columns :
                df["ln_supply_ratio_pct"] = (df["ln_total_capacity_btc"] / df["btc_total_supply"]) * 100

        #HODL
        if "btc_total_supply" in df.columns and "volume_usdt" in df.columns :
            # 1. Kapitalizacja rynkowa (Market Cap)
            df["market_cap_usd"] = df[price_col] * df["btc_total_supply"]

            # 2. Wskaźnik NVT (Kapitalizacja / 24h Wolumen)
            # Wykorzystujemy 24h sumę wolumenu, aby wygładzić szum godzinowy
            volume_24h = df["volume_usdt"].rolling(24).sum()
            df["nvt_ratio"] = df["market_cap_usd"] / (volume_24h + 1e-9)

            # 3. NVT Signal (Z-score NVT z okna 30-dniowego)
            nvt_mean_720 = df["nvt_ratio"].rolling(720).mean()
            nvt_std_720 = df["nvt_ratio"].rolling(720).std()
            df["nvt_zscore_30d"] = (df["nvt_ratio"] - nvt_mean_720) / (nvt_std_720 + 1e-9)

        # ---------------------------------------------------------------------
        # 5. CZYSZCZENIE I ZAPIS KOŃCOWEGO ZBIORU
        # ---------------------------------------------------------------------
        print("  [5/5] Czyszczenie wartości NaN po okienkach kroczących...")

        # Usuwamy wiersze z początkowego okna SMA (720 godzin = 30 dni)
        initial_len = len(df)
        df.dropna(subset=["sma_720h"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        print(
            f"  Odrzucono {initial_len - len(df)} początkowych wierszy rozgrzewkowych (okno 30-dniowe SMA)."
        )

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)

        print(f"\nSUKCES! Gotowy zestaw cech zapisano do: {self.output_path}")
        print(
            f"Zakres dat: {df['timestamp'].min()} → {df['timestamp'].max()}"
        )
        print(
            f"Wymiary zbioru danych: {len(df)} wierszy x {len(df.columns)} kolumn"
        )

        return df


if __name__ == "__main__":
    engineer = HourlyFeatureEngineer()
    final_df = engineer.generate_features()

    print("\nOstatnie 5 wierszy gotowego pliku:")
    cols_to_show = [
        col
        for col in [
            "timestamp",
            "close",
            "log_return_1h",
            "rsi_14h",
            "macd_hist",
            "dvol_close",
            "delta_spread_30d",
        ]
        if col in final_df.columns
    ]
    print(final_df[cols_to_show].tail())
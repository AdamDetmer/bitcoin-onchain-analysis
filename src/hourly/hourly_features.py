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

        self.df = pd.read_csv(file_path)

        # Standaryzacja kolumny czasu
        time_col = "timestamp" if "timestamp" in self.df.columns else "date"
        self.df["timestamp"] = pd.to_datetime(self.df[time_col])
        if time_col != "timestamp":
            self.df.drop(columns=[time_col], inplace=True)

        self.df = self.df.sort_values("timestamp").reset_index(drop=True)

    def generate_features(self):
        print("=== GENEROWANIE CECH GODZINOWYCH (FEATURE ENGINEERING) ===")
        df = self.df.copy()

        # Szukamy właściwej kolumny cenowej (close / price)
        price_col = "close" if "close" in df.columns else "price"
        if price_col not in df.columns:
            raise KeyError("Brak kolumny cenowej ('close' lub 'price') w DataFrame!")

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

        # RSI - Relative Strength Index (Standardowe 14 godzin oraz 168 godzin / 7 dni)
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

        # Wstęgi Bollingera (20 godzin, 2 odchylenia standardowe)
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
            print("  [2/5] Obliczanie wskaźników rynku opcji (Deribit DVOL/Delta)...")

            # 24-godzinna zmiana indeksu DVOL
            df["dvol_pct_change_24h"] = df["dvol_close"].pct_change(24)

            # Z-score DVOL z okna 7-dniowego (168h) - wykrywa nagłe skoki strachu/optymizmu
            dvol_mean_168 = df["dvol_close"].rolling(168).mean()
            dvol_std_168 = df["dvol_close"].rolling(168).std()
            df["dvol_zscore_168h"] = (df["dvol_close"] - dvol_mean_168) / (
                dvol_std_168 + 1e-9
            )

            # Delta Spread (Różnica między Deltą Call i Put ATM 30d)
            if (
                "delta_call_atm_30d" in df.columns
                and "delta_put_atm_30d" in df.columns
            ):
                df["delta_spread_30d"] = (
                    df["delta_call_atm_30d"] - df["delta_put_atm_30d"]
                )
                df["delta_call_momentum_24h"] = df[
                    "delta_call_atm_30d"
                ].diff(24)

            # Wybicia z oczekiwanych widełek cenowych opcji (Breaches)
            if "implied_upper_bound_usd" in df.columns:
                df["price_above_options_upper"] = (
                    df[price_col] > df["implied_upper_bound_usd"]
                ).astype(int)
                df["price_below_options_lower"] = (
                    df[price_col] < df["implied_lower_bound_usd"]
                ).astype(int)

        # ---------------------------------------------------------------------
        # 3. TRANSAKCJE WIELORYBÓW (Whale Metrics - Dune & Market)
        # ---------------------------------------------------------------------
        print("  [3/5] Obliczanie wskaźników transakcji wielorybów...")

        if "avg_trade_size_usd" in df.columns:
            # Wygładzona średnia wielkość transakcji na giełdzie (24h i 168h)
            df["avg_trade_size_sma_24h"] = (
                df["avg_trade_size_usd"].rolling(24).mean()
            )
            df["whale_trade_intensity_pct"] = (
                df["avg_trade_size_usd"].pct_change(1)
            )

        if "whale_tx_count" in df.columns:
            # Anomalia liczby dużych przelewów on-chain (>500 BTC) z okna 7-dniowego
            w_mean = df["whale_tx_count"].rolling(168).mean()
            w_std = df["whale_tx_count"].rolling(168).std()
            df["whale_tx_count_zscore_168h"] = (
                df["whale_tx_count"] - w_mean
            ) / (w_std + 1e-9)

        if "total_whale_volume_btc" in df.columns:
            # Pęd wolumenu wielorybów (Suma 24h vs Suma 168h)
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

            # Całkowity wolumen transferów USDT + USDC
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

            # Sumaryczna liczba dużych transakcji stablecoinami (>100k USD)
            if (
                "usdt_whale_tx_count" in df.columns
                and "usdc_whale_tx_count" in df.columns
            ):
                df["total_stablecoin_whale_txs"] = (
                    df["usdt_whale_tx_count"].fillna(0)
                    + df["usdc_whale_tx_count"].fillna(0)
                )
                s_mean = df["total_stablecoin_whale_txs"].rolling(168).mean()
                s_std = df["total_stablecoin_whale_txs"].rolling(168).std()
                df["stable_whale_tx_zscore_168h"] = (
                    df["total_stablecoin_whale_txs"] - s_mean
                ) / (s_std + 1e-9)

            # Tether vs Circle Dominance (Stosunek USDT do USDC)
            df["usdt_dominance_ratio"] = df["usdt_volume_usd"] / (
                df["total_stablecoin_vol_usd"] + 1e-9
            )

        # ---------------------------------------------------------------------
        # 5. CZYSZCZENIE I ZAPIS KOŃCOWEGO ZBIORU
        # ---------------------------------------------------------------------
        print("  [5/5] Czyszczenie wartości NaN po okienkach kroczących...")

        # Usuwamy pierwsze wiersze, które zawierają NaN w wyniku najdłuższego okna (np. SMA 720h)
        initial_len = len(df)
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

        print(
            f"  Odrzucono {initial_len - len(df)} początkowych wierszy zawierających NaN."
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
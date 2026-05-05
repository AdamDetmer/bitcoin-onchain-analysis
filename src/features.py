import pandas as pd
import numpy as np
import os

class FeatureEngineer:
    def __init__(self, file_path="data/processed/bitcoin_master.csv"):
        self.file_path = file_path
        self.df = pd.read_csv(file_path)
        self.df['date'] = pd.to_datetime(self.df['date'])

    def add_technical_indicators(self):
        print("Generowanie cech technicznych...")
        df = self.df.copy()

        # 1. Log Returns (Logarytmiczne stopy zwrotu)
        # LSTM lepiej uczy się zmian procentowych niż surowych cen (które rosną w nieskończoność)
        df['log_return'] = np.log(df['price'] / df['price'].shift(1))

        # 2. SMA (Simple Moving Averages) - Średnie kroczące 7 i 30 dni
        df['sma_7'] = df['price'].rolling(window=7).mean()
        df['sma_30'] = df['price'].rolling(window=30).mean()

        # 3. RSI (Relative Strength Index) - Wskaźnik siły względnej
        delta = df['price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # Wskaźnik SSR (Stablecoin Supply Ratio) - Kapitalizacja BTC / Kapitalizacja Stablecoinów
        # Niskie SSR oznacza dużo "gotówki" z boku czekającej na wejście w BTC (sygnał byczy)
        if 'stablecoin_market_cap' in df.columns and 'market_cap' in df.columns:
            df['ssr_ratio'] = df['market_cap'] / df['stablecoin_market_cap']
            df['stablecoin_flow_usd'] = df['stablecoin_market_cap'].diff()

        if 'estimated-transaction-volume-usd' in df.columns :
            if 'n-transactions' in df.columns :
                # 1. Whale Proxy: Średnia wartość transakcji w USD (skoki oznaczają ruchy wielorybów)
                df['whale_proxy_avg_tx'] = df['estimated-transaction-volume-usd'] / df['n-transactions']
                # Wygładzenie 7-dniowe dla modelu (wyłapanie trendu zamiast jednodniowego szumu)
                df['whale_trend_7d'] = df['whale_proxy_avg_tx'].rolling(window=7).mean()

            if 'market_cap' in df.columns :
                # 2. HODL Proxy (Network Velocity): Jaki % kapitalizacji zmienił dziś właściciela?
                # Niska wartość = silny HODL. Wysoka = dystrybucja/panika.
                df['hodl_velocity'] = df['estimated-transaction-volume-usd'] / df['market_cap']
                # Wygładzenie 30-dniowe dla szerszego kontekstu cyklu
                df['hodl_trend_30d'] = df['hodl_velocity'].rolling(window=30).mean()

            # =======================================================================
            # HODL WAVE FEATURES
            # =======================================================================

            if 'hodl_1yr_supply' in df.columns :
                # Zmiana tygodniowa hodl supply — detektuje moment, gdy HODLerzy zaczynają sprzedawać
                # Ujemna delta = coins opuszczają "silne ręce" → sygnał potencjalnego szczytu
                df['hodl_1yr_delta_7d'] = df['hodl_1yr_supply'].diff(7)

                # Momentum: kierunek trendu HODL (czy rośnie czy maleje?)
                df['hodl_1yr_momentum'] = df['hodl_1yr_supply'].rolling(14).mean() - \
                                          df['hodl_1yr_supply'].rolling(30).mean()

            if 'stale_vs_liquid_ratio' in df.columns :
                # Trend 30-dniowy ratio stalych/płynnych BTC
                # Rośnie → rynek się "zamraża" (bullish long-term), maleje → dystrybucja
                df['hodl_stale_trend_30d'] = df['stale_vs_liquid_ratio'].rolling(30).mean()

            if 'realized_value_usd' in df.columns and 'market_cap' in df.columns :
                # MVRV Ratio (Market Value to Realized Value)
                # > 3.5 = historycznie strefa przegrzania (sprzedawaj)
                # < 1.0 = kapitulacja (kupuj)
                # UWAGA: realized_value_usd z CoinMetrics to "RevAllTime" — nie jest to
                # klasyczny Realized Cap. Jeśli masz dostęp do CapRealUSD, użyj jego.
                df['mvrv_proxy'] = df['market_cap'] / (df['realized_value_usd'] + 1)
                # Znormalizowany MVRV (z-score 365-dniowy) — redukuje bias epoki
                mvrv_mean = df['mvrv_proxy'].rolling(365).mean()
                mvrv_std = df['mvrv_proxy'].rolling(365).std()
                df['mvrv_zscore'] = (df['mvrv_proxy'] - mvrv_mean) / (mvrv_std + 1e-9)

            # =======================================================================
            # WHALE TRANSACTION FEATURES
            # =======================================================================

            if 'whale_tx_above_1m' in df.columns :
                # Anomalia wielorybów: odchylenie od 30-dniowej średniej kroczącej
                # Skoki >2 odchyleń standardowych = niezwykła aktywność "grubych ryb"
                whale_mean = df['whale_tx_above_1m'].rolling(30).mean()
                whale_std = df['whale_tx_above_1m'].rolling(30).std()
                df['whale_1m_zscore'] = (df['whale_tx_above_1m'] - whale_mean) / (whale_std + 1e-9)

            if 'whale_tx_above_100k' in df.columns and 'whale_total_tx_count' in df.columns :
                # Dominacja wielorybów — trend 14-dniowy (wygładza noise weekendów)
                df['whale_dominance_14d'] = (
                        df['whale_tx_above_100k'] / (df['whale_total_tx_count'] + 1) * 100
                ).rolling(14).mean()

            if 'whale_adj_transfer_usd' in df.columns and 'market_cap' in df.columns :
                # Whale Volume Ratio: ile % market cap jest "prawdziwie" transferowane?
                # Wysoki wynik przy spadku ceny = panic selling wielorybów (bearish)
                # Wysoki wynik przy wzroście = akumulacja / dystrybucja
                df['whale_vol_to_mcap'] = df['whale_adj_transfer_usd'] / (df['market_cap'] + 1)
                df['whale_vol_mcap_7d'] = df['whale_vol_to_mcap'].rolling(7).mean()

            if 'whale_median_tx_usd' in df.columns :
                # Zmiana mediany transakcji — gdy "przeciętna" transakcja drożeje,
                # nawet małe portfele wysyłają dużo → szeroka dystrybucja lub hype
                df['whale_median_tx_delta'] = df['whale_median_tx_usd'].pct_change(7)

        # Czyszczenie: Wskaźniki techniczne generują NaN na początku (np. SMA 30 potrzebuje 30 dni)
        df.dropna(inplace=True)
        
        output_path = "data/processed/bitcoin_final.csv"
        df.to_csv(output_path, index=False)
        print(f"Cechy dodane. Plik końcowy zapisany w: {output_path}")
        return df

if __name__ == "__main__":
    engineer = FeatureEngineer()
    final_df = engineer.add_technical_indicators()
    print(final_df[['date', 'price', 'log_return', 'rsi']].tail())
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
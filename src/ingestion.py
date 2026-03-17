import pandas as pd
import yfinance as yf
import requests
import os
import time
from datetime import datetime

class DataIngestor:
    def __init__(self, base_dir="data/raw"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def get_market_data(self, ticker="BTC-USD", period="5y"):
        """
        Pobiera dane rynkowe z Yahoo Finance (zamiast CoinGecko).
        period: '1y', '5y', 'max'
        """
        print(f"Pobieranie danych dla {ticker} z Yahoo Finance (zakres: {period})...")
        try:
            btc = yf.Ticker(ticker)
            df = btc.history(period=period)
            
            # Resetujemy indeks, by data była kolumną
            df = df.reset_index()
            # Wybieramy tylko potrzebne kolumny i zmieniamy nazwy na nasze standardowe
            df = df[['Date', 'Close', 'Volume']]
            df.columns = ['date', 'price', 'total_volume']
            
            # Dodajemy uproszczony market_cap (yfinance nie daje go wprost w history, więc estymujemy lub pomijamy)
            # Na potrzeby modelu price i volume są kluczowe.
            df['market_cap'] = df['price'] * 19000000 # Przybliżona liczba BTC w obiegu
            
            # Normalizacja daty do północy
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.normalize()
            
            path = os.path.join(self.base_dir, "bitcoin_daily_market_data.csv")
            df.to_csv(path, index=False)
            print(f"Sukces! Dane rynkowe (Yahoo) zapisane w: {path}")
            return df
        except Exception as e:
            print(f"Błąd Yahoo Finance: {e}")
            return None

    def get_blockchain_com_data(self, charts=["n-transactions", "hash-rate", "estimated-transaction-volume-usd"]):
        """
        Pobiera dane on-chain z Blockchain.com (5 lat).
        """
        all_onchain_data = []
        for chart in charts:
            print(f"Pobieranie metryki on-chain: {chart}...")
            url = f"https://api.blockchain.info/charts/{chart}"
            params = {
                "timespan": "5years",
                "sampled": "true",
                "format": "json",
                "cors": "true"
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                df_chart = pd.DataFrame(data['values'])
                df_chart.columns = ['timestamp', chart]
                df_chart['date'] = pd.to_datetime(df_chart['timestamp'], unit='s').dt.normalize()
                df_chart.drop(columns=['timestamp'], inplace=True)
                
                all_onchain_data.append(df_chart)
                time.sleep(1) 
            except Exception as e:
                print(f"Błąd przy pobieraniu {chart}: {e}")

        if all_onchain_data:
            final_onchain_df = all_onchain_data[0]
            for next_df in all_onchain_data[1:]:
                final_onchain_df = final_onchain_df.merge(next_df, on='date', how='outer')
            
            path = os.path.join(self.base_dir, "bitcoin_onchain_data.csv")
            final_onchain_df.to_csv(path, index=False)
            print(f"Dane on-chain zapisane w: {path}")
            return final_onchain_df
        return None
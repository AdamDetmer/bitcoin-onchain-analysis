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

    def get_blockchain_com_data(self, charts=["n-transactions", "hash-rate", "estimated-transaction-volume-usd", "n-unique-addresses"]):
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

    def get_active_addresses_data(self, start_time="2014-01-01"):
        # Pobiera dzienną liczbę aktywnych adresów BTC z CoinMetrics Community API.
        print(f"Pobieranie liczby aktywnych adresów z CoinMetrics (od {start_time})...")
        try:
            url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
            params = {
                "assets": "btc",
                "metrics": "AdrActCnt",
                "frequency": "1d",
                "start_time": start_time,
                "page_size": 10000
            }
            all_data = []
            next_page = None

            while True:
                if next_page:
                    params["next_page_token"] = next_page
                response = requests.get(url, params=params)
                response.raise_for_status()
                result = response.json()

                all_data.extend(result["data"])
                next_page = result.get("next_page_token")
                if not next_page:
                    break
                time.sleep(0.5)

            df = pd.DataFrame(all_data)
            df["date"] = pd.to_datetime(df["time"]).dt.tz_localize(None).dt.normalize()
            df["active_addresses"] = pd.to_numeric(df["AdrActCnt"], errors="coerce")
            df = df[["date", "active_addresses"]].dropna()

            path = os.path.join(self.base_dir, "active_addresses_data.csv")
            df.to_csv(path, index=False)
            print(f"Sukces! Liczba aktywnych adresów zapisana w: {path} ({len(df)} rekordów)")
            return df
        except Exception as e:
            print(f"Błąd przy pobieraniu aktywnych adresów z CoinMetrics: {e}")
            return None

    def get_lightning_network_data(self):
        """
        Pobiera historyczne dane Lightning Network z mempool.space.
        Pobiera: liczbę kanałów, węzłów oraz pojemność sieci (satoshi).
        """
        print("Pobieranie danych Lightning Network z mempool.space...")
        try:
            # Endpoint zwraca dane dzienne za ~3 lata (najdłuższy dostępny zakres)
            url = "https://mempool.space/api/v1/lightning/statistics/3y"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["added"], unit="s").dt.normalize()
            # Historyczne API mempool nie ma 'node_count', ale ma sumę składowych
            if "node_count" not in df.columns:
                node_cols = ["tor_nodes", "clearnet_nodes", "unannounced_nodes", "clearnet_tor_nodes"]
                available_cols = [c for c in node_cols if c in df.columns]
                df["ln_node_count"] = df[available_cols].sum(axis=1)
            else:
                df = df.rename(columns={"node_count": "ln_node_count"})

            df = df.rename(columns={
                "channel_count": "ln_channel_count",
                "total_capacity": "ln_total_capacity_sat"
            })

            # Zachowaj tylko kluczowe kolumny i filtruj anomalie (channel_count=0 to błędy)
            df = df[["date", "ln_channel_count", "ln_node_count", "ln_total_capacity_sat"]]
            df = df[df["ln_channel_count"] > 0]

            # Pojemność z satoshi na BTC (czytelniejsze)
            df["ln_total_capacity_btc"] = df["ln_total_capacity_sat"] / 1e8

            # Usuń duplikaty dat (zachowaj ostatni rekord dla danej daty)
            df = df.sort_values("date").drop_duplicates(subset="date", keep="last")
            df = df.reset_index(drop=True)

            path = os.path.join(self.base_dir, "lightning_network_data.csv")
            df.to_csv(path, index=False)
            print(f"Sukces! Dane LN zapisane w: {path} ({len(df)} rekordów)")
            print(f"   Zakres: {df['date'].min().date()} → {df['date'].max().date()}")
            return df
        except Exception as e:
            print(f"Błąd przy pobieraniu danych Lightning Network: {e}")
            return None

    def get_stablecoin_data(self):
        """
        Pobiera całkowitą kapitalizację rynkową stablecoinów z DefiLlama (darmowe API).
        """
        print("Pobieranie danych o stablecoinach z DefiLlama...")
        try:
            url = "https://stablecoins.llama.fi/stablecoincharts/all"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Ekstrakcja danych
            df = pd.DataFrame(data)

            # NAPRAWA 1: Wymuszenie typu numerycznego dla daty przed konwersją
            df['date'] = pd.to_numeric(df['date'])
            df['date'] = pd.to_datetime(df['date'], unit='s').dt.normalize()

            # NAPRAWA 2: Wyciągnięcie wartości z zagnieżdżonego słownika {'peggedUSD': wartość}
            df['stablecoin_market_cap'] = df['totalCirculatingUSD'].apply(
                lambda x: x.get('peggedUSD') if isinstance(x, dict) else x
            )

            # Filtrowanie i zmiana nazw
            df = df[['date', 'stablecoin_market_cap']]

            path = os.path.join(self.base_dir, "stablecoin_data.csv")
            df.to_csv(path, index=False)
            print(f"Sukces! Dane stablecoin zapisane w: {path}")
            return df
        except Exception as e:
            print(f"Błąd przy pobieraniu danych stablecoin: {e}")
            return None

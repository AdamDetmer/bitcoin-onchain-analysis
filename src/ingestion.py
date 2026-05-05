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

    def get_exchange_flows_coinmetrics(self, start_time="2019-01-01"):
        """
        Pobiera dane o przepływach giełdowych z darmowego API CoinMetrics.
        Metryki: ExNetFlow (Netto), ExInflow (Napływ), ExOutflow (Wypływ).
        """
        print(f"Pobieranie przepływów giełdowych z CoinMetrics (od {start_time})...")
        try:
            url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
            params = {
                "assets": "btc",
                "metrics": "ExNetFlow",
                "frequency": "1d",
                "start_time": start_time,
                "page_size": 10000
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            result = response.json()

            df = pd.DataFrame(result["data"])
            df["date"] = pd.to_datetime(df["time"]).dt.tz_localize(None).dt.normalize()

            # Konwersja metryk na liczby
            df["net_flow"] = pd.to_numeric(df["ExNetFlow"], errors="coerce")
            df["inflow"] = pd.to_numeric(df["ExInflow"], errors="coerce")
            df["outflow"] = pd.to_numeric(df["ExOutflow"], errors="coerce")

            df = df[["date", "net_flow", "inflow", "outflow"]].dropna()

            path = os.path.join(self.base_dir, "exchange_flows_coinmetrics.csv")
            df.to_csv(path, index=False)
            print(f"Sukces! Przepływy zapisane w: {path}")
            return df
        except Exception as e:
            print(f"Błąd CoinMetrics (Exchange Flows): {e}")
            return None
    def get_hodl_wave_data(self, start_time="2014-01-01"):
        """
        Pobiera dane HODL Wave z CoinMetrics Community API (bezpłatne, bez klucza).

        Metryki:
        - SplyAct1yr:    % supply, który NIE ruszył się przez >1 rok (HODL proxy)
        - SplyAct180d:   % supply aktywny w ostatnich 180 dniach (STH proxy)
        - SplyActEver:   łączna podaż, która kiedykolwiek się ruszyła

        Interpretacja dla modelu:
        - Rosnący SplyAct1yr = akumulacja / HODL → sygnał byczy długoterminowo
        - Spadający SplyAct1yr przy wzroście ceny = wieloryby sprzedają szczyty
        """
        print(f"Pobieranie danych HODL Wave z CoinMetrics Community (od {start_time})...")

        # Te metryki są DARMOWE w Community API — sprawdzone przez CoinMetrics docs
        metrics_to_fetch = [
            "SplyAct1yr",    # Supply nieruszone >1 rok (core HODL wave)
            "SplyAct180d",   # Supply aktywne w <180 dni (short-term holders)
            "SplyAct30d",    # Supply aktywne w <30 dni (spekulanci)
            "RevAllTime",    # Realized Value — proxy wyceny rynku przez "smart money"
        ]

        url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        all_data = []
        next_page = None

        params = {
            "assets": "btc",
            "metrics": ",".join(metrics_to_fetch),
            "frequency": "1d",
            "start_time": start_time,
            "page_size": 10000,
        }

        try:
            while True:
                if next_page:
                    params["next_page_token"] = next_page
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                result = response.json()
                all_data.extend(result["data"])
                next_page = result.get("next_page_token")
                if not next_page:
                    break
                time.sleep(0.5)

            df = pd.DataFrame(all_data)
            df["date"] = pd.to_datetime(df["time"]).dt.tz_localize(None).dt.normalize()

            # Konwersja metryk na numeryczne
            for col in metrics_to_fetch:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.rename(columns={
                "SplyAct1yr":  "hodl_1yr_supply",   # BTC nieruszone >1 rok
                "SplyAct180d": "hodl_180d_supply",   # BTC aktywne <180d (STH)
                "SplyAct30d":  "hodl_30d_supply",    # BTC aktywne <30d (spekulanci)
                "RevAllTime":  "realized_value_usd", # Realized cap USD
            })

            # Oblicz hodl_ratio: jaki % podaży siedzi w "silnych rękach" (>1 rok)?
            # Wymagana całkowita podaż — estymacja ze stałej (dokładniejsza niż magic number)
            if "hodl_1yr_supply" in df.columns and "hodl_30d_supply" in df.columns:
                # Różnica: BTC trzymane 30d-1yr to "chwiejne" ręce
                df["stale_vs_liquid_ratio"] = df["hodl_1yr_supply"] / (df["hodl_30d_supply"] + 1)

            keep_cols = ["date"] + [c for c in [
                "hodl_1yr_supply", "hodl_180d_supply", "hodl_30d_supply",
                "realized_value_usd", "stale_vs_liquid_ratio"
            ] if c in df.columns]

            df = df[keep_cols].dropna(subset=["hodl_1yr_supply"])

            path = os.path.join(self.base_dir, "hodl_wave_data.csv")
            df.to_csv(path, index=False)
            print(f"Sukces! Dane HODL Wave zapisane w: {path} ({len(df)} rekordów)")
            print(f"   Zakres: {df['date'].min().date()} → {df['date'].max().date()}")
            return df

        except Exception as e:
            print(f"Błąd przy pobieraniu HODL Wave z CoinMetrics: {e}")
            return None

    def get_whale_transaction_data(self, start_time="2014-01-01"):
        """
        Pobiera proxy dla ruchów wielorybów z CoinMetrics Community API + Blockchain.com.

        Strategia dwóch źródeł (oba bezpłatne):

        1. CoinMetrics Community → TxTfrValAdjUSD + TxCnt100kUSD (liczba tx >100k USD)
           - TxTfrValAdjUSD: Wartość przesyłów oczyszczona z "change outputs" (noise)
                             Dużo dokładniejsza od surowego volume — proxy "prawdziwego"
                             przepływu kapitału między portfelami.
           - TxCnt10kUSD, TxCnt100kUSD, TxCnt1mUSD: liczba transakcji powyżej progu $
                             KLUCZOWE: skoki w TxCnt1mUSD = wieloryby się ruszają.

        2. Blockchain.com → median-transaction-value (mediana wartości tx w USD)
           - Mediana jest odporna na outliery — gdy rośnie, WSZYSCY wysyłają więcej,
             nie tylko jeden wieloryb. Komplementarne do metryk CoinMetrics.

        Połączone dają solidny, darmowy "whale fingerprint" bez płatnych API.
        """
        print(f"Pobieranie danych ruchów wielorybów (od {start_time})...")

        # --- Część 1: CoinMetrics Community (bez klucza) ---
        cm_metrics = [
            "TxTfrValAdjUSD",  # Adjusted transfer value USD (bez change outputs)
            "TxCnt",           # Łączna liczba transakcji
            "TxCnt10kUSD",     # Liczba tx > $10k — "średnia ryba"
            "TxCnt100kUSD",    # Liczba tx > $100k — duży gracz
            "TxCnt1mUSD",      # Liczba tx > $1M — wieloryb
        ]

        url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        all_data = []
        next_page = None
        params = {
            "assets": "btc",
            "metrics": ",".join(cm_metrics),
            "frequency": "1d",
            "start_time": start_time,
            "page_size": 10000,
        }

        cm_df = None
        try:
            while True:
                if next_page:
                    params["next_page_token"] = next_page
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                result = r.json()
                all_data.extend(result["data"])
                next_page = result.get("next_page_token")
                if not next_page:
                    break
                time.sleep(0.5)

            cm_df = pd.DataFrame(all_data)
            cm_df["date"] = pd.to_datetime(cm_df["time"]).dt.tz_localize(None).dt.normalize()

            for col in cm_metrics:
                if col in cm_df.columns:
                    cm_df[col] = pd.to_numeric(cm_df[col], errors="coerce")

            cm_df = cm_df.rename(columns={
                "TxTfrValAdjUSD": "whale_adj_transfer_usd",  # Oczyszczony wolumen USD
                "TxCnt":          "whale_total_tx_count",
                "TxCnt10kUSD":    "whale_tx_above_10k",      # Tx > $10k
                "TxCnt100kUSD":   "whale_tx_above_100k",     # Tx > $100k ← kluczowe
                "TxCnt1mUSD":     "whale_tx_above_1m",       # Tx > $1M ← wieloryby
            })

            # Wskaźnik dominacji wielorybów: % wszystkich tx to "wielorybie"
            if "whale_tx_above_100k" in cm_df.columns and "whale_total_tx_count" in cm_df.columns:
                cm_df["whale_dominance_pct"] = (
                    cm_df["whale_tx_above_100k"] / (cm_df["whale_total_tx_count"] + 1) * 100
                )

            # Trend 7-dniowy dla modelu (filtruje jednorazowe szoki)
            if "whale_tx_above_1m" in cm_df.columns:
                cm_df["whale_tx_1m_7d_avg"] = cm_df["whale_tx_above_1m"].rolling(7).mean()

            keep = ["date"] + [c for c in [
                "whale_adj_transfer_usd", "whale_total_tx_count",
                "whale_tx_above_10k", "whale_tx_above_100k", "whale_tx_above_1m",
                "whale_dominance_pct", "whale_tx_1m_7d_avg"
            ] if c in cm_df.columns]
            cm_df = cm_df[keep]
            print(f"   CoinMetrics: {len(cm_df)} rekordów whale tx")

        except Exception as e:
            print(f"   Uwaga: CoinMetrics whale TX nie powiodło się: {e}")
            cm_df = None

        # --- Część 2: Blockchain.com — mediana wartości transakcji ---
        bchain_df = None
        try:
            print("   Pobieranie mediany tx z Blockchain.com...")
            url2 = "https://api.blockchain.info/charts/median-transaction-value"
            params2 = {
                "timespan": "5years",
                "sampled": "true",
                "format": "json",
                "cors": "true"
            }
            r2 = requests.get(url2, params=params2, timeout=30)
            r2.raise_for_status()
            data2 = r2.json()

            bchain_df = pd.DataFrame(data2["values"])
            bchain_df.columns = ["timestamp", "whale_median_tx_usd"]
            bchain_df["date"] = pd.to_datetime(bchain_df["timestamp"], unit="s").dt.normalize()
            bchain_df = bchain_df[["date", "whale_median_tx_usd"]]
            print(f"   Blockchain.com mediana: {len(bchain_df)} rekordów")
            time.sleep(1)

        except Exception as e:
            print(f"   Uwaga: Blockchain.com median TX nie powiodło się: {e}")

        # --- Łączenie obu źródeł ---
        if cm_df is not None and bchain_df is not None:
            final_df = pd.merge(cm_df, bchain_df, on="date", how="outer")
        elif cm_df is not None:
            final_df = cm_df
        elif bchain_df is not None:
            final_df = bchain_df
        else:
            print("Błąd: Oba źródła danych whale TX zawiodły.")
            return None

        final_df = final_df.sort_values("date").reset_index(drop=True)

        path = os.path.join(self.base_dir, "whale_transaction_data.csv")
        final_df.to_csv(path, index=False)
        print(f"Sukces! Dane whale TX zapisane w: {path} ({len(final_df)} rekordów)")
        return final_df
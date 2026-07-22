import os
import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests
from scipy.stats import norm
import yaml


class HourlyDataIngestor :
    def __init__(self, base_dir="data/hourly/raw") :
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

        secrets = yaml.safe_load(open("../../secrets.yml"))
        self.DUNE_API_KEY = secrets['dune_api_key']
        self.WHALE_QUERY_ID = secrets['dune_whale_query_id']
        self.STABLECOIN_QUERY_ID = secrets["dune_stable_coin_query_id"]

        self.headers = {"X-Dune-API-Key" : self.DUNE_API_KEY}

    def get_hourly_price(self):
        url = "https://www.cryptodatadownload.com/cdd/Binance_BTCUSDT_1h.csv"
        df = pd.read_csv(url, skiprows=1)
        df = df[
            [
                "Date",
                "Volume USDT",
                "Volume BTC",
                "tradecount",
                "Open",
                "High",
                "Low",
                "Close",
            ]
        ]
        path = os.path.join(self.base_dir, "bitcoin_hourly_market_data.csv")
        df.to_csv(path, index=False)
        return df

    def get_dune_data(self, query_id):
        execute_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
        response = requests.post(execute_url, headers=self.headers)
        if response.status_code != 200:
            print(f"Błąd uruchamiania: {response.text}")
            return pd.DataFrame()

        execution_id = response.json()["execution_id"]
        status_url = (
            f"https://api.dune.com/api/v1/execution/{execution_id}/status"
        )
        while True:
            status_res = requests.get(status_url, headers=self.headers).json()
            state = status_res["state"]
            if state == "QUERY_STATE_COMPLETED":
                break
            elif state in ["QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"]:
                return pd.DataFrame()
            time.sleep(15)

        results_url = (
            f"https://api.dune.com/api/v1/execution/{execution_id}/results"
        )
        results_res = requests.get(results_url, headers=self.headers).json()
        df = pd.DataFrame(results_res["result"]["rows"])

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        return df

    def get_whale_data(self):
        df = self.get_dune_data(self.WHALE_QUERY_ID)
        path = os.path.join(self.base_dir, "bitcoin_hourly_whale_data.csv")
        df.to_csv(path, index=False)
        return df

    def get_stablecoin_data(self):
        df = self.get_dune_data(self.STABLECOIN_QUERY_ID)
        path = os.path.join(
            self.base_dir, "bitcoin_hourly_stablecoin_data.csv"
        )
        df.to_csv(path, index=False)
        return df

    def get_btc_options_dvol_and_delta_hourly(
        self, days_back=365 * 5, risk_free_rate=0.0
    ):
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        step_days = 30
        current_start = start_time

        all_dvol = []
        all_btc = []

        while current_start < end_time:
            current_end = min(current_start + timedelta(days=step_days), end_time)
            start_ms = int(current_start.timestamp() * 1000)
            end_ms = int(current_end.timestamp() * 1000)

            # DVOL Index
            url_dvol = (
                "https://www.deribit.com/api/v2/public/get_volatility_index_data"
            )
            res_dvol = requests.get(
                url_dvol,
                params={
                    "currency": "BTC",
                    "start_timestamp": start_ms,
                    "end_timestamp": end_ms,
                    "resolution": "3600",
                },
            ).json()

            if "result" in res_dvol and "data" in res_dvol["result"]:
                for row in res_dvol["result"]["data"]:
                    all_dvol.append(
                        {
                            "timestamp": pd.to_datetime(row[0], unit="ms"),
                            "dvol_close": row[4],
                        }
                    )

            # BTC-PERPETUAL
            url_btc = (
                "https://www.deribit.com/api/v2/public/get_tradingview_chart_data"
            )
            res_btc = requests.get(
                url_btc,
                params={
                    "instrument_name": "BTC-PERPETUAL",
                    "start_timestamp": start_ms,
                    "end_timestamp": end_ms,
                    "resolution": "60",
                },
            ).json()

            if "result" in res_btc and "ticks" in res_btc["result"]:
                ticks = res_btc["result"]
                for i in range(len(ticks["ticks"])):
                    all_btc.append(
                        {
                            "timestamp": pd.to_datetime(
                                ticks["ticks"][i], unit="ms"
                            ),
                            "btc_price": ticks["close"][i],
                        }
                    )

            current_start = current_end
            time.sleep(0.3)

        df_dvol = (
            pd.DataFrame(all_dvol)
            .drop_duplicates(subset=["timestamp"])
            .set_index("timestamp")
        )
        df_btc = (
            pd.DataFrame(all_btc)
            .drop_duplicates(subset=["timestamp"])
            .set_index("timestamp")
        )
        df = df_dvol.join(df_btc, how="inner").reset_index()

        # Calculation Greks
        S = df["btc_price"]
        sigma = df["dvol_close"] / 100.0

        def calc_delta(strike, t_days, option_type="call"):
            T_years = t_days / 365.0
            d1 = (
                np.log(S / strike) + (risk_free_rate + 0.5 * (sigma**2)) * T_years
            ) / (sigma * np.sqrt(T_years))
            return norm.cdf(d1) if option_type == "call" else norm.cdf(d1) - 1.0

        df["delta_call_atm_30d"] = calc_delta(S, 30, "call")
        df["delta_put_atm_30d"] = calc_delta(S, 30, "put")

        path = os.path.join(self.base_dir, "bitcoin_hourly_options_data.csv")
        df.to_csv(path, index=False)
        return df


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
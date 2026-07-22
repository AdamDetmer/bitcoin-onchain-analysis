import os
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import yaml
from scipy.stats import norm



class HourlyDataIngestor :
	def __init__(self, base_dir="data/hourly/raw") :
		self.base_dir = base_dir
		os.makedirs(self.base_dir, exist_ok=True)

		secrets = yaml.safe_load(open("../../secrets.yml"))
		self.DUNE_API_KEY = secrets['dune_api_key']
		self.WHALE_QUERY_ID = secrets['dune_whale_query_id']
		self.STABLECOIN_QUERY_ID = secrets["dune_stable_coin_query_id"]

		self.headers = {"X-Dune-API-Key" : self.DUNE_API_KEY}

	def get_hourly_price(self) :
		url = "https://www.cryptodatadownload.com/cdd/Binance_BTCUSDT_1h.csv"
		df = pd.read_csv(url, skiprows=1)
		print(df.columns)
		df = df[["Date", "Volume USDT", 'Volume BTC', "tradecount", 'Open', "High", "Low", "Close"]]
		path = os.path.join(self.base_dir, "bitcoin_hourly_market_data.csv")
		df.to_csv(path, index=False)
		return df

	def get_dune_data(self, query_id) :
		execute_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
		response = requests.post(execute_url, headers=self.headers)

		if response.status_code != 200 :
			print(f"Błąd uruchamiania: {response.text}")
			return pd.DataFrame()

		execution_id = response.json()["execution_id"]

		status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
		while True :
			status_res = requests.get(status_url, headers=self.headers).json()
			state = status_res["state"]
			print(f"Status zapytania: {state}...")

			if state == "QUERY_STATE_COMPLETED" :
				print("Obliczenia zakończone!")
				break
			elif state in ["QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"] :
				print("Błąd wykonania zapytania na Dune.")
				return pd.DataFrame()

			time.sleep(15)

		results_url = (
			f"https://api.dune.com/api/v1/execution/{execution_id}/results"
		)
		results_res = requests.get(results_url, headers=self.headers).json()

		rows = results_res["result"]["rows"]

		df = pd.DataFrame(rows)

		if "timestamp" in df.columns :
			df["timestamp"] = pd.to_datetime(df["timestamp"])
			df.set_index("timestamp", inplace=True)

		return df

	def get_whale_data(self) :

		df = self.get_dune_data(self.WHALE_QUERY_ID)
		path = os.path.join(self.base_dir, "bitcoin_hourly_whale_data.csv")
		df.to_csv(path, index=False)
		return df

	def get_stablecoin_data(self) :
		df = self.get_dune_data(self.STABLECOIN_QUERY_ID)
		path = os.path.join(self.base_dir, "bitcoin_hourly_stablecoin_data.csv")
		df.to_csv(path, index=False)
		return df

	def get_btc_options_dvol_and_delta_hourly(self,days_back=365*5, risk_free_rate=0.0) :
		"""Pobiera dane godzinowe DVOL i cenę BTC z Deribit oraz oblicza Deltę opcji

		zgodnie z grillem Blacka-Scholesa dla różnych terminów wygaśnięcia i
		strike'ów.
		"""
		end_time = datetime.now()
		start_time = end_time - timedelta(days=days_back)

		step_days = 30
		current_start = start_time

		all_dvol = []
		all_btc = []

		print(
			f"Pobieranie danych z Deribit z ostatnich {days_back} dni (interwał 1h)..."
		)

		while current_start < end_time :
			current_end = current_start + timedelta(days=step_days)
			if current_end > end_time :
				current_end = end_time

			start_ms = int(current_start.timestamp() * 1000)
			end_ms = int(current_end.timestamp() * 1000)

			# 1. Endpoint DVOL
			url_dvol = (
				"https://www.deribit.com/api/v2/public/get_volatility_index_data"
			)
			params_dvol = {
				"currency" : "BTC",
				"start_timestamp" : start_ms,
				"end_timestamp" : end_ms,
				"resolution" : "3600",  # 1h
			}

			res_dvol = requests.get(url_dvol, params=params_dvol).json()

			if "result" in res_dvol and "data" in res_dvol["result"] :
				for row in res_dvol["result"]["data"] :
					all_dvol.append(
						{
							"timestamp" : pd.to_datetime(row[0], unit="ms"),
							"dvol_close" : row[4],
						}
					)

			# 2. Endpoint BTC-PERPETUAL
			url_btc = (
				"https://www.deribit.com/api/v2/public/get_tradingview_chart_data"
			)
			params_btc = {
				"instrument_name" : "BTC-PERPETUAL",
				"start_timestamp" : start_ms,
				"end_timestamp" : end_ms,
				"resolution" : "60",  # 1h
			}

			res_btc = requests.get(url_btc, params=params_btc).json()

			if "result" in res_btc and "ticks" in res_btc["result"] :
				ticks = res_btc["result"]
				for i in range(len(ticks["ticks"])) :
					all_btc.append(
						{
							"timestamp" : pd.to_datetime(
								ticks["ticks"][i], unit="ms"
							),
							"btc_price" : ticks["close"][i],
						}
					)

			current_start = current_end
			time.sleep(0.3)

		if not all_dvol or not all_btc :
			print("[BŁĄD] Nie udało się pobrać wystarczających danych.")
			return pd.DataFrame()

		# Tworzenie tabel i fuzja po czasie
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

		df = df_dvol.join(df_btc, how="inner")

		# --- OBLICZENIA ZMIENNOŚCI (CRYPTODATADOWNLOAD) ---
		df["implied_daily_move_pct"] = df["dvol_close"] / np.sqrt(365)
		df["implied_daily_move_usd"] = df["btc_price"] * (
				df["implied_daily_move_pct"] / 100
		)

		# --- OBLICZENIA DELTY W MODELU BLACKA-SCHOLESA ---
		S = df["btc_price"]
		sigma = (
				df["dvol_close"] / 100.0
		)  # Konwersja zmienności z procentów na ułamek dziesiętny
		r = risk_free_rate

		# Pomocnicza funkcja do wyliczania d1 i delty
		def calc_delta(strike, t_days, option_type="call") :
			T_years = t_days / 365.0
			d1 = (np.log(S / strike) + (r + 0.5 * (sigma ** 2)) * T_years) / (
					sigma * np.sqrt(T_years)
			)
			if option_type == "call" :
				return norm.cdf(d1)
			else :
				return norm.cdf(d1) - 1.0

		# 1. Delta dla opcji At-The-Money (ATM, Strike K = Price S)
		df["delta_call_atm_30d"] = calc_delta(strike=S, t_days=30, option_type="call")
		df["delta_put_atm_30d"] = calc_delta(strike=S, t_days=30, option_type="put")

		# 2. Delta dla opcji krótkoterminowych (7 dni) i długoterminowych (90 dni)
		df["delta_call_atm_7d"] = calc_delta(strike=S, t_days=7, option_type="call")
		df["delta_call_atm_90d"] = calc_delta(strike=S, t_days=90, option_type="call")

		# 3. Delta dla opcji Out-of-the-Money (+10% od ceny spot)
		K_otm = S * 1.10
		df["delta_call_otm_10pct_30d"] = calc_delta(
			strike=K_otm, t_days=30, option_type="call"
		)

		print(
			f"\n=== POBIERANIE I OBLICZENIA ZAKOŃCZONE! Pobrano {len(df)} wierszy. ==="
		)


		path = os.path.join(self.base_dir, "bitcoin_hourly_options_data.csv")
		df.to_csv(path, index=False)

		return df


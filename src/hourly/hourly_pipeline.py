import os
import subprocess
import sys

# Importy klas godzinowych z Twojego katalogu src/
# Dostosuj ścieżki importu, jeśli Twoje pliki nazywają się inaczej w katalogu src/
try :
	from src.hourly_features import HourlyFeatureEngineer
	from src.hourly_ingestion import HourlyDataIngestor
	from src.hourly_preprocessing import HourlyDataPreprocessor
except ImportError :
	# Wariant, jeśli odpalasz skrypt bezpośrednio z folderu głównego lub bez struktury pakietowej
	from hourly_features import HourlyFeatureEngineer
	from hourly_ingestion import HourlyDataIngestor
	from hourly_preprocessing import HourlyDataPreprocessor


def run_hourly_pipeline() :
	print("==================================================")
	print("🚀 STARTUJEMY GODZINOWY PIPELINE DANYCH (1H)")
	print("==================================================\n")

	# ---------------------------------------------------------
	# 1. INGESTION (Pobieranie surowych danych godzinowych)
	# ---------------------------------------------------------
	print(" KROK 1: Pobieranie surowych danych godzinowych (Ingestion)...")
	ingestor = HourlyDataIngestor()

	print(" -> [1/4] Pobieranie cen rynkowych BTC/USDT (Binance / Spot)...")
	ingestor.get_hourly_price()

	print(" -> [2/4] Pobieranie danych on-chain o wielorybach (Dune Analytics)...")
	ingestor.get_whale_data()

	print(" -> [3/4] Pobieranie danych o przepływach stablecoinów (Dune Analytics)...")
	ingestor.get_stablecoin_data()

	print(" -> [4/4] Pobieranie indeksu DVOL oraz wyliczanie Delty opcji (Deribit)...")
	# Pobieramy dane z ostatnich 5 lat (365 * 5 dni)
	ingestor.get_btc_options_dvol_and_delta_hourly(days_back=365 * 5)

	print("\n Pobieranie surowych danych zakończone sukcesem!\n")

	# ---------------------------------------------------------
	# 2. PREPROCESSING (Czyszczenie, wyrównanie i fuzja tabel)
	# ---------------------------------------------------------
	print("🧹 KROK 2: Preprocessing, fuzja i czyszczenie szeregów czasowych...")
	preprocessor = HourlyDataPreprocessor()
	master_df = preprocessor.merge_and_clean()

	if master_df is None or master_df.empty :
		print("[CRITICAL ERROR] Preprocessing nie zwrócił danych. Przerywam pipeline.")
		sys.exit(1)

	print("\nPołączono tabele w jeden master dataset godzinowy!\n")

	# ---------------------------------------------------------
	# 3. FEATURE ENGINEERING (Generowanie cech technicznych i on-chain)
	# ---------------------------------------------------------
	print("KROK 3: Generowanie cech technicznych, DVOL Z-Score i Greków...")
	engineer = HourlyFeatureEngineer()
	final_df = engineer.generate_features()

	print("\n PIPELINE DANYCH GODZINOWYCH ZAKOŃCZONY SUKCESEM!")
	print(f" Utworzono zbiorczy plik: data/hourly/processed/bitcoin_hourly_final.csv")
	print(f"Liczba wierszy (godzin): {len(final_df)} | Liczba cech (kolumn): {len(final_df.columns)}")

	# ---------------------------------------------------------
	# 4. WERYFIKACJA I TESTY
	# ---------------------------------------------------------
	test_script = "test_hourly_pipeline.py" if os.path.exists("test_hourly_pipeline.py") else "test_pipeline.py"

	if os.path.exists(test_script) :
		print(f"\n Uruchamiam testy weryfikacyjne z pliku {test_script}...\n")
		subprocess.run([sys.executable, test_script])
	else :
		print("\n️ Pomijam krok testów (brak pliku testowego test_pipeline.py).")


if __name__ == "__main__" :
	run_hourly_pipeline()
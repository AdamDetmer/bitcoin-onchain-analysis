import os
import subprocess
import sys

from hourly_ingestion import HourlyDataIngestor
from hourly_preprocessing import HourlyDataPreprocessor
from hourly_features import HourlyFeatureEngineer


def run_hourly_pipeline():
    print("==================================================")
    print(" STARTUJEMY CZYSZCZONY PIPELINE GODZINOWY (1H)")
    print("==================================================\n")

    # ---------------------------------------------------------
    # 1. INGESTION (Pobieranie danych)
    # ---------------------------------------------------------
    print(" KROK 1: Pobieranie danych...")
    ingestor = HourlyDataIngestor()

    # A. DANE GODZINOWE (1H - Core)
    print(" -> [1H] Pobieranie cen rynkowych BTC/USDT (Binance)...")
    ingestor.get_hourly_price()

    print(" -> [1H] Pobieranie transakcji wielorybów >500 BTC (Dune SQL)...")
    ingestor.get_whale_data()

    print(" -> [1H] Pobieranie przepływów stablecoinów (Dune SQL)...")
    ingestor.get_stablecoin_data()

    print(" -> [1H] Pobieranie DVOL i wyliczanie Delty opcji (Deribit)...")
    ingestor.get_btc_options_dvol_and_delta_hourly(days_back=365 * 5)

    # B. DANE DZIENNE (1D - Strukturalne tło do propagacji na 24h)
    print(" -> [1D] Pobieranie danych HODL Wave (CoinMetrics)...")
    ingestor.get_hodl_wave_data()

    print(" -> [1D] Pobieranie aktywnych adresów (CoinMetrics)...")
    ingestor.get_active_addresses_data()

    print(" -> [1D] Pobieranie statystyk Lightning Network (mempool.space)...")
    ingestor.get_lightning_network_data()

    print("\n Pobieranie danych zakończone sukcesem!\n")

    # ---------------------------------------------------------
    # 2. PREPROCESSING (Fuzja 1H + 1D)
    # ---------------------------------------------------------
    print(" KROK 2: Preprocessing i fuzja danych w jeden Master Dataset...")
    preprocessor = HourlyDataPreprocessor()
    master_df = preprocessor.merge_and_clean()

    if master_df is None or master_df.empty:
        print(" [CRITICAL ERROR] Preprocessing się wyłożył. Przerywam.")
        sys.exit(1)

    # ---------------------------------------------------------
    # 3. FEATURE ENGINEERING (Wskaźniki techniczne + Greki + Z-Score)
    # ---------------------------------------------------------
    print("️ KROK 3: Wyliczanie cech ekonometrycznych i wskaźników...")
    engineer = HourlyFeatureEngineer()
    final_df = engineer.generate_features()

    print("\n==================================================")
    print(" GOTOWE! PIPELINE PRZETWORZYŁ CAŁY ZBIOR DANYCH")
    print("==================================================")
    print(f" Zapisano plik: data/hourly/processed/bitcoin_hourly_final.csv")
    print(f" Wymiary zbioru: {final_df.shape[0]} wierszy (godzin) x {final_df.shape[1]} kolumn\n")

    # ---------------------------------------------------------
    # 4. AUTOMATYCZNY TEST INTEGRALNOŚCI DANYCH
    # ---------------------------------------------------------
    test_script = "test_hourly_pipeline.py"
    if os.path.exists(test_script):
        print(" Uruchamiam testy weryfikacyjne...")
        subprocess.run([sys.executable, test_script])


if __name__ == "__main__":
    run_hourly_pipeline()
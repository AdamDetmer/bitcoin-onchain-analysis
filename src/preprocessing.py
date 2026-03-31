import pandas as pd
import os

class DataPreprocessor:
    def __init__(self, raw_dir="data/raw", processed_dir="data/processed"):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        os.makedirs(self.processed_dir, exist_ok=True)

    def merge_and_clean(self):
        print("Rozpoczynam integrację danych...")
        
        # 1. Wczytywanie plików
        try:
            market_df = pd.read_csv(os.path.join(self.raw_dir, "bitcoin_daily_market_data.csv"))
            onchain_df = pd.read_csv(os.path.join(self.raw_dir, "bitcoin_onchain_data.csv"))
        except FileNotFoundError as e:
            print(f"Błąd: Nie znaleziono plików w {self.raw_dir}. Uruchom najpierw ingestion.py.")
            return None

        # Konwersja daty na format datetime dla obu DF
        market_df['date'] = pd.to_datetime(market_df['date'])
        onchain_df['date'] = pd.to_datetime(onchain_df['date'])

        # 2. Łączenie danych (Inner join, aby mieć tylko pełne dni z obu źródeł)
        master_df = pd.merge(market_df, onchain_df, on='date', how='inner')

        stablecoin_path = os.path.join(self.raw_dir, "stablecoin_data.csv")
        if os.path.exists(stablecoin_path):
            stable_df = pd.read_csv(stablecoin_path)
            stable_df['date'] = pd.to_datetime(stable_df['date'])
            master_df = pd.merge(master_df, stable_df, on='date', how='left')

        active_addr_path = os.path.join(self.raw_dir, "active_addresses_data.csv")
        if os.path.exists(active_addr_path):
            addr_df = pd.read_csv(active_addr_path)
            addr_df['date'] = pd.to_datetime(addr_df['date'])
            master_df = pd.merge(master_df, addr_df, on='date', how='left')
            print(f"  + Aktywne adresy (CoinMetrics): {addr_df['date'].min().date()} → {addr_df['date'].max().date()}")

        ln_path = os.path.join(self.raw_dir, "lightning_network_data.csv")
        if os.path.exists(ln_path):
            ln_df = pd.read_csv(ln_path)
            ln_df['date'] = pd.to_datetime(ln_df['date'])
            master_df = pd.merge(master_df, ln_df, on='date', how='left')
            print(f"  + Lightning Network (mempool.space): {ln_df['date'].min().date()} → {ln_df['date'].max().date()}")
            print(f"    (Uwaga: LN istnieje od 2018 — wiersze przed 2018 mają NaN w kolumnach LN)")

        
        # Sortowanie po dacie (kluczowe dla szeregów czasowych!)
        master_df = master_df.sort_values('date').reset_index(drop=True)

        # 3. Obsługa brakujących wartości (Interpolacja)
        # Jeśli brakuje pojedynczego dnia, uzupełniamy go średnią z sąsiednich dni
        if master_df.isnull().values.any():
            print("Wykryto braki w danych. Stosuję interpolację liniową...")
            master_df = master_df.interpolate(method='linear', limit_direction='both')
        
        # 4. Zapisanie "Master DataFrame"
        output_path = os.path.join(self.processed_dir, "bitcoin_master.csv")
        master_df.to_csv(output_path, index=False)
        
        print(f"Sukces! Master dataset utworzony: {output_path}")
        print(f"Zakres dat: {master_df['date'].min()} do {master_df['date'].max()}")
        print(f"Liczba wierszy: {len(master_df)}")
        
        return master_df

if __name__ == "__main__":
    preprocessor = DataPreprocessor()
    df = preprocessor.merge_and_clean()
    if df is not None:
        print(df.tail())
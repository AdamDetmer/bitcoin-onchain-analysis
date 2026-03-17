import pandas as pd
import os

def check_data():
    path = "data/processed/bitcoin_final.csv"
    
    print("--- WERYFIKACJA PIPELINE'U ---")
    
    # 1. Sprawdzenie czy plik istnieje
    if not os.path.exists(path):
        print("❌ BŁĄD: Plik końcowy nie istnieje!")
        return

    df = pd.read_csv(path)

    # 2. Sprawdzenie brakujących danych
    nulls = df.isnull().sum().sum()
    if nulls == 0:
        print("✅ Brak brakujących wartości (NaN).")
    else:
        print(f"❌ UWAGA: Znaleziono {nulls} pustych komórek!")

    # 3. Sprawdzenie kolumn
    required_cols = ['price', 'log_return', 'rsi', 'n-transactions', 'hash-rate']
    missing_cols = [c for c in required_cols if c not in df.columns]
    
    if not missing_cols:
        print("✅ Wszystkie kluczowe cechy są obecne.")
    else:
        print(f"❌ BRAKUJĄCE KOLUMNY: {missing_cols}")

    # 4. Sprawdzenie ciągłości dat
    df['date'] = pd.to_datetime(df['date'])
    date_diffs = df['date'].diff().dt.days[1:]
    if (date_diffs == 1).all():
        print("✅ Szereg czasowy jest ciągły (brak dziur w dniach).")
    else:
        print("❌ UWAGA: Wykryto przerwy w datach!")

    print(f"\nGotowy zestaw danych ma kształt: {df.shape} (Wiersze, Kolumny)")
    print("------------------------------")

if __name__ == "__main__":
    check_data()
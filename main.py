from src.ingestion import DataIngestor
from src.preprocessing import DataPreprocessor
from src.features import FeatureEngineer
import subprocess
import sys

def run_pipeline():
    print("🚀 STARTUJEMY PIPELINE DANYCH\n")

    # 1. Ingestion
    ingestor = DataIngestor()
    ingestor.get_market_data(period="5y")
    ingestor.get_blockchain_com_data()
    ingestor.get_stablecoin_data()

    # 2. Preprocessing
    preprocessor = DataPreprocessor()
    preprocessor.merge_and_clean()

    # 3. Features
    engineer = FeatureEngineer()
    engineer.add_technical_indicators()

    print("\n✅ PIPELINE ZAKOŃCZONY SUKCESEM!")
    print("Uruchamiam testy weryfikacyjne...\n")
    
    # 4. Wywołanie testu
    subprocess.run([sys.executable, "test_pipeline.py"])

if __name__ == "__main__":
    run_pipeline()
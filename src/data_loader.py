"""
data_loader.py — Interfejs do danych.
Wszystkie operacje na CSV w jednym miejscu.
Notebook i predict.py importują stąd, nigdy nie czytają CSV bezpośrednio.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import joblib
from pathlib import Path

from config import CSV_FINAL, EXOG_COLS, MODELS_DIR


def load_full(path=CSV_FINAL) -> pd.DataFrame:
    """
    Wczytuje bitcoin_final.csv i zwraca DataFrame z indeksem na dacie.
    Automatycznie filtruje do kolumn które faktycznie istnieją w pliku.
    """
    df = pd.read_csv(path, parse_dates=['date']).set_index('date').sort_index()
    available = [c for c in EXOG_COLS if c in df.columns]
    missing   = [c for c in EXOG_COLS if c not in df.columns]
    if missing:
        print(f"[data_loader] Brak kolumn (pomiń lub uzupełnij pipeline): {missing}")
    return df, available


def make_sequences(df: pd.DataFrame, features: list, window: int):
    """
    Buduje sekwencje (X, y) dla LSTM/BLSTM z DataFrame.

    Zwraca:
        X              — (n_samples, window, n_features)
        y              — (n_samples,)  — log-return ceny
        scaler_all     — MinMaxScaler wszystkich cech (do inverse przy predykcji)
        scaler_price   — MinMaxScaler samej ceny (do inverse log-returnu)
        dates          — indeks dat odpowiadających każdej próbce y
    """
    data = df[features].copy().dropna()

    # Log-return ceny (stacjonarność, brak dryftu skali)
    data['price'] = np.log(data['price'] / data['price'].shift(1))
    data = data.dropna()

    scaler_all   = MinMaxScaler(feature_range=(-1, 1))
    scaler_price = MinMaxScaler(feature_range=(-1, 1))
    scaler_price.fit(data[['price']])

    scaled = scaler_all.fit_transform(data)

    X, y, dates = [], [], []
    for i in range(window, len(scaled)):
        X.append(scaled[i - window:i, :])
        y.append(scaled[i, 0])
        dates.append(data.index[i])

    return (
        np.array(X),
        np.array(y),
        scaler_all,
        scaler_price,
        pd.DatetimeIndex(dates),
    )


def train_test_split_ts(X, y, dates, test_days: int):
    """
    Podział czasowy (bez shufflowania).
    Zwraca (X_train, X_test, y_train, y_test, dates_train, dates_test).
    """
    split = len(X) - test_days
    return (
        X[:split], X[split:],
        y[:split], y[split:],
        dates[:split], dates[split:],
    )


def save_scalers(scaler_all, scaler_price, prefix: str):
    """Zapisuje oba scalery do models/ z podanym prefixem (lstm / blstm)."""
    MODELS_DIR.mkdir(exist_ok=True)
    joblib.dump(scaler_all,   MODELS_DIR / f"scaler_{prefix}_all.pkl")
    joblib.dump(scaler_price, MODELS_DIR / f"scaler_{prefix}_price.pkl")
    print(f"[data_loader] Scalery zapisane: models/scaler_{prefix}_*.pkl")


def load_scalers(prefix: str):
    """Wczytuje oba scalery z models/."""
    scaler_all   = joblib.load(MODELS_DIR / f"scaler_{prefix}_all.pkl")
    scaler_price = joblib.load(MODELS_DIR / f"scaler_{prefix}_price.pkl")
    return scaler_all, scaler_price


def select_top_features(df: pd.DataFrame, target_col: str, available_features: list, top_k: int) -> list :
	"""
	Wybiera top_k cech o najwyższej absolutnej korelacji z log-zwrotami celu.
	Używamy korelacji Spearmana (odporniejsza na outliery i zależności nieliniowe).
	"""
	# Obliczamy log-zwroty dla ceny, bo model docelowo na nich operuje
	target_returns = np.log(df[target_col] / df[target_col].shift(1))

	correlations = {}
	for col in available_features :
		# Pamiętaj o usunięciu NaN przed liczeniem korelacji
		corr = target_returns.corr(df[col], method='spearman')
		correlations[col] = abs(corr)

	# Sortowanie malejąco po wartości bezwzględnej korelacji
	sorted_features = sorted(correlations.items(), key=lambda x : x[1], reverse=True)
	top_features = [f[0] for f in sorted_features[:top_k]]

	print(f"[data_loader] Wybrano {top_k} cech: {top_features}")
	return top_features
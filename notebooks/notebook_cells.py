# ════════════════════════════════════════════════════════════════════════════
# KOMÓRKA 1 — Instalacja i import (zastępuje pierwsze 2 komórki notebooka)
# ════════════════════════════════════════════════════════════════════════════
import sys, os
sys.path.insert(0, os.path.abspath('../src'))  # żeby Jupyter widział src/

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import warnings
warnings.filterwarnings('ignore')

# Importy z projektu
from config      import EXOG_COLS, HORIZONS, TEST_DAYS, LSTM_WINDOW, BLSTM_WINDOW
from data_loader import load_full, make_sequences, train_test_split_ts

plt.style.use('dark_background')
plt.rcParams['figure.figsize'] = (14, 5)
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3

print("OK — ścieżka src/ dodana, config i data_loader załadowane")


# ════════════════════════════════════════════════════════════════════════════
# KOMÓRKA 2 — Wczytanie danych przez interfejs (zamiast pd.read_csv wszędzie)
# ════════════════════════════════════════════════════════════════════════════
df_full, available = load_full()
price = df_full['price']

print(f"Wczytano {len(df_full)} wierszy  ({df_full.index[0].date()} → {df_full.index[-1].date()})")
print(f"Dostępne cechy on-chain: {available}")


# ════════════════════════════════════════════════════════════════════════════
# KOMÓRKA 3 — Przygotowanie danych LSTM (zamiast cell 47)
# ════════════════════════════════════════════════════════════════════════════
features_list_lstm = ['price'] + available

X_lstm, y_lstm, scaler_lstm_all, scaler_lstm_price, dates_lstm = make_sequences(
    df_full, features_list_lstm, LSTM_WINDOW
)
X_train_lstm, X_test_lstm, y_train_lstm, y_test_lstm, _, dates_test_lstm = \
    train_test_split_ts(X_lstm, y_lstm, dates_lstm, TEST_DAYS)

print(f"LSTM — X_train: {X_train_lstm.shape}  X_test: {X_test_lstm.shape}")


# ════════════════════════════════════════════════════════════════════════════
# KOMÓRKA 4 — Trening LSTM w notebooku (alternatywnie: python src/train.py)
# ════════════════════════════════════════════════════════════════════════════
# OPCJA A: trenuj tu (wolniej, ale widzisz live output)
# OPCJA B: wyjdź z Jupytera, odpal:  python src/train.py --model lstm
#          i wróć do komórki 5 (wczytanie zapisanego modelu)

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, BatchNormalization
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from config import LSTM_UNITS, DROPOUT, BATCH_SIZE, EPOCHS, LR, PATIENCE, MODEL_LSTM
from data_loader import save_scalers

def build_lstm(input_shape):
    model = Sequential([
        LSTM(LSTM_UNITS[0], return_sequences=True, input_shape=input_shape),
        BatchNormalization(), Dropout(DROPOUT),
        LSTM(LSTM_UNITS[1], return_sequences=False),
        BatchNormalization(), Dropout(DROPOUT),
        Dense(16, activation='relu'), Dense(1),
    ])
    model.compile(optimizer=tf.keras.optimizers.Adam(LR), loss='mse')
    return model

model_lstm = build_lstm(X_train_lstm.shape[1:])
model_lstm.summary()

history_lstm = model_lstm.fit(
    X_train_lstm, y_train_lstm,
    epochs=EPOCHS, batch_size=BATCH_SIZE, validation_split=0.15,
    callbacks=[
        EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True),
        ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=7),
    ], verbose=1,
)

model_lstm.save(MODEL_LSTM)
save_scalers(scaler_lstm_all, scaler_lstm_price, 'lstm')
print(f"Model zapisany: {MODEL_LSTM}")


# ════════════════════════════════════════════════════════════════════════════
# KOMÓRKA 5 — Wczytanie modelu (jeśli trenowałeś przez train.py)
# ════════════════════════════════════════════════════════════════════════════
# model_lstm   = tf.keras.models.load_model(MODEL_LSTM)
# scaler_lstm_all, scaler_lstm_price = load_scalers('lstm')
# print("Model LSTM wczytany z dysku")


# ════════════════════════════════════════════════════════════════════════════
# KOMÓRKA 6 — Predykcja przez interfejs predict_all (na koniec notebooka)
# ════════════════════════════════════════════════════════════════════════════
from predict import predict_all

for h in HORIZONS:
    results = predict_all(h)
    print(results.tail(1))
    print()

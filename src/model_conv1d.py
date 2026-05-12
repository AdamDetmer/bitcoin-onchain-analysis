"""
model_conv1d.py — Conv1D + LSTM (temporal convolutional network hybryd).

Dlaczego Conv1D przed LSTM?
    Warstwy konwolucyjne wychwytują lokalne wzorce (np. formacje świecowe,
    tygodniowe cykle) niezależnie od pozycji w oknie — działają jak filtr
    "feature detector". LSTM dostaje już przetworzone reprezentacje zamiast
    surowych danych, co skraca efektywną sekwencję i zmniejsza problem
    zanikającego gradientu.

    Conv1D → pattern detection (co się powtarza w oknie)
    LSTM   → sekwencyjne zależności między wykrytymi wzorcami
    Dense  → regresja log-returnu

Architektura:
    Conv1D(64,  kernel=3) → ReLU → BN
    Conv1D(128, kernel=3) → ReLU → BN → MaxPool(2)
    LSTM(32)
    Dropout → Dense(16, relu) → Dense(1)
"""

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import (
    Conv1D, MaxPooling1D, LSTM,
    Dense, Dropout, BatchNormalization,
)

from base_model import BitcoinModel
from config import MODEL_CONFIGS, DROPOUT, LR


class Conv1DModel(BitcoinModel):

    _cfg = MODEL_CONFIGS['conv1d']

    @property
    def name(self) -> str:
        return 'conv1d'

    @property
    def window(self) -> int:
        return self._cfg['window']

    def build(self, input_shape: tuple) -> tf.keras.Model:
        filters    = self._cfg['filters']
        kernel     = self._cfg['kernel']
        lstm_units = self._cfg['lstm_units']

        model = Sequential([
            # ── Blok konwolucyjny 1: lokalne wzorce krótkookresowe ────────────
            Conv1D(
                filters=filters[0],
                kernel_size=kernel,
                padding='causal',       # causal — nie patrzy w przyszłość
                activation='relu',
                input_shape=input_shape,
            ),
            BatchNormalization(),

            # ── Blok konwolucyjny 2: wzorce wyższego rzędu ───────────────────
            Conv1D(
                filters=filters[1],
                kernel_size=kernel,
                padding='causal',
                activation='relu',
            ),
            BatchNormalization(),
            MaxPooling1D(pool_size=2),  # redukuje długość sekwencji o połowę

            # ── LSTM: zależności między wykrytymi wzorcami ────────────────────
            LSTM(lstm_units, return_sequences=False),
            Dropout(DROPOUT),

            # ── Regresja ─────────────────────────────────────────────────────
            Dense(16, activation='relu'),
            Dense(1),
        ], name='conv1d')

        model.compile(
            optimizer=tf.keras.optimizers.Adam(LR),
            loss='mse',
        )
        return model

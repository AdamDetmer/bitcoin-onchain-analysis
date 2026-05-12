"""
model_lstm.py — Jednokierunkowy LSTM.
"""

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, BatchNormalization

from base_model import BitcoinModel
from config import MODEL_CONFIGS, DROPOUT, LR


class LSTMModel(BitcoinModel):
    """
    Jednokierunkowy LSTM z dwoma warstwami rekurencyjnymi.

    Architektura:
        LSTM(64) → BN → Dropout
        LSTM(32) → BN → Dropout
        Dense(16, relu) → Dense(1)

    Okno 60 dni — dłuższe niż BLSTM, bo LSTM ma więcej pojemności
    i lepiej wychwytuje powolne trendy (HODL wave, cykle halvingu).
    """

    _cfg = MODEL_CONFIGS['lstm']

    @property
    def name(self) -> str:
        return 'lstm'

    @property
    def window(self) -> int:
        return self._cfg['window']

    def build(self, input_shape: tuple) -> tf.keras.Model:
        units = self._cfg['units']
        model = Sequential([
            LSTM(units[0], return_sequences=True, input_shape=input_shape),
            BatchNormalization(),
            Dropout(DROPOUT),

            LSTM(units[1], return_sequences=False),
            BatchNormalization(),
            Dropout(DROPOUT),

            Dense(16, activation='relu'),
            Dense(1),
        ], name='lstm')

        model.compile(
            optimizer=tf.keras.optimizers.Adam(LR),
            loss='mse',
        )
        return model

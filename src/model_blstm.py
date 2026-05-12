"""
model_blstm.py — Bidirectional LSTM.
"""

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional, BatchNormalization

from base_model import BitcoinModel
from config import MODEL_CONFIGS, DROPOUT, LR


class BLSTMModel(BitcoinModel):
    """
    Dwukierunkowy LSTM (Bidirectional).

    Architektura:
        BiLSTM(64) → BN → Dropout
        BiLSTM(32) → BN → Dropout
        Dense(16, relu) → Dense(1)

    Okno 14 dni — krótsze, bo BiLSTM przetwarza sekwencję w obu kierunkach
    i jest bardziej wrażliwy na overfitting przy długich oknach.
    Lepiej wychwytuje krótkoterminowe wzorce momentum (RSI, whale spikes).
    """

    _cfg = MODEL_CONFIGS['blstm']

    @property
    def name(self) -> str:
        return 'blstm'

    @property
    def window(self) -> int:
        return self._cfg['window']

    def build(self, input_shape: tuple) -> tf.keras.Model:
        units = self._cfg['units']
        model = Sequential([
            Bidirectional(LSTM(units[0], return_sequences=True), input_shape=input_shape),
            BatchNormalization(),
            Dropout(DROPOUT),

            Bidirectional(LSTM(units[1], return_sequences=False)),
            BatchNormalization(),
            Dropout(DROPOUT),

            Dense(16, activation='relu'),
            Dense(1),
        ], name='blstm')

        model.compile(
            optimizer=tf.keras.optimizers.Adam(LR),
            loss='mse',
        )
        return model

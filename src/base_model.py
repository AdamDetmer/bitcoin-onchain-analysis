"""
base_model.py — Abstrakcyjny interfejs BitcoinModel.

Każdy model (LSTM, BLSTM, Conv1D) dziedziczy z tej klasy i implementuje:
    - build(input_shape) → tf.keras.Model
    - name              → str  (klucz do plików na dysku)
    - window            → int  (lookback w dniach)

Reszta (train / save_weights / load_weights / forecast) jest wspólna
i nie wymaga powtarzania w każdym modelu.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
import json

import numpy as np
import pandas as pd
import joblib
import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

from config import MODELS_DIR, BATCH_SIZE, EPOCHS, LR, PATIENCE


class BitcoinModel(ABC):
    """
    Abstrakcyjny interfejs dla modeli predykcji ceny BTC.

    Pliki zapisywane w models/<name>/:
        weights.weights.h5   — wagi Keras (lżejsze niż .keras, przenośne)
        scaler_all.pkl       — MinMaxScaler wszystkich cech
        scaler_price.pkl     — MinMaxScaler log-returnu ceny
        meta.json            — window, features, metryki ostatniego treningu
    """

    # ── Właściwości do zaimplementowania ─────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Krótka nazwa modelu używana jako klucz i nazwa katalogu: 'lstm', 'blstm', 'conv1d'."""

    @property
    @abstractmethod
    def window(self) -> int:
        """Lookback w dniach — ile dni historii widzi model na raz."""

    @abstractmethod
    def build(self, input_shape: tuple) -> tf.keras.Model:
        """
        Buduje i zwraca skompilowany model Keras.
        input_shape = (window, n_features)
        """

    # ── Ścieżki ───────────────────────────────────────────────────────────────

    @property
    def model_dir(self) -> Path:
        return MODELS_DIR / self.name

    @property
    def weights_path(self) -> Path:
        return self.model_dir / "weights.weights.h5"

    @property
    def scaler_all_path(self) -> Path:
        return self.model_dir / "scaler_all.pkl"

    @property
    def scaler_price_path(self) -> Path:
        return self.model_dir / "scaler_price.pkl"

    @property
    def meta_path(self) -> Path:
        return self.model_dir / "meta.json"

    # ── Trening ───────────────────────────────────────────────────────────────

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        scaler_all,
        scaler_price,
        features: list[str],
        X_val: np.ndarray | None = None,
        y_val: np.ndarray | None = None,
    ) -> tf.keras.callbacks.History:
        """
        Trenuje model, zapisuje wagi i scalery.

        Jeśli X_val/y_val nie podane, Keras użyje validation_split=0.15
        z X_train (tak jak wcześniej).
        """
        print(f"\n{'='*55}")
        print(f"  Trening: {self.name.upper()}")
        print(f"  Okno: {self.window} dni   Cechy: {len(features)}")
        print(f"  X_train: {X_train.shape}   X_val: {X_val.shape if X_val is not None else 'auto 15%'}")
        print(f"{'='*55}")

        self._keras_model = self.build(X_train.shape[1:])
        self._keras_model.summary()

        fit_kwargs = dict(
            epochs=EPOCHS,
            batch_size=BATCH_SIZE,
            callbacks=self._callbacks(),
            verbose=1,
        )
        if X_val is not None:
            fit_kwargs['validation_data'] = (X_val, y_val)
        else:
            fit_kwargs['validation_split'] = 0.15

        history = self._keras_model.fit(X_train, y_train, **fit_kwargs)

        self._save(scaler_all, scaler_price, features, history)
        return history

    # ── Zapis / odczyt ────────────────────────────────────────────────────────

    def _save(self, scaler_all, scaler_price, features: list[str], history) -> None:
        """Zapisuje wagi, scalery i metadane."""
        self.model_dir.mkdir(parents=True, exist_ok=True)

        # Wagi (tylko parametry, nie architektura — lżejszy format)
        self._keras_model.save_weights(str(self.weights_path))

        # Scalery
        joblib.dump(scaler_all,   self.scaler_all_path)
        joblib.dump(scaler_price, self.scaler_price_path)

        # Metadane — przydatne przy ładowaniu modelu bez znajomości historii
        val_loss = history.history.get('val_loss', [None])
        meta = {
            'name':          self.name,
            'window':        self.window,
            'features':      features,
            'n_features':    len(features),
            'best_val_loss': float(min(v for v in val_loss if v is not None)),
            'epochs_run':    len(history.history['loss']),
        }
        self.meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))

        print(f"\n  [saved] {self.model_dir}/")
        print(f"          weights.weights.h5  |  scaler_*.pkl  |  meta.json")

    def load(self) -> 'BitcoinModel':
        """
        Wczytuje wagi i scalery z dysku.
        Wymaga wcześniejszego wywołania build() z poprawnym input_shape.
        Korzysta z meta.json żeby odtworzyć input_shape automatycznie.
        """
        meta = json.loads(self.meta_path.read_text())
        input_shape = (meta['window'], meta['n_features'])

        self._keras_model = self.build(input_shape)
        self._keras_model.load_weights(str(self.weights_path))

        self._scaler_all   = joblib.load(self.scaler_all_path)
        self._scaler_price = joblib.load(self.scaler_price_path)
        self._features     = meta['features']

        print(f"  [loaded] {self.name.upper()}  "
              f"(window={meta['window']}, features={meta['n_features']}, "
              f"val_loss={meta['best_val_loss']:.6f})")
        return self

    def is_trained(self) -> bool:
        """Sprawdza czy plik wag istnieje na dysku."""
        return self.weights_path.exists()

    # ── Predykcja ─────────────────────────────────────────────────────────────

    def forecast(self, last_window: np.ndarray, last_price: float, n_steps: int) -> list[float]:
        """
        Rekurencyjna prognoza n_steps dni wprzód.

        last_window: np.ndarray shape (1, window, n_features) — ostatnie znane okno
        last_price:  float — ostatnia rzeczywista cena BTC
        """
        current       = last_window.copy()
        prices        = []
        current_price = last_price

        for _ in range(n_steps):
            pred_scaled = self._keras_model.predict(current, verbose=0)
            pred_return = self._scaler_price.inverse_transform(pred_scaled).flatten()[0]
            current_price = current_price * np.exp(pred_return)
            prices.append(current_price)

            # Przesuń okno — zachowaj cechy on-chain, wstaw nową cenę
            last_exog = current[0, -1, 1:]
            new_row   = np.insert(last_exog, 0, pred_scaled[0, 0]).reshape(1, 1, current.shape[2])
            current   = np.append(current[:, 1:, :], new_row, axis=1)

        return prices

    # ── Pomocnicze ────────────────────────────────────────────────────────────

    def _callbacks(self) -> list:
        return [
            EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True),
            ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=7, verbose=1),
        ]

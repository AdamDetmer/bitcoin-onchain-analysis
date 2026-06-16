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
import matplotlib
matplotlib.use('Agg')           # backend bez GUI — bezpieczne w skryptach i notebooku
import matplotlib.pyplot as plt
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

        self._keras_model.save_weights(str(self.weights_path))

        np.save(str(self.model_dir / 'history.npy'), history.history)

        joblib.dump(scaler_all,   self.scaler_all_path)
        joblib.dump(scaler_price, self.scaler_price_path)

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

        self._plot_history(history)

        print(f"\n  [saved] {self.model_dir}/")
        print(f"          weights.weights.h5  |  scaler_*.pkl  |  meta.json  |  training_history.png")

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

    def _plot_history(self, history, save: bool = True) -> plt.Figure:
        """
        Rysuje krzywe loss/val_loss (i opcjonalnie MAE) z historii treningu.

        Zachowanie:
          - Automatycznie wykrywa dostępne metryki (obsługuje modele
            z pojedynczym outputem i multi-output, np. Transformer z AE).
          - Zapisuje PNG do models/<name>/training_history.png.
          - Zwraca obiekt Figure — można go wywołać bezpośrednio w notebooku.

        Przykład użycia w notebooku:
            from base_model import BitcoinModel
            from model_registry import get_model
            model = get_model('transformer')
            fig = model.load()._plot_history_from_disk()
            plt.show()
        """
        h = history.history

        # ── Wykryj dostępne klucze ────────────────────────────────────────────
        # Modele multi-output mają klucze jak 'price_output_loss'; obsługujemy oba
        loss_key     = 'price_output_loss' if 'price_output_loss' in h else 'loss'
        val_loss_key = ('val_price_output_loss' if 'val_price_output_loss' in h
                        else 'val_loss' if 'val_loss' in h else None)

        mae_key     = next((k for k in ('price_output_mae', 'mae') if k in h), None)
        val_mae_key = next((k for k in ('val_price_output_mae', 'val_mae') if k in h), None)

        has_mae = mae_key is not None
        n_plots = 2 if has_mae else 1

        fig, axes = plt.subplots(1, n_plots, figsize=(6 * n_plots, 4))
        if n_plots == 1:
            axes = [axes]

        epochs = range(1, len(h[loss_key]) + 1)

        # ── Panel 1: Loss ─────────────────────────────────────────────────────
        ax = axes[0]
        ax.plot(epochs, h[loss_key],     label='train loss', linewidth=1.5)
        if val_loss_key:
            ax.plot(epochs, h[val_loss_key], label='val loss',   linewidth=1.5,
                    linestyle='--')
            best_ep  = int(np.argmin(h[val_loss_key])) + 1
            best_val = min(h[val_loss_key])
            ax.axvline(best_ep, color='red', linestyle=':', alpha=0.6,
                       label=f'best ep={best_ep}  val={best_val:.5f}')

        ax.set_title(f'{self.name.upper()} — Loss (MSE)')
        ax.set_xlabel('Epoka')
        ax.set_ylabel('MSE')
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_yscale('log')        # log-skala żeby widać było plateau

        # ── Panel 2: MAE (jeśli dostępne) ─────────────────────────────────────
        if has_mae:
            ax2 = axes[1]
            ax2.plot(epochs, h[mae_key],     label='train MAE', linewidth=1.5)
            if val_mae_key:
                ax2.plot(epochs, h[val_mae_key], label='val MAE',
                         linewidth=1.5, linestyle='--')
            ax2.set_title(f'{self.name.upper()} — MAE')
            ax2.set_xlabel('Epoka')
            ax2.set_ylabel('MAE (scaled)')
            ax2.legend(fontsize=8)
            ax2.grid(True, alpha=0.3)

        fig.suptitle(
            f'Historia treningu: {self.name.upper()}  '
            f'({len(h[loss_key])} epok)',
            fontsize=11, y=1.02,
        )
        fig.tight_layout()

        if save:
            out_path = self.model_dir / 'training_history.png'
            fig.savefig(str(out_path), dpi=130, bbox_inches='tight')
            print(f"  [plot]  {out_path}")

        plt.close(fig)
        return fig

    def plot_history_from_disk(self) -> plt.Figure:
        """
        Odtwarza wykres historii treningu z pliku meta.json + history.npy
        (o ile history.npy istnieje).  Przydatne w notebooku po zakończeniu treningu.

        Jeśli plik history.npy nie istnieje, rzuca FileNotFoundError
        z czytelnym komunikatem.
        """
        history_path = self.model_dir / 'history.npy'
        if not history_path.exists():
            raise FileNotFoundError(
                f"Brak pliku historii: {history_path}\n"
                f"Wywołaj model.train() — automatycznie zapisze history.npy."
            )
        h_dict = np.load(str(history_path), allow_pickle=True).item()

        class _FakeHistory:
            def __init__(self, d):
                self.history = d

        return self._plot_history(_FakeHistory(h_dict), save=False)

    def _callbacks(self) -> list:
        return [
            EarlyStopping(monitor='val_loss', patience=PATIENCE, restore_best_weights=True),
            ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=7, verbose=1),
        ]

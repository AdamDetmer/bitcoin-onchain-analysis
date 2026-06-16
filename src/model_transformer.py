"""
model_transformer.py — Transformer z wbudowanym autoenkoderowym blokiem.

Architektura:
    ┌──────────────────────────────────────────────────────────────────────┐
    │ BLOK AUTOENKODERA (cechy on-chain → latent → rekonstrukcja)         │
    │   Encoder: Dense(latent_dim, relu)                                   │
    │   Decoder: Dense(n_exog, relu)  (auxiliary reconstruction loss)      │
    └──────────────────────────────────────────────────────────────────────┘
              │ latent representation (krok czasowy × latent_dim)
              ↓
    ┌──────────────────────────────────────────────────────────────────────┐
    │ BLOK TRANSFORMER ENCODER                                            │
    │   Wejście: [price_feature ‖ latent_features]  → Linear projection   │
    │   Positional Encoding (sinusoidal, non-trainable)                    │
    │   N × TransformerBlock:                                              │
    │       Multi-Head Self-Attention (n_heads)                            │
    │       Add & Norm                                                      │
    │       FFN: Dense(ff_dim, gelu) → Dense(d_model)                     │
    │       Add & Norm                                                      │
    │   GlobalAveragePooling1D                                             │
    │   Dropout                                                             │
    └──────────────────────────────────────────────────────────────────────┘
              │
              ↓
    Dense(16, relu) → Dense(1)   ← log-return prognozy

Dlaczego autoenkoder WEWNĄTRZ Transformera?
    Cechy on-chain (np. mvrv_zscore, whale_tx) są silnie skorelowane
    i zaszumione.  Bottleneck autoenkodera redukuje wymiarowość i
    odszumiwa je PRZED przekazaniem do głowic self-attention —
    Transformer nie musi „marnotrawić" pojemności na ucześnie szumów.
    Dodatkowy loss rekonstrukcji wymusza nauczenie się sensownych
    reprezentacji latentnych niezależnie od celu prognozy.

Okno: 30 dni (kompromis pomiędzy LSTM-60 a BLSTM-14).
"""

from __future__ import annotations
import json
import math
from pathlib import Path

import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, Model

from base_model import BitcoinModel
from config import MODEL_CONFIGS, DROPOUT, LR, BATCH_SIZE, EPOCHS, PATIENCE, MODELS_DIR
import joblib
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau


# ── Hyperparametry ────────────────────────────────────────────────────────────

_DEFAULT_CFG = {
    'window':    30,
    'd_model':   64,       # wymiar embeddingu Transformera
    'n_heads':   4,        # liczba głowic self-attention
    'ff_dim':    128,      # wymiar FFN (feed-forward network)
    'n_layers':  2,        # liczba bloków TransformerEncoder
    'latent_dim': 8,       # bottleneck autoenkodera
}

# Nadpisz z config.py jeśli 'transformer' jest tam zdefiniowany
_CFG = MODEL_CONFIGS.get('transformer', _DEFAULT_CFG)


# ── Pomocnicze warstwy ────────────────────────────────────────────────────────

class PositionalEncoding(layers.Layer):
    """Sinusoidalne kodowanie pozycji (nieparameteryzowane, jak w AIAYN)."""

    def __init__(self, max_len: int, d_model: int, **kwargs):
        super().__init__(**kwargs)
        # Precompute PE matrix  shape (1, max_len, d_model)
        pe = np.zeros((max_len, d_model), dtype=np.float32)
        pos = np.arange(max_len)[:, None]
        div = np.exp(np.arange(0, d_model, 2) * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = np.sin(pos * div)
        pe[:, 1::2] = np.cos(pos * div)
        self._pe = tf.constant(pe[None, :, :], dtype=tf.float32)  # (1, T, D)

    def call(self, x):
        return x + self._pe[:, : tf.shape(x)[1], :]

    def get_config(self):
        cfg = super().get_config()
        return cfg


class TransformerBlock(layers.Layer):
    """Jeden blok Transformer Encoder: MHA → Add&Norm → FFN → Add&Norm."""

    def __init__(self, d_model: int, n_heads: int, ff_dim: int,
                 dropout: float = 0.1, **kwargs):
        super().__init__(**kwargs)
        self.att   = layers.MultiHeadAttention(num_heads=n_heads,
                                               key_dim=d_model // n_heads,
                                               dropout=dropout)
        self.ffn   = tf.keras.Sequential([
            layers.Dense(ff_dim, activation='gelu'),
            layers.Dense(d_model),
        ])
        self.norm1 = layers.LayerNormalization(epsilon=1e-6)
        self.norm2 = layers.LayerNormalization(epsilon=1e-6)
        self.drop1 = layers.Dropout(dropout)
        self.drop2 = layers.Dropout(dropout)

    def call(self, x, training=False):
        attn = self.att(x, x, training=training)
        x    = self.norm1(x + self.drop1(attn, training=training))
        ffn  = self.ffn(x)
        x    = self.norm2(x + self.drop2(ffn, training=training))
        return x

    def get_config(self):
        cfg = super().get_config()
        return cfg


# ── Główna klasa ──────────────────────────────────────────────────────────────

class TransformerModel(BitcoinModel):
    """
    Transformer z wbudowanym autoenkoderowym blokiem normalizującym cechy on-chain.

    Kluczowe różnice vs vanilla Transformer:
      • Cechy egzogeniczne (on-chain) są kompresowane przez encoder AE
        do latent_dim wymiarów PRZED wejściem do Transformera.
      • Dekoder AE daje pomocniczy output (rekonstrukcja), który łączymy
        z głównym loss (MSE log-return) jako dodatkową regularyzację.
      • Price feature jest przekazywana BEZPOŚREDNIO, bez kompresji.
    """

    _cfg = _CFG

    @property
    def name(self) -> str:
        return 'transformer'

    @property
    def window(self) -> int:
        return self._cfg['window']

    # ── Budowanie modelu ──────────────────────────────────────────────────────

    def build(self, input_shape: tuple) -> tf.keras.Model:
        """
        input_shape = (window, n_features)
          feature[0]   = price (log-return, scaled)
          feature[1:]  = cechy on-chain (scaled)
        """
        window, n_features = input_shape
        n_exog    = n_features - 1          # liczba cech on-chain
        d_model   = self._cfg['d_model']
        n_heads   = self._cfg['n_heads']
        ff_dim    = self._cfg['ff_dim']
        n_layers  = self._cfg['n_layers']
        latent    = self._cfg['latent_dim']

        # ── Wejście ───────────────────────────────────────────────────────────
        inp = layers.Input(shape=(window, n_features), name='sequence_input')

        # ── Autoenkoder na cechach on-chain ───────────────────────────────────
        price_feat = inp[:, :, :1]                        # (B, T, 1)
        exog_feat  = inp[:, :, 1:]                        # (B, T, n_exog)

        # Encoder: każdy krok czasowy niezależnie (TimeDistributed)
        encoded = layers.TimeDistributed(
            layers.Dense(latent, activation='relu'),
            name='ae_encoder'
        )(exog_feat)                                      # (B, T, latent)

        # Decoder (auxiliary output — tylko do obliczania reconstruction loss)
        decoded = layers.TimeDistributed(
            layers.Dense(n_exog, activation='relu'),
            name='ae_decoder'
        )(encoded)                                        # (B, T, n_exog)

        # ── Łączenie price + latent → embedding Transformera ─────────────────
        combined = layers.Concatenate(name='price_latent_concat')(
            [price_feat, encoded]
        )                                                 # (B, T, 1+latent)

        # Projekcja liniowa do d_model
        projected = layers.Dense(d_model, name='input_projection')(combined)

        # ── Positional Encoding ───────────────────────────────────────────────
        x = PositionalEncoding(max_len=window, d_model=d_model,
                               name='pos_enc')(projected)

        # ── Bloki Transformer Encoder ─────────────────────────────────────────
        for i in range(n_layers):
            x = TransformerBlock(
                d_model=d_model,
                n_heads=n_heads,
                ff_dim=ff_dim,
                dropout=DROPOUT,
                name=f'transformer_block_{i}'
            )(x)

        # ── Pooling + Head regresji ───────────────────────────────────────────
        x = layers.GlobalAveragePooling1D(name='gap')(x)
        x = layers.Dropout(DROPOUT)(x)
        x = layers.Dense(16, activation='relu', name='dense_head')(x)

        # Główny output: prognoza log-returnu
        main_out = layers.Dense(1, name='price_output')(x)

        # ── Model z dwoma wyjściami ───────────────────────────────────────────
        model = Model(
            inputs=inp,
            outputs=[main_out, decoded],
            name='transformer_ae'
        )

        model.compile(
            optimizer=tf.keras.optimizers.Adam(LR),
            loss={
                'price_output':  'mse',
                'ae_decoder':    'mse',
            },
            loss_weights={
                'price_output':  1.0,
                'ae_decoder':    0.1,   # lekka regularyzacja rekonstrukcji
            },
            metrics={'price_output': 'mae'},
        )

        return model

    # ── Nadpisanie train() — obsługa dual-output ──────────────────────────────

    def train(self, X_train, y_train, scaler_all, scaler_price,
              features, X_val=None, y_val=None):
        """
        Rozszerza bazowy train() o przygotowanie dual-target dla AE.
        y_train/y_val  →  log-return price   (główny target)
        exog_target    →  cechy on-chain      (cel rekonstrukcji AE)
        """
        print(f"\n{'='*55}")
        print(f"  Trening: {self.name.upper()}")
        print(f"  Okno: {self.window} dni   Cechy: {len(features)}")
        print(f"  X_train: {X_train.shape}   X_val: "
              f"{X_val.shape if X_val is not None else 'auto 15%'}")
        print(f"{'='*55}")

        self._keras_model = self.build(X_train.shape[1:])
        self._keras_model.summary()

        # Cele rekonstrukcji = cechy on-chain (features[1:]) z okna sekwencji
        exog_train = X_train[:, :, 1:]   # (N, T, n_exog)

        fit_kwargs = dict(
            epochs=EPOCHS,
            batch_size=BATCH_SIZE,
            callbacks=self._callbacks(),
            verbose=1,
        )

        if X_val is not None:
            exog_val = X_val[:, :, 1:]
            fit_kwargs['validation_data'] = (
                X_val,
                {'price_output': y_val, 'ae_decoder': exog_val},
            )
            history = self._keras_model.fit(
                X_train,
                {'price_output': y_train, 'ae_decoder': exog_train},
                **fit_kwargs,
            )
        else:
            fit_kwargs['validation_split'] = 0.15
            history = self._keras_model.fit(
                X_train,
                {'price_output': y_train, 'ae_decoder': exog_train},
                **fit_kwargs,
            )

        self._save(scaler_all, scaler_price, features, history)
        return history

    # ── Nadpisanie forecast() — pobierz tylko main_output ────────────────────

    def forecast(self, last_window: np.ndarray, last_price: float,
                 n_steps: int) -> list:
        current       = last_window.copy()
        prices        = []
        current_price = last_price

        for _ in range(n_steps):
            # Model zwraca [main, ae_decoded]; chcemy tylko main
            outputs     = self._keras_model.predict(current, verbose=0)
            pred_scaled = outputs[0]               # (1, 1)
            pred_return = self._scaler_price.inverse_transform(
                pred_scaled).flatten()[0]
            current_price = current_price * np.exp(pred_return)
            prices.append(current_price)

            last_exog = current[0, -1, 1:]
            new_row   = np.insert(
                last_exog, 0, pred_scaled[0, 0]
            ).reshape(1, 1, current.shape[2])
            current = np.append(current[:, 1:, :], new_row, axis=1)

        return prices

    # ── Pomocnicza metoda predict() dla ewaluacji (tylko main output) ─────────

    def predict_main(self, X: np.ndarray) -> np.ndarray:
        """Zwraca tylko główny output (log-return), ignoruje AE output."""
        outputs = self._keras_model.predict(X, verbose=0)
        return outputs[0]   # shape (N, 1)

    def _save(self, scaler_all, scaler_price, features, history) -> None :
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self._keras_model.save_weights(str(self.weights_path))

        np.save(str(self.model_dir / 'history.npy'), history.history)
        self._plot_history(history)

        joblib.dump(scaler_all, self.scaler_all_path)
        joblib.dump(scaler_price, self.scaler_price_path)

        val_loss_key = None
        for key in ('val_price_output_loss', 'val_loss'):
            if key in history.history:
                val_loss_key = key
                break

        val_losses = history.history.get(val_loss_key, [None])
        best_val   = float(min(v for v in val_losses if v is not None))

        meta = {
            'name':          self.name,
            'window':        self.window,
            'features':      features,
            'n_features':    len(features),
            'best_val_loss': best_val,
            'epochs_run':    len(history.history['loss']),
        }
        self.meta_path.write_text(
            json.dumps(meta, indent=2, ensure_ascii=False))

        print(f"\n  [saved] {self.model_dir}/")
        print(f"          weights.weights.h5  |  scaler_*.pkl  |  meta.json")

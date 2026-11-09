"""
Deep Regularization & Architectural Optimization for Temporal Fusion Transformer (TFT).
Addresses financial noise overfitting via:
- Shorter focused sequence horizons (W=12 vs W=24)
- Compact embedding dimensions (d_model=16, 24, 32)
- Heavy dropout (0.25 to 0.40) & LayerNorm stabilization
- Low-head attention (heads=2, 4) to prevent spurious pattern memorization
Logs all trials to CSV for master's thesis data tables.
"""

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['OMP_NUM_THREADS'] = '12'
os.environ['TF_NUM_INTRAOP_THREADS'] = '12'
os.environ['TF_NUM_INTEROP_THREADS'] = '2'

import sys
import time
import json
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, regularizers, optimizers, callbacks
from sklearn.metrics import roc_auc_score, accuracy_score

from benchmark_evaluator import load_benchmark_data, create_sequences, evaluate_and_save_model

LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'benchmarks', 'tuning_logs'))
os.makedirs(LOGS_DIR, exist_ok=True)

SEED = 42
np.random.seed(SEED)
tf.random.set_seed(SEED)

class GLU(layers.Layer):
    def __init__(self, units, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        self.dense = layers.Dense(units * 2)

    def call(self, inputs):
        res = self.dense(inputs)
        return res[..., :self.units] * tf.sigmoid(res[..., self.units:])

class GRN(layers.Layer):
    def __init__(self, units, dropout_rate=0.25, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        self.dense1 = layers.Dense(units, activation='elu')
        self.dense2 = layers.Dense(units)
        self.dropout = layers.Dropout(dropout_rate)
        self.glu = GLU(units)
        self.ln = layers.LayerNormalization()
        self.res_proj = layers.Dense(units)

    def call(self, inputs):
        x = self.dense1(inputs)
        x = self.dense2(x)
        x = self.dropout(x)
        x = self.glu(x)
        res = self.res_proj(inputs)
        return self.ln(x + res)

class VariableSelectionNetwork(layers.Layer):
    def __init__(self, num_features, units, dropout_rate=0.25, **kwargs):
        super().__init__(**kwargs)
        self.num_features = num_features
        self.units = units
        self.grn_weights = GRN(num_features, dropout_rate)
        self.feature_proj = layers.Dense(units)
        self.post_grn = GRN(units, dropout_rate)
        self.softmax = layers.Softmax(axis=-1)

    def call(self, inputs):
        weights = self.softmax(self.grn_weights(inputs))
        feat_expanded = tf.expand_dims(inputs, axis=-1)
        feat_emb = self.feature_proj(feat_expanded)
        weighted = feat_emb * tf.expand_dims(weights, axis=-1)
        combined = tf.reduce_sum(weighted, axis=2)
        return self.post_grn(combined)

class TemporalAttentionPooling(layers.Layer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.score_dense = layers.Dense(1)
        self.softmax = layers.Softmax(axis=1)

    def call(self, inputs):
        scores = self.score_dense(inputs)
        weights = self.softmax(scores)
        return tf.reduce_sum(inputs * weights, axis=1)

def build_compact_tft(input_shape, d_model=24, num_heads=2, dropout_rate=0.30, lr=8e-4):
    inputs = layers.Input(shape=input_shape)
    time_steps, num_features = input_shape
    
    vsn = VariableSelectionNetwork(num_features, d_model, dropout_rate=dropout_rate)(inputs)
    mha = layers.MultiHeadAttention(num_heads=num_heads, key_dim=d_model // num_heads)(vsn, vsn)
    mha_post = GRN(d_model, dropout_rate=dropout_rate)(mha)
    seq_repr = layers.LayerNormalization()(layers.add([vsn, mha_post]))
    
    pooled = TemporalAttentionPooling()(seq_repr)
    dense_out = GRN(16, dropout_rate=dropout_rate)(pooled)
    outputs = layers.Dense(1, activation='sigmoid')(dense_out)
    
    model = tf.keras.Model(inputs=inputs, outputs=outputs)
    model.compile(
        optimizer=optimizers.Adam(learning_rate=lr),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    return model

def run_tft_deep_tuning():
    print("=" * 80)
    print(" DEEP TUNING: TEMPORAL FUSION TRANSFORMER (REGULARIZED COMPACT SEARCH)")
    print("=" * 80)
    
    data = load_benchmark_data(num_features=24)
    X_train_sc, y_train = data["X_train"], data["y_train"]
    X_val_sc, y_val = data["X_val"], data["y_val"]
    X_test_sc, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    
    configs = [
        # W=12 Horizon: short-term focus, lower noise
        {"window": 12, "d_model": 16, "heads": 2, "dropout": 0.25, "lr": 1e-3, "name": "W12_d16_h2_dr0.25"},
        {"window": 12, "d_model": 24, "heads": 2, "dropout": 0.30, "lr": 8e-4, "name": "W12_d24_h2_dr0.30"},
        {"window": 12, "d_model": 32, "heads": 4, "dropout": 0.35, "lr": 6e-4, "name": "W12_d32_h4_dr0.35"},
        
        # W=24 Horizon: daily context with anti-overfitting regularization
        {"window": 24, "d_model": 16, "heads": 2, "dropout": 0.30, "lr": 1e-3, "name": "W24_d16_h2_dr0.30"},
        {"window": 24, "d_model": 24, "heads": 2, "dropout": 0.35, "lr": 8e-4, "name": "W24_d24_h2_dr0.35"},
        {"window": 24, "d_model": 32, "heads": 4, "dropout": 0.35, "lr": 6e-4, "name": "W24_d32_h4_dr0.35"},
        {"window": 24, "d_model": 48, "heads": 4, "dropout": 0.40, "lr": 5e-4, "name": "W24_d48_h4_dr0.40"}
    ]
    
    seq_cache = {}
    for w in [12, 24]:
        seq_cache[w] = {
            "tr": create_sequences(X_train_sc, y_train, window=w),
            "va": create_sequences(X_val_sc, y_val, window=w),
            "te": create_sequences(X_test_sc, y_test, window=w)
        }
        
    results = []
    best_val_auc = 0.0
    best_model = None
    best_cfg = None
    best_test_prob = None
    best_test_true = None
    
    start_time = time.time()
    
    for idx, cfg in enumerate(configs):
        w = cfg["window"]
        X_tr_seq, y_tr_seq = seq_cache[w]["tr"]
        X_va_seq, y_va_seq = seq_cache[w]["va"]
        X_te_seq, y_te_seq = seq_cache[w]["te"]
        
        input_shape = (w, X_train_sc.shape[1])
        model = build_compact_tft(
            input_shape=input_shape,
            d_model=cfg["d_model"],
            num_heads=cfg["heads"],
            dropout_rate=cfg["dropout"],
            lr=cfg["lr"]
        )
        
        early_stop = callbacks.EarlyStopping(monitor="val_auc", mode="max", patience=7, restore_best_weights=True)
        reduce_lr = callbacks.ReduceLROnPlateau(monitor="val_auc", mode="max", factor=0.5, patience=3, min_lr=1e-5)
        
        hist = model.fit(
            X_tr_seq, y_tr_seq,
            validation_data=(X_va_seq, y_va_seq),
            epochs=22,
            batch_size=128,
            callbacks=[early_stop, reduce_lr],
            verbose=0
        )
        
        va_pred = model.predict(X_va_seq, batch_size=256, verbose=0).flatten()
        te_pred = model.predict(X_te_seq, batch_size=256, verbose=0).flatten()
        
        val_auc = float(roc_auc_score(y_va_seq, va_pred))
        test_auc = float(roc_auc_score(y_te_seq, te_pred))
        test_acc = float(accuracy_score(y_te_seq, (te_pred >= 0.50).astype(int)))
        
        best_p, best_t, best_tau = 0.0, 0, 0.50
        for tau in [0.55, 0.60, 0.65, 0.68, 0.70]:
            m = (te_pred >= tau)
            nt = int(np.sum(m))
            if nt >= 25:
                prec = float(np.mean(y_te_seq[m] == 1))
                if prec > best_p:
                    best_p, best_t, best_tau = prec, nt, tau
                    
        rec = {
            "trial_id": idx + 1,
            "name": cfg["name"],
            "window": w,
            "d_model": cfg["d_model"],
            "heads": cfg["heads"],
            "dropout": cfg["dropout"],
            "lr": cfg["lr"],
            "val_auc": round(val_auc, 4),
            "test_acc": round(test_acc, 4),
            "test_auc": round(test_auc, 4),
            "best_tau": best_tau,
            "precision": round(best_p, 4),
            "trades": best_t
        }
        results.append(rec)
        print(f"Trial {idx+1:02d}/07 | {cfg['name']:<22} | Val AUC: {val_auc:.4f} | Test AUC: {test_auc:.4f} | Prec: {best_p*100:.1f}% ({best_t}t)")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = model
            best_cfg = cfg
            best_test_prob = te_pred
            best_test_true = y_te_seq
            
    df_results = pd.DataFrame(results)
    csv_out = os.path.join(LOGS_DIR, "tft_deep_tuning_results.csv")
    df_results.to_csv(csv_out, index=False)
    print(f"\nZapisano wyniki wszystkich prób TFT do: {csv_out}")
    print(f"Czas tuningu: {(time.time() - start_time):.1f} s")
    
    print("\n" + "=" * 80)
    print(f"NAJLEPSZY WARIANT TFT (Val AUC = {best_val_auc:.4f}):")
    print(json.dumps(best_cfg, indent=2))
    print("=" * 80)
    
    # Official evaluation of champion TFT
    metrics = evaluate_and_save_model(
        model_name="TFT",
        y_true=best_test_true,
        y_prob=best_test_prob,
        test_df=test_df,
        window_offset=best_cfg["window"] - 1,
        details={
            "best_config": best_cfg,
            "val_auc": float(best_val_auc),
            "architecture": f"Temporal Fusion Transformer ({best_cfg['name']})"
        }
    )
    return metrics

if __name__ == '__main__':
    run_tft_deep_tuning()

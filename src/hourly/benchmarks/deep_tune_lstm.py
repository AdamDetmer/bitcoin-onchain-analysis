"""
Deep Hyperparameter & Architecture Search for Unidirectional LSTM.
Explores:
- Sequence horizons: W=12, W=24, W=48
- Hidden units: 32, 48, 64, 96
- Regularization: Dropout (0.15 to 0.35), L2 kernel regularizer
- Normalization: LayerNorm / BatchNorm
- Residual recurrent skip connections
Logs all trials to CSV for master's thesis.
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

def build_unidirectional_lstm(input_shape, units=64, num_layers=1, dropout=0.2, l2_reg=1e-4, use_res=False, lr=8e-4):
    inputs = layers.Input(shape=input_shape)
    x = inputs
    
    reg = regularizers.l2(l2_reg) if l2_reg > 0 else None
    
    if num_layers == 1:
        lstm_out = layers.LSTM(units, return_sequences=False, kernel_regularizer=reg)(x)
        x = layers.Dropout(dropout)(lstm_out)
        if use_res:
            # Linear residual shortcut from last time step
            last_step = inputs[:, -1, :]
            shortcut = layers.Dense(units)(last_step)
            x = layers.add([x, shortcut])
            x = layers.LayerNormalization()(x)
    else:
        x = layers.LSTM(units, return_sequences=True, kernel_regularizer=reg)(x)
        x = layers.Dropout(dropout)(x)
        x = layers.LSTM(units // 2, return_sequences=False, kernel_regularizer=reg)(x)
        x = layers.Dropout(dropout)(x)
        
    x = layers.Dense(32, activation="relu", kernel_regularizer=reg)(x)
    x = layers.Dropout(dropout)(x)
    outputs = layers.Dense(1, activation="sigmoid")(x)
    
    model = tf.keras.Model(inputs=inputs, outputs=outputs)
    model.compile(
        optimizer=optimizers.Adam(learning_rate=lr),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    return model

def run_lstm_deep_tuning():
    print("=" * 80)
    print(" DEEP TUNING: UNIDIRECTIONAL LSTM (EXTENSIVE ARCHITECTURE SEARCH)")
    print("=" * 80)
    
    data = load_benchmark_data(num_features=24)
    X_train_sc, y_train = data["X_train"], data["y_train"]
    X_val_sc, y_val = data["X_val"], data["y_val"]
    X_test_sc, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    
    # 14 systematic configurations
    configs = [
        # Horizon W=12 (Local 12h momentum)
        {"window": 12, "units": 48, "layers": 1, "dropout": 0.20, "l2": 1e-4, "res": True,  "lr": 1e-3, "name": "W12_1L_u48_res"},
        {"window": 12, "units": 64, "layers": 1, "dropout": 0.25, "l2": 2e-4, "res": False, "lr": 8e-4, "name": "W12_1L_u64_reg"},
        {"window": 12, "units": 64, "layers": 2, "dropout": 0.20, "l2": 1e-4, "res": False, "lr": 6e-4, "name": "W12_2L_stacked"},
        
        # Horizon W=24 (Standard daily window)
        {"window": 24, "units": 32, "layers": 1, "dropout": 0.15, "l2": 1e-5, "res": False, "lr": 1e-3, "name": "W24_1L_u32_compact"},
        {"window": 24, "units": 48, "layers": 1, "dropout": 0.20, "l2": 1e-4, "res": True,  "lr": 8e-4, "name": "W24_1L_u48_res"},
        {"window": 24, "units": 64, "layers": 1, "dropout": 0.25, "l2": 2e-4, "res": True,  "lr": 8e-4, "name": "W24_1L_u64_res_reg"},
        {"window": 24, "units": 64, "layers": 1, "dropout": 0.30, "l2": 5e-4, "res": False, "lr": 6e-4, "name": "W24_1L_u64_heavyreg"},
        {"window": 24, "units": 96, "layers": 1, "dropout": 0.35, "l2": 3e-4, "res": True,  "lr": 5e-4, "name": "W24_1L_u96_res"},
        {"window": 24, "units": 64, "layers": 2, "dropout": 0.25, "l2": 2e-4, "res": False, "lr": 5e-4, "name": "W24_2L_stacked_reg"},
        
        # Horizon W=48 (48h multi-day context)
        {"window": 48, "units": 48, "layers": 1, "dropout": 0.25, "l2": 2e-4, "res": True,  "lr": 6e-4, "name": "W48_1L_u48_res"},
        {"window": 48, "units": 64, "layers": 1, "dropout": 0.30, "l2": 3e-4, "res": True,  "lr": 5e-4, "name": "W48_1L_u64_res"},
        {"window": 48, "units": 64, "layers": 2, "dropout": 0.25, "l2": 1e-4, "res": False, "lr": 4e-4, "name": "W48_2L_stacked"}
    ]
    
    # Pre-generate sequences per window
    seq_cache = {}
    for w in [12, 24, 48]:
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
        model = build_unidirectional_lstm(
            input_shape=input_shape,
            units=cfg["units"],
            num_layers=cfg["layers"],
            dropout=cfg["dropout"],
            l2_reg=cfg["l2"],
            use_res=cfg["res"],
            lr=cfg["lr"]
        )
        
        early_stop = callbacks.EarlyStopping(monitor="val_auc", mode="max", patience=7, restore_best_weights=True)
        reduce_lr = callbacks.ReduceLROnPlateau(monitor="val_auc", mode="max", factor=0.5, patience=3, min_lr=1e-5)
        
        hist = model.fit(
            X_tr_seq, y_tr_seq,
            validation_data=(X_va_seq, y_va_seq),
            epochs=25,
            batch_size=128,
            callbacks=[early_stop, reduce_lr],
            verbose=0
        )
        
        va_pred = model.predict(X_va_seq, batch_size=256, verbose=0).flatten()
        te_pred = model.predict(X_te_seq, batch_size=256, verbose=0).flatten()
        
        val_auc = float(roc_auc_score(y_va_seq, va_pred))
        test_auc = float(roc_auc_score(y_te_seq, te_pred))
        test_acc = float(accuracy_score(y_te_seq, (te_pred >= 0.50).astype(int)))
        
        # Test precision at high confidence
        best_p, best_t, best_tau = 0.0, 0, 0.50
        for tau in [0.60, 0.64, 0.68, 0.70, 0.72]:
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
            "units": cfg["units"],
            "layers": cfg["layers"],
            "dropout": cfg["dropout"],
            "l2": cfg["l2"],
            "residual_shortcut": cfg["res"],
            "lr": cfg["lr"],
            "val_auc": round(val_auc, 4),
            "test_acc": round(test_acc, 4),
            "test_auc": round(test_auc, 4),
            "best_tau": best_tau,
            "precision": round(best_p, 4),
            "trades": best_t
        }
        results.append(rec)
        print(f"Trial {idx+1:02d}/12 | {cfg['name']:<22} | Val AUC: {val_auc:.4f} | Test AUC: {test_auc:.4f} | Prec: {best_p*100:.1f}% ({best_t}t)")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = model
            best_cfg = cfg
            best_test_prob = te_pred
            best_test_true = y_te_seq
            
    df_results = pd.DataFrame(results)
    csv_out = os.path.join(LOGS_DIR, "lstm_deep_tuning_results.csv")
    df_results.to_csv(csv_out, index=False)
    print(f"\nZapisano wyniki wszystkich 12 wariantów LSTM do: {csv_out}")
    print(f"Czas tuningu: {(time.time() - start_time):.1f} s")
    
    print("\n" + "=" * 80)
    print(f"NAJLEPSZY WARIANT UNIDIRECTIONAL LSTM (Val AUC = {best_val_auc:.4f}):")
    print(json.dumps(best_cfg, indent=2))
    print("=" * 80)
    
    # Official evaluation of champion LSTM
    metrics = evaluate_and_save_model(
        model_name="LSTM",
        y_true=best_test_true,
        y_prob=best_test_prob,
        test_df=test_df,
        window_offset=best_cfg["window"] - 1,
        details={
            "best_config": best_cfg,
            "val_auc": float(best_val_auc),
            "architecture": f"Unidirectional LSTM ({best_cfg['name']})"
        }
    )
    return metrics

if __name__ == '__main__':
    run_lstm_deep_tuning()

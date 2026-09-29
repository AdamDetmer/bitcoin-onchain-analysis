"""
Export & Persist Champion Models to Disk.
Saves all 5 benchmark champions + BLSTM champion to `models/benchmarks/`
so they can be loaded instantly in Jupyter Notebooks without retraining.
"""

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['OMP_NUM_THREADS'] = '12'
os.environ['TF_NUM_INTRAOP_THREADS'] = '12'
os.environ['TF_NUM_INTEROP_THREADS'] = '2'

import sys
import json
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
import tensorflow as tf
from tensorflow.keras import layers, regularizers, optimizers, callbacks
from sklearn.svm import LinearSVC
from sklearn.kernel_approximation import Nystroem
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

from benchmark_evaluator import load_benchmark_data, create_sequences

MODELS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'models', 'benchmarks'))
os.makedirs(MODELS_DIR, exist_ok=True)

def export_all_champion_models():
    print("=" * 80)
    print(" EXPORTING ALL CHAMPION MODELS TO DISK (`models/benchmarks/`)")
    print("=" * 80)
    
    # 1. Load data
    data = load_benchmark_data(num_features=36)
    train_df = data["train_df"]
    val_df = data["val_df"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    scaler = data["scaler"]
    
    X_train_full = data["X_train"]
    y_train = data["y_train"]
    X_val_full = data["X_val"]
    y_val = data["y_val"]
    
    metadata = {
        "feature_cols_36": feature_cols,
        "feature_cols_24": feature_cols[:24],
        "models": {}
    }
    
    # -------------------------------------------------------------
    # 1. XGBoost Champion
    # -------------------------------------------------------------
    print("\n[1/5] Trenowanie i zapisywanie XGBoost Champion...")
    xgb_clf = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.06,
        subsample=0.65,
        colsample_bytree=0.90,
        min_child_weight=5,
        gamma=0.0,
        reg_alpha=1.5,
        reg_lambda=5.0,
        eval_metric=["logloss", "auc"],
        random_state=42,
        n_jobs=-1
    )
    xgb_clf.fit(X_train_full, y_train, eval_set=[(X_val_full, y_val)], verbose=False)
    xgb_path = os.path.join(MODELS_DIR, "xgboost_champion.json")
    xgb_clf.save_model(xgb_path)
    print(f"  -> Zapisano: {xgb_path}")
    metadata["models"]["XGBoost"] = {
        "file": "xgboost_champion.json",
        "num_features": 36,
        "best_tau": 0.62,
        "win_rate": 0.8571,
        "net_return": 0.2954
    }
    
    # -------------------------------------------------------------
    # 2. SVM Champion (RBF Nystroem)
    # -------------------------------------------------------------
    print("\n[2/5] Trenowanie i zapisywanie SVM Champion (RBF Nystroem)...")
    X_tr_24 = X_train_full[:, :24]
    X_va_24 = X_val_full[:, :24]
    
    nystroem = Nystroem(gamma=0.015, n_components=400, random_state=42)
    X_tr_nys = nystroem.fit_transform(X_tr_24)
    X_va_nys = nystroem.transform(X_va_24)
    
    base_svc = LinearSVC(C=0.3, dual=False, max_iter=2500, random_state=42)
    base_svc.fit(X_tr_nys, y_train)
    
    calibrated_svm = CalibratedClassifierCV(estimator=base_svc, method='sigmoid', cv='prefit')
    calibrated_svm.fit(X_va_nys, y_val)
    
    svm_bundle = {
        "nystroem": nystroem,
        "calibrated_model": calibrated_svm,
        "num_features": 24,
        "feature_cols": feature_cols[:24]
    }
    svm_path = os.path.join(MODELS_DIR, "svm_champion.joblib")
    joblib.dump(svm_bundle, svm_path)
    print(f"  -> Zapisano: {svm_path}")
    metadata["models"]["SVM"] = {
        "file": "svm_champion.joblib",
        "num_features": 24,
        "best_tau": 0.66,
        "win_rate": 0.7561,
        "net_return": 0.1318
    }
    
    # -------------------------------------------------------------
    # 3. SARIMAX Champion
    # -------------------------------------------------------------
    print("\n[3/5] Trenowanie i zapisywanie SARIMAX Champion...")
    y_tr_cont = train_df["target_max_return_fwd"].values[-10000:]
    y_va_cont = val_df["target_max_return_fwd"].values
    
    deriv_feats = [c for c in feature_cols if any(k in c for k in ['delta', 'dvol', 'funding'])][:5]
    exog_idx = [feature_cols.index(f) for f in deriv_feats]
    exog_tr_sub = X_train_full[-10000:, exog_idx]
    exog_va = X_val_full[:, exog_idx]
    
    sarimax_model = SARIMAX(
        y_tr_cont,
        exog=exog_tr_sub,
        order=(2, 0, 2),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    sarimax_res = sarimax_model.fit(disp=False, maxiter=50)
    
    val_forecast = sarimax_res.get_forecast(steps=len(y_va_cont), exog=exog_va)
    val_mu = val_forecast.predicted_mean
    
    mu_scaler = StandardScaler()
    val_mu_sc = mu_scaler.fit_transform(val_mu.reshape(-1, 1))
    
    calibrator = LogisticRegression(C=1.0)
    calibrator.fit(val_mu_sc, y_val)
    
    sarimax_bundle = {
        "results": sarimax_res,
        "mu_scaler": mu_scaler,
        "calibrator": calibrator,
        "exog_features": deriv_feats,
        "exog_idx": exog_idx
    }
    sarimax_path = os.path.join(MODELS_DIR, "sarimax_champion.joblib")
    joblib.dump(sarimax_bundle, sarimax_path)
    print(f"  -> Zapisano: {sarimax_path}")
    metadata["models"]["SARIMAX"] = {
        "file": "sarimax_champion.joblib",
        "order": [2, 0, 2],
        "exog_features": deriv_feats,
        "best_tau": 0.54,
        "win_rate": 0.8095,
        "net_return": 0.7956
    }
    
    # -------------------------------------------------------------
    # 4. Unidirectional LSTM Champion (W=48 + Residual)
    # -------------------------------------------------------------
    print("\n[4/5] Trenowanie i zapisywanie Unidirectional LSTM Champion (W=48)...")
    X_tr_48, y_tr_48 = create_sequences(X_train_full[:, :24], y_train, window=48)
    X_va_48, y_va_48 = create_sequences(X_val_full[:, :24], y_val, window=48)
    
    lstm_inputs = layers.Input(shape=(48, 24))
    reg = regularizers.l2(2e-4)
    lstm_out = layers.LSTM(48, return_sequences=False, kernel_regularizer=reg)(lstm_inputs)
    lstm_drop = layers.Dropout(0.25)(lstm_out)
    
    # Residual skip connection from last time step
    last_step = lstm_inputs[:, -1, :]
    shortcut = layers.Dense(48)(last_step)
    merged = layers.add([lstm_drop, shortcut])
    normed = layers.LayerNormalization()(merged)
    
    dense = layers.Dense(32, activation="relu", kernel_regularizer=reg)(normed)
    dense_drop = layers.Dropout(0.25)(dense)
    lstm_outputs = layers.Dense(1, activation="sigmoid")(dense_drop)
    
    lstm_model = tf.keras.Model(inputs=lstm_inputs, outputs=lstm_outputs, name="LSTM_W48_Champion")
    lstm_model.compile(
        optimizer=optimizers.Adam(learning_rate=6e-4),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    
    early_stop = callbacks.EarlyStopping(monitor="val_auc", mode="max", patience=6, restore_best_weights=True)
    lstm_model.fit(
        X_tr_48, y_tr_48,
        validation_data=(X_va_48, y_va_48),
        epochs=15,
        batch_size=128,
        callbacks=[early_stop],
        verbose=0
    )
    lstm_path = os.path.join(MODELS_DIR, "lstm_champion.keras")
    lstm_model.save(lstm_path)
    print(f"  -> Zapisano: {lstm_path}")
    metadata["models"]["LSTM"] = {
        "file": "lstm_champion.keras",
        "window": 48,
        "num_features": 24,
        "best_tau": 0.72,
        "win_rate": 0.9000,
        "net_return": 0.5527
    }
    
    # -------------------------------------------------------------
    # 5. Conv1D-BLSTM Champion with Attention (W=24)
    # -------------------------------------------------------------
    print("\n[5/5] Trenowanie i zapisywanie Conv1D-BLSTM Champion (W=24)...")
    X_tr_24_seq, y_tr_24_seq = create_sequences(X_train_full[:, :24], y_train, window=24)
    X_va_24_seq, y_va_24_seq = create_sequences(X_val_full[:, :24], y_val, window=24)
    
    class TemporalAttention(layers.Layer):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
        def build(self, input_shape):
            self.W = self.add_weight(shape=(input_shape[-1], 1), initializer="glorot_uniform", trainable=True)
            self.b = self.add_weight(shape=(1,), initializer="zeros", trainable=True)
            super().build(input_shape)
        def call(self, x):
            e = tf.tanh(tf.matmul(x, self.W) + self.b)
            a = tf.nn.softmax(e, axis=1)
            return tf.reduce_sum(x * a, axis=1)

    blstm_inputs = layers.Input(shape=(24, 24))
    conv = layers.Conv1D(filters=32, kernel_size=3, padding="same", activation="relu")(blstm_inputs)
    bilstm = layers.Bidirectional(layers.LSTM(64, return_sequences=True))(conv)
    att = TemporalAttention()(bilstm)
    drop = layers.Dropout(0.3)(att)
    fc = layers.Dense(32, activation="relu")(drop)
    blstm_outputs = layers.Dense(1, activation="sigmoid")(fc)
    
    blstm_model = tf.keras.Model(inputs=blstm_inputs, outputs=blstm_outputs, name="Conv1D_BLSTM_Champion")
    blstm_model.compile(
        optimizer=optimizers.Adam(learning_rate=8e-4),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    
    early_stop_b = callbacks.EarlyStopping(monitor="val_auc", mode="max", patience=6, restore_best_weights=True)
    blstm_model.fit(
        X_tr_24_seq, y_tr_24_seq,
        validation_data=(X_va_24_seq, y_va_24_seq),
        epochs=15,
        batch_size=128,
        callbacks=[early_stop_b],
        verbose=0
    )
    blstm_path = os.path.join(MODELS_DIR, "blstm_champion.keras")
    blstm_model.save(blstm_path)
    print(f"  -> Zapisano: {blstm_path}")
    metadata["models"]["BLSTM"] = {
        "file": "blstm_champion.keras",
        "window": 24,
        "num_features": 24,
        "best_tau": 0.70,
        "win_rate": 0.8125,
        "net_return": 0.3009
    }
    
    # Save scaler and metadata
    scaler_path = os.path.join(MODELS_DIR, "feature_scaler.joblib")
    joblib.dump(scaler, scaler_path)
    
    meta_path = os.path.join(MODELS_DIR, "champion_models_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"\nZapisano pełne metadane modeli: {meta_path}")
    print("Wszystkie modele zostały pomyślnie zserializowane na dysku!")

if __name__ == '__main__':
    export_all_champion_models()

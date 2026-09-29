"""
Benchmark Model 2: Standard Unidirectional LSTM.
Systematic tuning across depth, units, regularization, and Conv1D hybrid feature extraction.
Direct ablation against Bidirectional LSTM (BLSTM).
"""

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['OMP_NUM_THREADS'] = '12'
os.environ['TF_NUM_INTRAOP_THREADS'] = '12'
os.environ['TF_NUM_INTEROP_THREADS'] = '2'

import sys
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, regularizers, optimizers, callbacks
from sklearn.metrics import roc_auc_score

from benchmark_evaluator import load_benchmark_data, create_sequences, evaluate_and_save_model

SEED = 42
np.random.seed(SEED)
tf.random.set_seed(SEED)

def build_lstm_model(variant, input_shape, lr=1e-3):
    inputs = layers.Input(shape=input_shape)
    
    if variant == "LSTM_1L_64":
        x = layers.LSTM(64, return_sequences=False)(inputs)
        x = layers.Dropout(0.2)(x)
        x = layers.Dense(32, activation="relu")(x)
        x = layers.Dropout(0.2)(x)
        
    elif variant == "LSTM_2L_stacked":
        x = layers.LSTM(64, return_sequences=True)(inputs)
        x = layers.Dropout(0.2)(x)
        x = layers.LSTM(32, return_sequences=False)(x)
        x = layers.Dropout(0.2)(x)
        x = layers.Dense(16, activation="relu")(x)
        
    elif variant == "LSTM_1L_128_reg":
        x = layers.LSTM(128, return_sequences=False, kernel_regularizer=regularizers.l2(1e-4))(inputs)
        x = layers.Dropout(0.3)(x)
        x = layers.Dense(32, activation="relu", kernel_regularizer=regularizers.l2(1e-4))(x)
        x = layers.Dropout(0.2)(x)
        
    elif variant == "Conv1D_LSTM":
        x = layers.Conv1D(filters=32, kernel_size=3, padding="same", activation="relu")(inputs)
        x = layers.BatchNormalization()(x)
        x = layers.LSTM(64, return_sequences=False)(x)
        x = layers.Dropout(0.25)(x)
        x = layers.Dense(32, activation="relu")(x)
        x = layers.Dropout(0.2)(x)
        
    else:
        raise ValueError(f"Unknown variant: {variant}")
        
    outputs = layers.Dense(1, activation="sigmoid")(x)
    model = tf.keras.Model(inputs=inputs, outputs=outputs, name=variant)
    
    model.compile(
        optimizer=optimizers.Adam(learning_rate=lr),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    return model

def run_lstm_benchmark():
    print("=" * 75)
    print(" BENCHMARK 2: STANDARD UNIDIRECTIONAL LSTM")
    print("=" * 75)
    
    window = 24
    data = load_benchmark_data(num_features=24)
    X_train_sc, y_train = data["X_train"], data["y_train"]
    X_val_sc, y_val = data["X_val"], data["y_val"]
    X_test_sc, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    
    X_tr_seq, y_tr_seq = create_sequences(X_train_sc, y_train, window=window)
    X_val_seq, y_val_seq = create_sequences(X_val_sc, y_val, window=window)
    X_te_seq, y_te_seq = create_sequences(X_test_sc, y_test, window=window)
    
    print(f"Sequences shape: Train={X_tr_seq.shape}, Val={X_val_seq.shape}, Test={X_te_seq.shape}")
    
    variants = [
        {"name": "LSTM_1L_64", "lr": 1e-3, "batch_size": 128},
        {"name": "LSTM_2L_stacked", "lr": 8e-4, "batch_size": 128},
        {"name": "LSTM_1L_128_reg", "lr": 6e-4, "batch_size": 128},
        {"name": "Conv1D_LSTM", "lr": 8e-4, "batch_size": 128}
    ]
    
    best_val_auc = 0.0
    best_model = None
    best_variant = None
    best_history = None
    
    input_shape = (window, X_train_sc.shape[1])
    
    for v in variants:
        print(f"\nTrenowanie architektury: {v['name']} (lr={v['lr']})...")
        model = build_lstm_model(v["name"], input_shape, lr=v["lr"])
        
        early_stop = callbacks.EarlyStopping(
            monitor="val_auc",
            mode="max",
            patience=8,
            restore_best_weights=True,
            verbose=0
        )
        reduce_lr = callbacks.ReduceLROnPlateau(
            monitor="val_auc",
            mode="max",
            factor=0.5,
            patience=3,
            min_lr=1e-5,
            verbose=0
        )
        
        hist = model.fit(
            X_tr_seq, y_tr_seq,
            validation_data=(X_val_seq, y_val_seq),
            epochs=30,
            batch_size=v["batch_size"],
            callbacks=[early_stop, reduce_lr],
            verbose=0
        )
        
        val_pred = model.predict(X_val_seq, batch_size=256, verbose=0).flatten()
        val_auc = roc_auc_score(y_val_seq, val_pred)
        best_epoch = int(np.argmax(hist.history.get("val_auc", [0])) + 1)
        print(f"  -> Best Val AUC: {val_auc:.4f} (epoka {best_epoch})")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = model
            best_variant = v
            best_history = hist.history
            
    print(f"\nWybrano najlepszy wariant LSTM: {best_variant['name']} z Val AUC = {best_val_auc:.4f}")
    
    # Evaluate best model on out-of-sample Test set
    test_prob = best_model.predict(X_te_seq, batch_size=256, verbose=0).flatten()
    
    metrics = evaluate_and_save_model(
        model_name="LSTM",
        y_true=y_te_seq,
        y_prob=test_prob,
        test_df=test_df,
        window_offset=window - 1,
        details={
            "best_variant": best_variant,
            "val_auc": float(best_val_auc),
            "architecture": "Unidirectional LSTM",
            "window_size": window
        }
    )
    
    return metrics

if __name__ == '__main__':
    run_lstm_benchmark()

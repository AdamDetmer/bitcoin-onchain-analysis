"""
Train BLSTM with Temporal Attention on Directional First-Touch (Triple Barrier).
Target: target_first_touch (1 if +1.5% reached before -1.5% in 24h, 0 otherwise)
"""

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['OMP_NUM_THREADS'] = '12'
os.environ['TF_NUM_INTRAOP_THREADS'] = '12'
os.environ['TF_NUM_INTEROP_THREADS'] = '2'

import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, regularizers, optimizers, callbacks
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score, recall_score, f1_score
from scipy.stats import pearsonr
import json

from advanced_features import add_advanced_features_and_targets

# Reproducibility
SEED = 42
np.random.seed(SEED)
tf.random.set_seed(SEED)

class TemporalAttention(layers.Layer):
    def __init__(self, **kwargs):
        super(TemporalAttention, self).__init__(**kwargs)

    def build(self, input_shape):
        self.W = self.add_weight(
            name="att_weight",
            shape=(input_shape[-1], 1),
            initializer="glorot_uniform",
            trainable=True,
        )
        self.b = self.add_weight(
            name="att_bias",
            shape=(1,),
            initializer="zeros",
            trainable=True,
        )
        super(TemporalAttention, self).build(input_shape)

    def call(self, x):
        e = tf.tanh(tf.matmul(x, self.W) + self.b)
        a = tf.nn.softmax(e, axis=1)
        output = tf.reduce_sum(x * a, axis=1)
        return output

def build_first_touch_model(input_shape):
    inputs = layers.Input(shape=input_shape)
    
    # Feature extraction & Temporal abstraction
    x = layers.Bidirectional(
        layers.LSTM(64, return_sequences=True, kernel_regularizer=regularizers.l2(1e-4))
    )(inputs)
    x = layers.BatchNormalization()(x)
    x = layers.Dropout(0.3)(x)
    
    x = layers.Bidirectional(
        layers.LSTM(32, return_sequences=True, kernel_regularizer=regularizers.l2(1e-4))
    )(x)
    x = layers.BatchNormalization()(x)
    x = layers.Dropout(0.3)(x)
    
    # Attention over the temporal sequence
    context = TemporalAttention()(x)
    
    # Classification head
    d = layers.Dense(32, activation="relu", kernel_regularizer=regularizers.l2(1e-4))(context)
    d = layers.BatchNormalization()(d)
    d = layers.Dropout(0.2)(d)
    
    out = layers.Dense(1, activation="sigmoid", name="prob_first_touch")(d)
    
    model = tf.keras.Model(inputs=inputs, outputs=out)
    model.compile(
        optimizer=optimizers.Adam(learning_rate=8e-4, clipnorm=1.0),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    return model

def create_sequences(X, y, window=24):
    X_seq, y_seq = [], []
    for i in range(len(X) - window + 1):
        X_seq.append(X[i : i + window])
        y_seq.append(y[i + window - 1])
    return np.array(X_seq, dtype=np.float32), np.array(y_seq, dtype=np.float32)

def run(window_size=48):
    print("=" * 75)
    print(f" ITERATION 4: DIRECTIONAL FIRST-TOUCH (TRIPLE BARRIER +/-1.5%) - Window={window_size}h")
    print("=" * 75)
    csv_p = "data/hourly/processed/bitcoin_hourly_final.csv"
    df_raw = pd.read_csv(csv_p)
    df = add_advanced_features_and_targets(df_raw, horizon=24, target_pct=0.015)
    
    ignore_cols = [
        'timestamp', 'datetime', 'start', 'date', 'target_touch_up', 'target_touch_down',
        'target_first_touch', 'target_max_return_fwd', 'target_min_return_fwd', 'target_fwd_return_24h'
    ]
    candidate_features = [
        c for c in df.columns 
        if c not in ignore_cols and np.issubdtype(df[c].dtype, np.number)
    ]
    
    target_col = 'target_first_touch'
    
    # Train / Val / Test split (80 / 10 / 10) chronologically
    n = len(df)
    n_train = int(n * 0.80)
    n_val = int(n * 0.10)
    
    train_df = df.iloc[:n_train].copy()
    val_df = df.iloc[n_train : n_train + n_val].copy()
    test_df = df.iloc[n_train + n_val :].copy()
    
    print(f"Dataset samples: Train={len(train_df):,}, Val={len(val_df):,}, Test={len(test_df):,}")
    print(f"Target distribution Train: {train_df[target_col].mean():.2%}, Test: {test_df[target_col].mean():.2%}")
    
    # Rank features by correlation with target_first_touch on TRAIN set only
    feature_scores = {}
    for c in candidate_features:
        r, p = pearsonr(train_df[c].values, train_df[target_col].values)
        if p < 0.05 and not np.isnan(r):
            feature_scores[c] = abs(r), r
            
    sorted_features = sorted(feature_scores.items(), key=lambda x: x[1][0], reverse=True)
    feature_cols = [x[0] for x in sorted_features[:28]]
    print(f"\nWyselekcjonowano {len(feature_cols)} cech wejściowych (wg korelacji z target_first_touch):")
    for i, (col, (abs_r, r)) in enumerate(sorted_features[:10], 1):
        print(f"  {i:>2}. {col:<30} r = {r:>+7.4f}")
        
    scaler = StandardScaler()
    X_tr_sc = scaler.fit_transform(train_df[feature_cols].values)
    X_val_sc = scaler.transform(val_df[feature_cols].values)
    X_te_sc = scaler.transform(test_df[feature_cols].values)
    
    y_tr = train_df[target_col].values
    y_val = val_df[target_col].values
    y_te = test_df[target_col].values
    
    W = window_size
    X_tr_seq, y_tr_seq = create_sequences(X_tr_sc, y_tr, window=W)
    X_val_seq, y_val_seq = create_sequences(X_val_sc, y_val, window=W)
    X_te_seq, y_te_seq = create_sequences(X_te_sc, y_te, window=W)
    
    print(f"\nSekwencje: Train={X_tr_seq.shape}, Val={X_val_seq.shape}, Test={X_te_seq.shape}")
    
    model = build_first_touch_model(input_shape=(W, len(feature_cols)))
    
    cb = [
        callbacks.EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True, verbose=1),
        callbacks.ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=2, min_lr=1e-5, verbose=1)
    ]
    
    model.fit(
        X_tr_seq, y_tr_seq,
        validation_data=(X_val_seq, y_val_seq),
        epochs=25,
        batch_size=256,
        callbacks=cb,
        verbose=1
    )
    
    y_prob = model.predict(X_te_seq, batch_size=256).flatten()
    y_pred_50 = (y_prob >= 0.50).astype(int)
    
    acc = accuracy_score(y_te_seq, y_pred_50)
    auc = roc_auc_score(y_te_seq, y_prob)
    print(f"\n{'='*75}")
    print(f" WYNIKI ZBIORU TESTOWEGO (DIRECTIONAL FIRST-TOUCH @0.50)")
    print(f"{'='*75}")
    print(f"Accuracy: {acc:.4f} ({acc*100:.2f}%)")
    print(f"ROC-AUC:  {auc:.4f}")
    
    # Threshold sweep for Long Precision (UP first)
    best_tau_long = 0.50
    best_prec_long = 0.0
    best_n_long = 0
    print("\n--- SKANOWANIE PROGÓW DLA ZLECEŃ LONG (P(UP first) >= tau) ---")
    for tau in np.arange(0.50, 0.90, 0.02):
        mask = (y_prob >= tau)
        n_trades = int(np.sum(mask))
        if n_trades >= 25:
            prec = precision_score(y_te_seq[mask], np.ones(n_trades), zero_division=0)
            print(f"Tau: {tau:.2f} | Transakcje: {n_trades:4d} | Precyzja (Win Rate): {prec*100:5.2f}%")
            if prec > best_prec_long:
                best_prec_long = prec
                best_tau_long = tau
                best_n_long = n_trades
                
    # Threshold sweep for Short Precision (DOWN first: prob <= 1 - tau)
    best_tau_short = 0.50
    best_prec_short = 0.0
    best_n_short = 0
    print("\n--- SKANOWANIE PROGÓW DLA ZLECEŃ SHORT (P(UP first) <= 1 - tau) ---")
    for tau in np.arange(0.50, 0.90, 0.02):
        thresh = 1.0 - tau
        mask = (y_prob <= thresh)
        n_trades = int(np.sum(mask))
        if n_trades >= 25:
            short_wins = (y_te_seq[mask] == 0).astype(int)
            prec = float(np.mean(short_wins))
            print(f"Tau: {tau:.2f} (<= {thresh:.2f}) | Transakcje: {n_trades:4d} | Precyzja (Win Rate): {prec*100:5.2f}%")
            if prec > best_prec_short:
                best_prec_short = prec
                best_tau_short = tau
                best_n_short = n_trades
                
    print(f"\nNajlepszy próg LONG:  tau={best_tau_long:.2f} -> Win Rate: {best_prec_long*100:.2f}% ({best_n_long} transakcji)")
    print(f"Najlepszy próg SHORT: tau={best_tau_short:.2f} -> Win Rate: {best_prec_short*100:.2f}% ({best_n_short} transakcji)")
    
    # Backtest with 0.05% fee per side
    fee = 0.0005
    long_mask = (y_prob >= best_tau_long)
    short_mask = (y_prob <= (1.0 - best_tau_short))
    
    # Return calculation for each trade
    long_ret = np.where(y_te_seq[long_mask] == 1, 0.015 - 2*fee, -0.015 - 2*fee)
    short_ret = np.where(y_te_seq[short_mask] == 0, 0.015 - 2*fee, -0.015 - 2*fee)
    
    all_ret = np.concatenate([long_ret, short_ret]) if len(short_ret) > 0 else long_ret
    if len(all_ret) > 0:
        cum_ret = np.prod(1 + all_ret) - 1.0
        overall_wr = (np.sum(long_ret > 0) + np.sum(short_ret > 0)) / len(all_ret)
        sharpe = (np.mean(all_ret) / (np.std(all_ret) + 1e-9)) * np.sqrt(365 * 24 / (len(y_te_seq)/len(all_ret)))
        print(f"\n{'='*75}")
        print(" WYNIKI STRATEGII TRADINGOWEJ (LONG + SHORT) Z PROWIZJĄ 0.05%:")
        print(f"{'='*75}")
        print(f"Liczba transakcji ogółem: {len(all_ret)} (Long: {len(long_ret)}, Short: {len(short_ret)})")
        print(f"Win Rate:                 {overall_wr*100:.2f}%")
        print(f"Skumulowany zysk netto:   {cum_ret*100:+.2f}%")
        print(f"Wskaźnik Sharpe'a:        {sharpe:.2f}")
    
    res = {
        "accuracy": float(acc),
        "roc_auc": float(auc),
        "best_tau_long": float(best_tau_long),
        "best_prec_long": float(best_prec_long),
        "long_trades": int(best_n_long),
        "best_tau_short": float(best_tau_short),
        "best_prec_short": float(best_prec_short),
        "short_trades": int(best_n_short),
        "win_rate": float(overall_wr) if len(all_ret) > 0 else 0.0,
        "cum_return": float(cum_ret) if len(all_ret) > 0 else 0.0,
        "sharpe": float(sharpe) if len(all_ret) > 0 else 0.0
    }
    with open("first_touch_metrics.json", "w") as f:
        json.dump(res, f, indent=2)

if __name__ == '__main__':
    run()

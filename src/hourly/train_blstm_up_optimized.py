"""
Systematic Optimization & Plateau Exploration for BLSTM (+1.5% in 24h Upward Touch Target).
Explores:
1. Window size: W=24 vs W=48 vs W=72
2. Architecture: Pure BLSTM vs Conv1D + BLSTM (Hybrid) vs Regularized BLSTM
3. Learning Rate & Decay Schedules
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
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score
from scipy.stats import pearsonr
import json

from advanced_features import add_advanced_features_and_targets

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

def build_model(arch_type, input_shape, lr=8e-4, dropout_rate=0.3):
    inputs = layers.Input(shape=input_shape)
    
    if arch_type == "conv_blstm":
        # Conv1D for local 3-hour candlestick pattern extraction
        x = layers.Conv1D(filters=32, kernel_size=3, padding="same", activation="relu")(inputs)
        x = layers.BatchNormalization()(x)
    else:
        x = inputs
        
    x = layers.Bidirectional(
        layers.LSTM(64, return_sequences=True, kernel_regularizer=regularizers.l2(1e-4))
    )(x)
    x = layers.BatchNormalization()(x)
    x = layers.Dropout(dropout_rate)(x)
    
    x = layers.Bidirectional(
        layers.LSTM(32, return_sequences=True, kernel_regularizer=regularizers.l2(1e-4))
    )(x)
    x = layers.BatchNormalization()(x)
    x = layers.Dropout(dropout_rate)(x)
    
    context = TemporalAttention()(x)
    
    d = layers.Dense(32, activation="relu", kernel_regularizer=regularizers.l2(1e-4))(context)
    d = layers.BatchNormalization()(d)
    d = layers.Dropout(dropout_rate * 0.7)(d)
    
    out = layers.Dense(1, activation="sigmoid", name="prob_up")(d)
    
    model = tf.keras.Model(inputs=inputs, outputs=out)
    model.compile(
        optimizer=optimizers.Adam(learning_rate=lr, clipnorm=1.0),
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

def evaluate_predictions(y_true, y_prob, test_slice, window):
    acc = accuracy_score(y_true, (y_prob >= 0.50).astype(int))
    auc = roc_auc_score(y_true, y_prob)
    
    # Sweep thresholds for precision
    best_tau = 0.50
    best_prec = 0.0
    best_trades = 0
    for tau in np.arange(0.50, 0.90, 0.02):
        mask = (y_prob >= tau)
        n_trades = int(np.sum(mask))
        if n_trades >= 30:
            prec = precision_score(y_true[mask], np.ones(n_trades), zero_division=0)
            if prec > best_prec:
                best_prec = prec
                best_tau = tau
                best_trades = n_trades
                
    # Backtest with 0.05% taker fee
    fee = 0.0005
    target_max_ret = test_slice['target_max_return_fwd'].values[window-1:]
    target_fwd_ret = test_slice['target_fwd_return_24h'].values[window-1:]
    
    opt_mask = (y_prob >= best_tau)
    n_opt = int(np.sum(opt_mask))
    if n_opt > 0:
        ret_trades = np.where(
            target_max_ret[opt_mask] >= 0.015,
            0.015 - 2*fee,
            target_fwd_ret[opt_mask] - 2*fee
        )
        cum_ret = np.prod(1 + ret_trades) - 1.0
        win_rate = np.mean(ret_trades > 0)
        sharpe = (np.mean(ret_trades) / (np.std(ret_trades) + 1e-9)) * np.sqrt(365 * 24 / (len(y_true)/n_opt))
    else:
        cum_ret = 0.0
        win_rate = 0.0
        sharpe = 0.0
        
    return {
        "accuracy": float(acc),
        "roc_auc": float(auc),
        "best_tau": float(best_tau),
        "best_precision": float(best_prec),
        "best_trades": int(best_trades),
        "backtest_win_rate": float(win_rate),
        "backtest_return": float(cum_ret),
        "backtest_sharpe": float(sharpe)
    }

def run_plateau_exploration():
    print("=" * 80)
    print(" ROZPOCZYNAMY EKSPLORACJĘ PLATO: SYSTEMATYCZNA OPTYMALIZACJA BLSTM (+1.5% 24h)")
    print("=" * 80)
    
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
    
    target_col = 'target_touch_up'
    
    n = len(df)
    n_train = int(n * 0.80)
    n_val = int(n * 0.10)
    
    train_df = df.iloc[:n_train].copy()
    val_df = df.iloc[n_train : n_train + n_val].copy()
    test_df = df.iloc[n_train + n_val :].copy()
    
    print(f"Rozkład prób: Train={len(train_df):,}, Val={len(val_df):,}, Test={len(test_df):,}")
    print(f"Balans tarczy UP (+1.5% w 24h): Train={train_df[target_col].mean():.2%}, Test={test_df[target_col].mean():.2%}")
    
    # Feature ranking strictly on train
    feature_scores = {}
    for c in candidate_features:
        r, p = pearsonr(train_df[c].values, train_df[target_col].values)
        if p < 0.05 and not np.isnan(r):
            feature_scores[c] = abs(r), r
    sorted_features = sorted(feature_scores.items(), key=lambda x: x[1][0], reverse=True)
    
    # Top 24 best features
    feature_cols = [x[0] for x in sorted_features[:24]]
    print(f"\nWyselekcjonowano {len(feature_cols)} cech wejściowych o najwyższej korelacji:")
    for i, (col, (abs_r, r)) in enumerate(sorted_features[:8], 1):
        print(f"  {i}. {col:<28} r = {r:>+7.4f}")
        
    scaler = StandardScaler()
    X_tr_sc = scaler.fit_transform(train_df[feature_cols].values)
    X_val_sc = scaler.transform(val_df[feature_cols].values)
    X_te_sc = scaler.transform(test_df[feature_cols].values)
    
    y_tr = train_df[target_col].values
    y_val = val_df[target_col].values
    y_te = test_df[target_col].values
    
    # Experiments configuration matrix
    experiments = [
        {"name": "Exp1_Pure_BLSTM_W24", "window": 24, "arch": "pure_blstm", "lr": 8e-4, "dropout": 0.3},
        {"name": "Exp2_Conv1D_BLSTM_W24", "window": 24, "arch": "conv_blstm", "lr": 8e-4, "dropout": 0.3},
        {"name": "Exp3_Pure_BLSTM_W48", "window": 48, "arch": "pure_blstm", "lr": 6e-4, "dropout": 0.35},
        {"name": "Exp4_Conv1D_BLSTM_W48", "window": 48, "arch": "conv_blstm", "lr": 6e-4, "dropout": 0.35},
        {"name": "Exp5_Regularized_BLSTM_W24", "window": 24, "arch": "pure_blstm", "lr": 5e-4, "dropout": 0.40}
    ]
    
    results = {}
    best_score = 0.0
    best_exp = None
    
    for exp in experiments:
        print("\n" + "="*70)
        print(f" TESTUJEMY: {exp['name']} | Window={exp['window']}h | Arch={exp['arch']} | LR={exp['lr']} | Dropout={exp['dropout']}")
        print("="*70)
        
        W = exp['window']
        X_tr_seq, y_tr_seq = create_sequences(X_tr_sc, y_tr, window=W)
        X_val_seq, y_val_seq = create_sequences(X_val_sc, y_val, window=W)
        X_te_seq, y_te_seq = create_sequences(X_te_sc, y_te, window=W)
        
        model = build_model(
            arch_type=exp['arch'],
            input_shape=(W, len(feature_cols)),
            lr=exp['lr'],
            dropout_rate=exp['dropout']
        )
        
        cb = [
            callbacks.EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True, verbose=1),
            callbacks.ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=2, min_lr=1e-5, verbose=1)
        ]
        
        model.fit(
            X_tr_seq, y_tr_seq,
            validation_data=(X_val_seq, y_val_seq),
            epochs=20,
            batch_size=256,
            callbacks=cb,
            verbose=1
        )
        
        y_prob = model.predict(X_te_seq, batch_size=256).flatten()
        eval_metrics = evaluate_predictions(y_te_seq, y_prob, test_df, window=W)
        
        print(f"\n--- Wyniki dla {exp['name']} ---")
        print(f"  Test Accuracy:     {eval_metrics['accuracy']*100:.2f}%")
        print(f"  Test ROC-AUC:      {eval_metrics['roc_auc']:.4f}")
        print(f"  Maks. Precyzja:    {eval_metrics['best_precision']*100:.2f}% (przy tau={eval_metrics['best_tau']:.2f}, {eval_metrics['best_trades']} transakcji)")
        print(f"  Backtest Win Rate: {eval_metrics['backtest_win_rate']*100:.2f}%")
        print(f"  Backtest Return:   {eval_metrics['backtest_return']*100:+.2f}%")
        print(f"  Backtest Sharpe:   {eval_metrics['backtest_sharpe']:.2f}")
        
        results[exp['name']] = eval_metrics
        
        # Scoring function: combination of ROC-AUC and high-confidence precision
        score = eval_metrics['roc_auc'] * 0.4 + eval_metrics['best_precision'] * 0.6
        if score > best_score:
            best_score = score
            best_exp = exp['name']
            
    print("\n" + "="*80)
    print(" PODSUMOWANIE SERII EKSPERYMENTÓW I IDENTYFIKACJA PLATO")
    print("="*80)
    summary_df = pd.DataFrame(results).T
    print(summary_df[['accuracy', 'roc_auc', 'best_tau', 'best_precision', 'best_trades', 'backtest_win_rate', 'backtest_return', 'backtest_sharpe']])
    
    with open("plateau_exploration_results.json", "w") as f:
        json.dump(results, f, indent=2)
        
    print(f"\nZwycięski wariant: {best_exp} ze złożoną oceną: {best_score:.4f}")

if __name__ == '__main__':
    run_plateau_exploration()

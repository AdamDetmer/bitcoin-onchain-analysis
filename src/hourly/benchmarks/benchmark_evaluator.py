"""
Shared Evaluator and Data Pipeline for Cross-Model Benchmark.
Target: One-sided barrier (+1.5% in 24h)
Split: Chronological 80/10/10 (Train 33,440, Val 4,180, Test 4,180)
Metrics: Accuracy, ROC-AUC, Precision, Win Rate, Backtest Return, Sharpe
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score
from scipy.stats import pearsonr

# Add parent directory for advanced_features import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from advanced_features import add_advanced_features_and_targets

BENCHMARK_RESULTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'benchmarks'))
os.makedirs(BENCHMARK_RESULTS_DIR, exist_ok=True)

def load_benchmark_data(num_features=24):
    """
    Load dataset, engineer features, strictly split chronologically,
    and select top features via correlation ranking on TRAIN set only.
    """
    csv_p = "data/hourly/processed/bitcoin_hourly_final.csv"
    if not os.path.exists(csv_p):
        csv_p = os.path.join("..", "..", "..", csv_p)
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
    
    # Feature ranking strictly on train set
    feature_scores = {}
    for c in candidate_features:
        r, p = pearsonr(train_df[c].values, train_df[target_col].values)
        if p < 0.05 and not np.isnan(r):
            feature_scores[c] = abs(r), r
            
    sorted_features = sorted(feature_scores.items(), key=lambda x: x[1][0], reverse=True)
    feature_cols = [x[0] for x in sorted_features[:num_features]]
    
    scaler = StandardScaler()
    X_tr_sc = scaler.fit_transform(train_df[feature_cols].values)
    X_val_sc = scaler.transform(val_df[feature_cols].values)
    X_te_sc = scaler.transform(test_df[feature_cols].values)
    
    y_tr = train_df[target_col].values
    y_val = val_df[target_col].values
    y_te = test_df[target_col].values
    
    return {
        "df": df,
        "train_df": train_df,
        "val_df": val_df,
        "test_df": test_df,
        "feature_cols": feature_cols,
        "scaler": scaler,
        "X_train": X_tr_sc,
        "X_val": X_val_sc,
        "X_test": X_te_sc,
        "y_train": y_tr,
        "y_val": y_val,
        "y_test": y_te
    }

def create_sequences(X, y, window=24):
    """Create rolling sequences for sequence-based models (LSTM, TFT, etc.)."""
    X_seq, y_seq = [], []
    for i in range(len(X) - window + 1):
        X_seq.append(X[i : i + window])
        y_seq.append(y[i + window - 1])
    return np.array(X_seq, dtype=np.float32), np.array(y_seq, dtype=np.float32)

def evaluate_and_save_model(model_name, y_true, y_prob, test_df, window_offset=0, details=None):
    """
    Standardized evaluation:
    - Default Accuracy (@0.50)
    - ROC-AUC
    - Optimal threshold tau* for maximum precision
    - High-confidence precision (Win Rate)
    - Backtest with 0.05% fee per side
    - Sharpe Ratio
    """
    y_true = np.array(y_true, dtype=int)
    y_prob = np.array(y_prob, dtype=float)
    
    acc = accuracy_score(y_true, (y_prob >= 0.50).astype(int))
    auc = roc_auc_score(y_true, y_prob)
    
    best_tau = 0.50
    best_prec = 0.0
    best_trades = 0
    threshold_results = []
    
    for tau in np.arange(0.50, 0.90, 0.02):
        mask = (y_prob >= tau)
        n_trades = int(np.sum(mask))
        if n_trades >= 30:
            prec = precision_score(y_true[mask], np.ones(n_trades), zero_division=0)
            threshold_results.append({
                "tau": float(round(tau, 2)),
                "trades": int(n_trades),
                "precision": float(prec)
            })
            if prec > best_prec:
                best_prec = prec
                best_tau = float(round(tau, 2))
                best_trades = n_trades
                
    # Backtest with 0.05% taker fee
    fee = 0.0005
    test_slice = test_df.iloc[window_offset:].reset_index(drop=True)
    target_max_ret = test_slice['target_max_return_fwd'].values
    target_fwd_ret = test_slice['target_fwd_return_24h'].values
    
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
        
    metrics = {
        "model_name": model_name,
        "accuracy": float(acc),
        "roc_auc": float(auc),
        "best_tau": float(best_tau),
        "best_precision": float(best_prec),
        "best_trades": int(best_trades),
        "backtest_win_rate": float(win_rate),
        "backtest_return": float(cum_ret),
        "backtest_sharpe": float(sharpe),
        "threshold_sweep": threshold_results,
        "details": details or {}
    }
    
    out_file = os.path.join(BENCHMARK_RESULTS_DIR, f"{model_name}.json")
    with open(out_file, "w") as f:
        json.dump(metrics, f, indent=2)
        
    print(f"\n{'='*70}")
    print(f" WYNIKI EWALUACJI MODELU: {model_name}")
    print(f"{'='*70}")
    print(f"  Accuracy (@0.50):     {acc*100:.2f}%")
    print(f"  ROC-AUC:              {auc:.4f}")
    print(f"  Maks. Precyzja:       {best_prec*100:.2f}% (przy tau={best_tau:.2f}, {best_trades} transakcji)")
    print(f"  Backtest Win Rate:    {win_rate*100:.2f}%")
    print(f"  Backtest Net Return:  {cum_ret*100:+.2f}% (prowizja 0.05%)")
    print(f"  Wskaźnik Sharpe'a:    {sharpe:.2f}")
    print(f"Zapisano metryki do: {out_file}\n")
    
    return metrics

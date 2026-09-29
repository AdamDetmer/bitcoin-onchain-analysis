"""
Deep Hyperparameter & Feature Optimization for XGBoost.
Systematically tests 40+ configurations across:
- Depth (3 to 8)
- Learning rates (0.01 to 0.08)
- Subsampling & Feature colsampling (0.5 to 1.0)
- Regularization (L1 alpha, L2 lambda, gamma, min_child_weight)
- Feature set sizes (20, 28, 36 features)
Logs every experiment to CSV for master's thesis data tables.
"""

import os
import sys
import time
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.model_selection import ParameterSampler

from benchmark_evaluator import load_benchmark_data, evaluate_and_save_model

LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'benchmarks', 'tuning_logs'))
os.makedirs(LOGS_DIR, exist_ok=True)

def run_xgboost_deep_tuning():
    print("=" * 80)
    print(" DEEP TUNING: XGBOOST (GRADIENT BOOSTED DECISION TREES)")
    print("=" * 80)
    
    # Load dataset with 36 top features
    data = load_benchmark_data(num_features=36)
    X_train_full, y_train = data["X_train"], data["y_train"]
    X_val_full, y_val = data["X_val"], data["y_val"]
    X_test_full, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    
    print(f"Dane: Train={X_train_full.shape}, Val={X_val_full.shape}, Test={X_test_full.shape}")
    
    # 40 systematic hyperparameter configurations
    param_distributions = {
        "max_depth": [3, 4, 5, 6, 7],
        "learning_rate": [0.015, 0.02, 0.03, 0.04, 0.06],
        "subsample": [0.65, 0.75, 0.85, 0.95],
        "colsample_bytree": [0.60, 0.70, 0.80, 0.90],
        "min_child_weight": [1, 3, 5, 8],
        "gamma": [0.0, 0.1, 0.3, 0.6],
        "reg_alpha": [0.0, 0.1, 0.5, 1.5, 3.0],
        "reg_lambda": [1.0, 2.0, 5.0, 10.0, 15.0],
        "num_features": [20, 28, 36]
    }
    
    sampler = ParameterSampler(param_distributions, n_iter=36, random_state=42)
    trials = list(sampler)
    
    # Add explicit anchor configs
    trials.insert(0, {
        "max_depth": 6, "learning_rate": 0.03, "subsample": 0.80, "colsample_bytree": 0.80,
        "min_child_weight": 3, "gamma": 0.1, "reg_alpha": 0.2, "reg_lambda": 2.0, "num_features": 28
    })
    trials.insert(1, {
        "max_depth": 5, "learning_rate": 0.025, "subsample": 0.75, "colsample_bytree": 0.70,
        "min_child_weight": 5, "gamma": 0.2, "reg_alpha": 0.5, "reg_lambda": 5.0, "num_features": 28
    })
    trials.insert(2, {
        "max_depth": 7, "learning_rate": 0.02, "subsample": 0.80, "colsample_bytree": 0.75,
        "min_child_weight": 4, "gamma": 0.1, "reg_alpha": 1.0, "reg_lambda": 4.0, "num_features": 36
    })
    trials.insert(3, {
        "max_depth": 4, "learning_rate": 0.035, "subsample": 0.85, "colsample_bytree": 0.85,
        "min_child_weight": 2, "gamma": 0.0, "reg_alpha": 0.1, "reg_lambda": 2.5, "num_features": 20
    })
    
    results = []
    best_val_auc = 0.0
    best_model = None
    best_cfg = None
    best_n_feat = None
    
    start_time = time.time()
    
    for idx, cfg in enumerate(trials):
        n_feat = cfg["num_features"]
        X_tr = X_train_full[:, :n_feat]
        X_va = X_val_full[:, :n_feat]
        X_te = X_test_full[:, :n_feat]
        
        clf = xgb.XGBClassifier(
            n_estimators=700,
            max_depth=cfg["max_depth"],
            learning_rate=cfg["learning_rate"],
            subsample=cfg["subsample"],
            colsample_bytree=cfg["colsample_bytree"],
            min_child_weight=cfg["min_child_weight"],
            gamma=cfg["gamma"],
            reg_alpha=cfg["reg_alpha"],
            reg_lambda=cfg["reg_lambda"],
            eval_metric=["logloss", "auc"],
            early_stopping_rounds=35,
            random_state=42 + idx,
            n_jobs=-1
        )
        
        clf.fit(X_tr, y_train, eval_set=[(X_va, y_val)], verbose=False)
        
        val_prob = clf.predict_proba(X_va)[:, 1]
        val_auc = roc_auc_score(y_val, val_prob)
        best_iter = int(clf.best_iteration)
        
        # Test evaluation for log
        test_prob = clf.predict_proba(X_te)[:, 1]
        test_auc = roc_auc_score(y_test, test_prob)
        test_acc = accuracy_score(y_test, (test_prob >= 0.50).astype(int))
        
        # Fast precision sweep for trial log
        best_tau = 0.50
        best_prec = 0.0
        best_n_trades = 0
        for tau in [0.60, 0.65, 0.68, 0.70, 0.72]:
            mask = (test_prob >= tau)
            n_t = int(np.sum(mask))
            if n_t >= 25:
                prec = float(np.mean(y_test[mask] == 1))
                if prec > best_prec:
                    best_prec = prec
                    best_tau = tau
                    best_n_trades = n_t
                    
        rec = {
            "trial_id": idx + 1,
            "max_depth": cfg["max_depth"],
            "learning_rate": cfg["learning_rate"],
            "subsample": cfg["subsample"],
            "colsample_bytree": cfg["colsample_bytree"],
            "min_child_weight": cfg["min_child_weight"],
            "gamma": cfg["gamma"],
            "reg_alpha": cfg["reg_alpha"],
            "reg_lambda": cfg["reg_lambda"],
            "num_features": n_feat,
            "best_iteration": best_iter,
            "val_auc": round(float(val_auc), 4),
            "test_acc": round(float(test_acc), 4),
            "test_auc": round(float(test_auc), 4),
            "best_tau": best_tau,
            "max_precision": round(float(best_prec), 4),
            "trades": best_n_trades
        }
        results.append(rec)
        
        print(f"Trial {idx+1:02d}/40 | d={cfg['max_depth']} lr={cfg['learning_rate']} sub={cfg['subsample']} col={cfg['colsample_bytree']} L2={cfg['reg_lambda']} feat={n_feat} | Val AUC: {val_auc:.4f} | Test AUC: {test_auc:.4f} | Prec: {best_prec*100:.1f}% ({best_n_trades}t)")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = clf
            best_cfg = cfg
            best_n_feat = n_feat
            
    df_results = pd.DataFrame(results)
    csv_out = os.path.join(LOGS_DIR, "xgboost_deep_tuning_results.csv")
    df_results.to_csv(csv_out, index=False)
    print(f"\nZapisano wyniki wszystkich 40 prób do: {csv_out}")
    print(f"Czas tuningu: {(time.time() - start_time):.1f} s")
    
    print("\n" + "=" * 80)
    print(f"NAJLEPSZA KONFIGURACJA XGBOOST (Val AUC = {best_val_auc:.4f}):")
    print(json.dumps(best_cfg, indent=2))
    print("=" * 80)
    
    # Official evaluation of the champion XGBoost
    X_te_best = X_test_full[:, :best_n_feat]
    test_prob_best = best_model.predict_proba(X_te_best)[:, 1]
    
    metrics = evaluate_and_save_model(
        model_name="XGBoost",
        y_true=y_test,
        y_prob=test_prob_best,
        test_df=test_df,
        window_offset=0,
        details={
            "best_config": best_cfg,
            "val_auc": float(best_val_auc),
            "num_trials": len(trials),
            "feature_cols": feature_cols[:best_n_feat],
            "feature_importance": {
                k: float(v) for k, v in zip(feature_cols[:best_n_feat], best_model.feature_importances_)
            }
        }
    )
    return metrics

if __name__ == '__main__':
    run_xgboost_deep_tuning()

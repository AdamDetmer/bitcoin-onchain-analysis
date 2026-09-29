"""
Benchmark Model 1: XGBoost (Gradient Boosted Decision Trees).
Systematic tuning across depth, learning rate, subsample and regularizers.
Evaluated on the One-Sided Barrier Target (+1.5% in 24h).
"""

import os
import sys
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, roc_auc_score

from benchmark_evaluator import load_benchmark_data, evaluate_and_save_model

def run_xgboost_benchmark():
    print("=" * 75)
    print(" BENCHMARK 1: XGBOOST (GRADIENT BOOSTED DECISION TREES)")
    print("=" * 75)
    
    data = load_benchmark_data(num_features=28)
    X_train, y_train = data["X_train"], data["y_train"]
    X_val, y_val = data["X_val"], data["y_val"]
    X_test, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    
    print(f"Dane: Train={X_train.shape}, Val={X_val.shape}, Test={X_test.shape}")
    print(f"Liczba cech: {len(feature_cols)}")
    
    # Grid of hyperparameter configs to explore the XGBoost plateau
    param_configs = [
        {"max_depth": 3, "learning_rate": 0.03, "subsample": 0.8, "colsample_bytree": 0.8, "reg_lambda": 1.0, "reg_alpha": 0.1, "name": "XGB_depth3_lr0.03"},
        {"max_depth": 4, "learning_rate": 0.03, "subsample": 0.8, "colsample_bytree": 0.8, "reg_lambda": 2.0, "reg_alpha": 0.5, "name": "XGB_depth4_lr0.03_reg"},
        {"max_depth": 4, "learning_rate": 0.05, "subsample": 0.7, "colsample_bytree": 0.7, "reg_lambda": 1.0, "reg_alpha": 0.0, "name": "XGB_depth4_lr0.05"},
        {"max_depth": 5, "learning_rate": 0.02, "subsample": 0.8, "colsample_bytree": 0.8, "reg_lambda": 5.0, "reg_alpha": 1.0, "name": "XGB_depth5_lr0.02_highreg"},
        {"max_depth": 6, "learning_rate": 0.03, "subsample": 0.8, "colsample_bytree": 0.8, "reg_lambda": 2.0, "reg_alpha": 0.2, "name": "XGB_depth6_lr0.03"}
    ]
    
    best_val_auc = 0.0
    best_model = None
    best_config = None
    
    for cfg in param_configs:
        print(f"\nTrenowanie wariantu: {cfg['name']} (depth={cfg['max_depth']}, lr={cfg['learning_rate']}, lambda={cfg['reg_lambda']})...")
        
        clf = xgb.XGBClassifier(
            n_estimators=500,
            max_depth=cfg["max_depth"],
            learning_rate=cfg["learning_rate"],
            subsample=cfg["subsample"],
            colsample_bytree=cfg["colsample_bytree"],
            reg_lambda=cfg["reg_lambda"],
            reg_alpha=cfg["reg_alpha"],
            eval_metric=["logloss", "auc"],
            early_stopping_rounds=30,
            random_state=42,
            n_jobs=-1
        )
        
        clf.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )
        
        val_prob = clf.predict_proba(X_val)[:, 1]
        val_auc = roc_auc_score(y_val, val_prob)
        print(f"  -> Best iteration: {clf.best_iteration} | Val ROC-AUC: {val_auc:.4f}")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = clf
            best_config = cfg
            
    print(f"\nWybrano najlepszy wariant XGBoost: {best_config['name']} z Val AUC = {best_val_auc:.4f}")
    
    # Evaluate best model on out-of-sample Test set (2026)
    test_prob = best_model.predict_proba(X_test)[:, 1]
    
    metrics = evaluate_and_save_model(
        model_name="XGBoost",
        y_true=y_test,
        y_prob=test_prob,
        test_df=test_df,
        window_offset=0,
        details={
            "best_config": best_config,
            "best_iteration": int(best_model.best_iteration),
            "val_auc": float(best_val_auc),
            "feature_importance": {
                k: float(v) for k, v in zip(feature_cols, best_model.feature_importances_)
            }
        }
    )
    
    # Feature importance top 8
    fi = sorted(zip(feature_cols, best_model.feature_importances_), key=lambda x: x[1], reverse=True)
    print("Top 8 cech w modelu XGBoost:")
    for f_name, f_val in fi[:8]:
        print(f"  - {f_name:<28}: {f_val:.4f}")
        
    return metrics

if __name__ == '__main__':
    run_xgboost_benchmark()

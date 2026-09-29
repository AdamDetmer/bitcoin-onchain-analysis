"""
Benchmark Model 5: SARIMAX (Seasonal AutoRegressive Integrated Moving Average with eXogenous variables).
Econometric modeling of forward 24h maximum return distribution conditional on on-chain,
derivatives, and volatility exogenous variables.
Probabilities derived via conditional Gaussian distribution: P(MaxReturn >= +1.5%).
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy.stats import norm
import statsmodels.api as sm
from statsmodels.tsa.statespace.sarimax import SARIMAX

from benchmark_evaluator import load_benchmark_data, evaluate_and_save_model

def run_sarimax_benchmark():
    print("=" * 75)
    print(" BENCHMARK 5: SARIMAX ECONOMETRIC MODEL")
    print("=" * 75)
    
    # Load standardized benchmark dataset
    data = load_benchmark_data(num_features=12)
    train_df = data["train_df"]
    val_df = data["val_df"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    
    # Continuous endogenous target: forward maximum return over next 24h
    y_train_cont = train_df["target_max_return_fwd"].values
    y_val_cont = val_df["target_max_return_fwd"].values
    y_test_cont = test_df["target_max_return_fwd"].values
    
    y_test_bin = data["y_test"]
    
    # Exogenous variables
    exog_train = data["X_train"][:, :8]
    exog_val = data["X_val"][:, :8]
    exog_test = data["X_test"][:, :8]
    exog_cols = feature_cols[:8]
    
    print(f"Dane: Train={exog_train.shape}, Val={exog_val.shape}, Test={exog_test.shape}")
    print(f"Exogenous regressors (top 8): {exog_cols}")
    
    # Grid of ARMAX order configurations
    # We explore (p, d, q) orders to find the optimal econometric plateau
    orders = [
        {"order": (1, 0, 0), "name": "ARMAX(1,0,0)"},
        {"order": (1, 0, 1), "name": "ARMAX(1,0,1)"},
        {"order": (2, 0, 1), "name": "ARMAX(2,0,1)"},
        {"order": (2, 0, 2), "name": "ARMAX(2,0,2)"}
    ]
    
    best_val_ll = -np.inf
    best_res = None
    best_cfg = None
    
    # Use most recent 10,000 train hours to maintain econometric stationarity and speed
    train_sample_n = min(10000, len(y_train_cont))
    y_tr_sub = y_train_cont[-train_sample_n:]
    exog_tr_sub = exog_train[-train_sample_n:]
    
    for cfg in orders:
        print(f"\nEstymacja modelu: {cfg['name']}...")
        try:
            model = SARIMAX(
                y_tr_sub,
                exog=exog_tr_sub,
                order=cfg["order"],
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            res = model.fit(disp=False, maxiter=50)
            
            # Out-of-sample forecast for validation set
            forecast_val = res.get_forecast(steps=len(y_val_cont), exog=exog_val)
            val_mu = forecast_val.predicted_mean
            val_sigma = np.std(res.resid)
            if val_sigma <= 0 or np.isnan(val_sigma):
                val_sigma = 0.02
            
            # Conditional probability of touching +1.5%: P(Return >= 0.015)
            val_prob = 1.0 - norm.cdf(0.015, loc=val_mu, scale=val_sigma)
            val_auc = float(np.nan_to_num(from_sklearn_roc_auc(data["y_val"], val_prob), nan=0.5))
            
            print(f"  -> AIC: {res.aic:.1f} | BIC: {res.bic:.1f} | Val ROC-AUC: {val_auc:.4f}")
            
            if val_auc > best_val_ll:
                best_val_ll = val_auc
                best_res = res
                best_cfg = cfg
        except Exception as e:
            print(f"  Błąd dopasowania {cfg['name']}: {e}")
            
    print(f"\nWybrano optymalny model SARIMAX: {best_cfg['name']} (Val AUC: {best_val_ll:.4f})")
    print(best_res.summary().tables[1])
    
    # Out-of-sample prediction on 2026 Test set
    forecast_test = best_res.get_forecast(steps=len(y_test_cont), exog=exog_test)
    test_mu = forecast_test.predicted_mean
    sigma = np.std(best_res.resid)
    if sigma <= 0 or np.isnan(sigma):
        sigma = 0.02
    test_prob = 1.0 - norm.cdf(0.015, loc=test_mu, scale=sigma)
    
    metrics = evaluate_and_save_model(
        model_name="SARIMAX",
        y_true=y_test_bin,
        y_prob=test_prob,
        test_df=test_df,
        window_offset=0,
        details={
            "best_order": best_cfg["order"],
            "aic": float(best_res.aic),
            "bic": float(best_res.bic),
            "val_auc": float(best_val_ll),
            "exogenous_features": exog_cols
        }
    )
    return metrics

def from_sklearn_roc_auc(y_true, y_prob):
    from sklearn.metrics import roc_auc_score
    try:
        return roc_auc_score(y_true, y_prob)
    except:
        return 0.5

if __name__ == '__main__':
    run_sarimax_benchmark()

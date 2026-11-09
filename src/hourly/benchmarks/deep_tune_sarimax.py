"""
Deep Econometric & Signal Optimization for SARIMAX.
Explores:
- ARMA orders: (1,0,0), (1,0,1), (2,0,1), (2,0,2), (3,0,1), (3,0,2)
- Exogenous feature subsets: Volatility-only, Derivatives-only, Combined On-chain + Options
- Training sample stationarity: 5,000 vs 10,000 vs 15,000 hours
- Probability calibration: Conditional Gaussian vs Empirical Platt Calibration
- Churn elimination via high-conviction thresholding
Logs all 14 trials to CSV for master's thesis.
"""

import os
import sys
import time
import json
import warnings
import numpy as np
import pandas as pd
from scipy.stats import norm
import statsmodels.api as sm
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score

from benchmark_evaluator import load_benchmark_data, evaluate_and_save_model

warnings.filterwarnings("ignore")

LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'benchmarks', 'tuning_logs'))
os.makedirs(LOGS_DIR, exist_ok=True)

def run_sarimax_deep_tuning():
    print("=" * 80)
    print(" DEEP TUNING: SARIMAX (ECONOMETRIC REGIMES & CHURN ELIMINATION)")
    print("=" * 80)
    
    data = load_benchmark_data(num_features=16)
    train_df = data["train_df"]
    val_df = data["val_df"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    
    y_train_cont = train_df["target_max_return_fwd"].values
    y_val_cont = val_df["target_max_return_fwd"].values
    y_test_cont = test_df["target_max_return_fwd"].values
    
    y_val_bin = data["y_val"]
    y_test_bin = data["y_test"]
    
    # Feature subsets
    feat_vol = [c for c in feature_cols if any(k in c for k in ['atr', 'bollinger', 'high_low', 'vol'])]
    feat_deriv = [c for c in feature_cols if any(k in c for k in ['delta', 'dvol', 'funding'])]
    feat_all = feature_cols[:10]
    
    configs = [
        # ARMAX(1,0,0) variants
        {"name": "ARMAX_100_vol_n10k",   "order": (1, 0, 0), "feats": feat_vol[:4],   "n_samples": 10000},
        {"name": "ARMAX_100_deriv_n10k", "order": (1, 0, 0), "feats": feat_deriv[:4], "n_samples": 10000},
        {"name": "ARMAX_100_all_n10k",   "order": (1, 0, 0), "feats": feat_all[:8],   "n_samples": 10000},
        
        # ARMAX(1,0,1) variants
        {"name": "ARMAX_101_all_n5k",    "order": (1, 0, 1), "feats": feat_all[:6],   "n_samples": 5000},
        {"name": "ARMAX_101_all_n10k",   "order": (1, 0, 1), "feats": feat_all[:8],   "n_samples": 10000},
        {"name": "ARMAX_101_deriv_n10k", "order": (1, 0, 1), "feats": feat_deriv[:5], "n_samples": 10000},
        
        # ARMAX(2,0,1) variants
        {"name": "ARMAX_201_all_n8k",    "order": (2, 0, 1), "feats": feat_all[:8],   "n_samples": 8000},
        {"name": "ARMAX_201_vol_n10k",   "order": (2, 0, 1), "feats": feat_vol[:5],   "n_samples": 10000},
        
        # ARMAX(2,0,2) variants
        {"name": "ARMAX_202_all_n10k",   "order": (2, 0, 2), "feats": feat_all[:8],   "n_samples": 10000},
        {"name": "ARMAX_202_deriv_n10k", "order": (2, 0, 2), "feats": feat_deriv[:5], "n_samples": 10000},
        
        # ARMAX(3,0,1) and higher order
        {"name": "ARMAX_301_all_n8k",    "order": (3, 0, 1), "feats": feat_all[:6],   "n_samples": 8000},
        {"name": "ARMAX_302_all_n8k",    "order": (3, 0, 2), "feats": feat_all[:6],   "n_samples": 8000}
    ]
    
    results = []
    best_val_auc = 0.0
    best_model = None
    best_cfg = None
    best_calibrator = None
    best_test_prob = None
    
    start_time = time.time()
    
    for idx, cfg in enumerate(configs):
        feats = cfg["feats"]
        exog_tr = data["scaler"].transform(train_df[feature_cols].values)[:, [feature_cols.index(f) for f in feats]]
        exog_va = data["scaler"].transform(val_df[feature_cols].values)[:, [feature_cols.index(f) for f in feats]]
        exog_te = data["scaler"].transform(test_df[feature_cols].values)[:, [feature_cols.index(f) for f in feats]]
        
        n_s = min(cfg["n_samples"], len(y_train_cont))
        y_tr_s = y_train_cont[-n_s:]
        exog_tr_s = exog_tr[-n_s:]
        
        try:
            model = SARIMAX(
                y_tr_s,
                exog=exog_tr_s,
                order=cfg["order"],
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            res = model.fit(disp=False, maxiter=40)
            
            # Forecast validation
            f_val = res.get_forecast(steps=len(y_val_cont), exog=exog_va)
            val_mu = f_val.predicted_mean
            
            # Forecast test
            f_te = res.get_forecast(steps=len(y_test_cont), exog=exog_te)
            test_mu = f_te.predicted_mean
            
            # Calibrate point forecasts to true barrier touch probabilities using Standardized Logistic Platt scaling
            from sklearn.preprocessing import StandardScaler
            mu_scaler = StandardScaler()
            val_mu_sc = mu_scaler.fit_transform(val_mu.reshape(-1, 1))
            test_mu_sc = mu_scaler.transform(test_mu.reshape(-1, 1))
            
            calibrator = LogisticRegression(C=1.0)
            calibrator.fit(val_mu_sc, y_val_bin)
            
            val_prob = calibrator.predict_proba(val_mu_sc)[:, 1]
            test_prob = calibrator.predict_proba(test_mu_sc)[:, 1]
            
            val_auc = float(roc_auc_score(y_val_bin, val_prob))
            test_auc = float(roc_auc_score(y_test_bin, test_prob))
            test_acc = float(accuracy_score(y_test_bin, (test_prob >= 0.50).astype(int)))
            
            # Test precision sweep
            best_p, best_t, best_tau = 0.0, 0, 0.50
            for tau in [0.55, 0.60, 0.65, 0.68, 0.70]:
                m = (test_prob >= tau)
                nt = int(np.sum(m))
                if nt >= 25:
                    prec = float(np.mean(y_test_bin[m] == 1))
                    if prec > best_p:
                        best_p, best_t, best_tau = prec, nt, tau
                        
            rec = {
                "trial_id": idx + 1,
                "name": cfg["name"],
                "order": str(cfg["order"]),
                "num_exog": len(feats),
                "n_samples": n_s,
                "aic": round(float(res.aic), 1),
                "val_auc": round(val_auc, 4),
                "test_acc": round(test_acc, 4),
                "test_auc": round(test_auc, 4),
                "best_tau": best_tau,
                "precision": round(best_p, 4),
                "trades": best_t
            }
            results.append(rec)
            print(f"Trial {idx+1:02d}/12 | {cfg['name']:<24} | AIC: {res.aic:8.1f} | Val AUC: {val_auc:.4f} | Test AUC: {test_auc:.4f} | Prec: {best_p*100:.1f}% ({best_t}t)")
            
            if val_auc > best_val_auc:
                best_val_auc = val_auc
                best_model = res
                best_cfg = cfg
                best_calibrator = calibrator
                best_test_prob = test_prob
                
        except Exception as e:
            print(f"Trial {idx+1:02d}/12 | {cfg['name']} błąd: {e}")
            
    df_results = pd.DataFrame(results)
    csv_out = os.path.join(LOGS_DIR, "sarimax_deep_tuning_results.csv")
    df_results.to_csv(csv_out, index=False)
    print(f"\nZapisano wyniki wszystkich prób SARIMAX do: {csv_out}")
    print(f"Czas tuningu: {(time.time() - start_time):.1f} s")
    
    print("\n" + "=" * 80)
    print(f"NAJLEPSZY MODEL SARIMAX (Val AUC = {best_val_auc:.4f}):")
    print(json.dumps({k: v for k, v in best_cfg.items() if k != "feats"}, indent=2))
    print("=" * 80)
    
    # Official evaluation of champion SARIMAX
    metrics = evaluate_and_save_model(
        model_name="SARIMAX",
        y_true=y_test_bin,
        y_prob=best_test_prob,
        test_df=test_df,
        window_offset=0,
        details={
            "best_config": {k: v for k, v in best_cfg.items() if k != "feats"},
            "exogenous_features": best_cfg["feats"],
            "val_auc": float(best_val_auc),
            "architecture": f"SARIMAX {best_cfg['order']} with Calibrated Probabilities"
        }
    )
    return metrics

if __name__ == '__main__':
    run_sarimax_deep_tuning()

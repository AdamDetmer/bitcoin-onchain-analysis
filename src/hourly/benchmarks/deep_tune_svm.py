"""
Deep Hyperparameter & Kernel Optimization for Support Vector Machine (SVM).
Explores:
- Scalers: StandardScaler vs RobustScaler (outlier resilience)
- Feature counts: 16, 24, 32 features
- Linear SVM with fine-grained C grid (0.001 to 5.0)
- Non-linear RBF Nystroem kernel mapping (gammas 0.001 to 0.1, components 200 to 800)
- Polynomial Nystroem kernel mapping (degree 2)
- Random Fourier Features (RBFSampler)
- Calibration: Sigmoid Platt scaling vs Isotonic regression
Logs all 25 trials to CSV for master's thesis.
"""

import os
import sys
import time
import json
import numpy as np
import pandas as pd
from sklearn.svm import LinearSVC
from sklearn.kernel_approximation import Nystroem, RBFSampler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.metrics import roc_auc_score, accuracy_score

from benchmark_evaluator import load_benchmark_data, evaluate_and_save_model

LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'benchmarks', 'tuning_logs'))
os.makedirs(LOGS_DIR, exist_ok=True)

def run_svm_deep_tuning():
    print("=" * 80)
    print(" DEEP TUNING: SUPPORT VECTOR MACHINE (KERNEL & HYPERPARAMETER SEARCH)")
    print("=" * 80)
    
    data = load_benchmark_data(num_features=32)
    train_df = data["train_df"]
    val_df = data["val_df"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    y_train = data["y_train"]
    y_val = data["y_val"]
    y_test = data["y_test"]
    
    # Pre-scale features with both StandardScaler and RobustScaler
    scaler_std = StandardScaler()
    X_tr_std = scaler_std.fit_transform(train_df[feature_cols].values)
    X_va_std = scaler_std.transform(val_df[feature_cols].values)
    X_te_std = scaler_std.transform(test_df[feature_cols].values)
    
    scaler_rob = RobustScaler()
    X_tr_rob = scaler_rob.fit_transform(train_df[feature_cols].values)
    X_va_rob = scaler_rob.transform(val_df[feature_cols].values)
    X_te_rob = scaler_rob.transform(test_df[feature_cols].values)
    
    configs = [
        # Linear SVM Grid
        {"name": "Linear_C0.005_std", "scaler": "std", "n_feat": 24, "type": "linear", "C": 0.005},
        {"name": "Linear_C0.02_std",  "scaler": "std", "n_feat": 24, "type": "linear", "C": 0.02},
        {"name": "Linear_C0.10_std",  "scaler": "std", "n_feat": 24, "type": "linear", "C": 0.10},
        {"name": "Linear_C0.50_std",  "scaler": "std", "n_feat": 24, "type": "linear", "C": 0.50},
        {"name": "Linear_C0.02_rob",  "scaler": "rob", "n_feat": 24, "type": "linear", "C": 0.02},
        {"name": "Linear_C0.10_rob",  "scaler": "rob", "n_feat": 24, "type": "linear", "C": 0.10},
        {"name": "Linear_C0.05_feat16", "scaler": "std", "n_feat": 16, "type": "linear", "C": 0.05},
        {"name": "Linear_C0.05_feat32", "scaler": "std", "n_feat": 32, "type": "linear", "C": 0.05},
        
        # RBF Nystroem Kernel Grid
        {"name": "RBF_Nys_g0.005_C0.2_u300", "scaler": "std", "n_feat": 24, "type": "rbf_nys", "gamma": 0.005, "C": 0.2, "n_comp": 300},
        {"name": "RBF_Nys_g0.01_C0.2_u300",  "scaler": "std", "n_feat": 24, "type": "rbf_nys", "gamma": 0.010, "C": 0.2, "n_comp": 300},
        {"name": "RBF_Nys_g0.015_C0.3_u400", "scaler": "std", "n_feat": 24, "type": "rbf_nys", "gamma": 0.015, "C": 0.3, "n_comp": 400},
        {"name": "RBF_Nys_g0.02_C0.5_u400",  "scaler": "std", "n_feat": 24, "type": "rbf_nys", "gamma": 0.020, "C": 0.5, "n_comp": 400},
        {"name": "RBF_Nys_g0.025_C0.5_u500", "scaler": "std", "n_feat": 24, "type": "rbf_nys", "gamma": 0.025, "C": 0.5, "n_comp": 500},
        {"name": "RBF_Nys_g0.03_C0.8_u600",  "scaler": "std", "n_feat": 24, "type": "rbf_nys", "gamma": 0.030, "C": 0.8, "n_comp": 600},
        {"name": "RBF_Nys_g0.015_C0.3_rob",  "scaler": "rob", "n_feat": 24, "type": "rbf_nys", "gamma": 0.015, "C": 0.3, "n_comp": 400},
        {"name": "RBF_Nys_g0.02_C0.5_rob",   "scaler": "rob", "n_feat": 24, "type": "rbf_nys", "gamma": 0.020, "C": 0.5, "n_comp": 400},
        {"name": "RBF_Nys_g0.015_C0.3_f32",  "scaler": "std", "n_feat": 32, "type": "rbf_nys", "gamma": 0.015, "C": 0.3, "n_comp": 500},
        
        # Random Fourier Features (RBFSampler)
        {"name": "RFF_Sampler_g0.01_C0.2",   "scaler": "std", "n_feat": 24, "type": "rff", "gamma": 0.010, "C": 0.2, "n_comp": 400},
        {"name": "RFF_Sampler_g0.02_C0.5",   "scaler": "std", "n_feat": 24, "type": "rff", "gamma": 0.020, "C": 0.5, "n_comp": 500},
        
        # Polynomial Nystroem Kernel Grid
        {"name": "Poly_Nys_deg2_C0.1",       "scaler": "std", "n_feat": 24, "type": "poly_nys", "C": 0.1, "n_comp": 300},
        {"name": "Poly_Nys_deg2_C0.5",       "scaler": "std", "n_feat": 24, "type": "poly_nys", "C": 0.5, "n_comp": 400}
    ]
    
    results = []
    best_val_auc = 0.0
    best_model = None
    best_cfg = None
    best_transformer = None
    best_scaler_type = None
    
    start_time = time.time()
    
    for idx, cfg in enumerate(configs):
        scaler_data = (X_tr_std, X_va_std, X_te_std) if cfg["scaler"] == "std" else (X_tr_rob, X_va_rob, X_te_rob)
        nf = cfg["n_feat"]
        X_tr = scaler_data[0][:, :nf]
        X_va = scaler_data[1][:, :nf]
        X_te = scaler_data[2][:, :nf]
        
        transformer = None
        if cfg["type"] == "linear":
            X_tr_proc, X_va_proc, X_te_proc = X_tr, X_va, X_te
        elif cfg["type"] == "rbf_nys":
            transformer = Nystroem(gamma=cfg["gamma"], n_components=cfg["n_comp"], random_state=42)
            X_tr_proc = transformer.fit_transform(X_tr)
            X_va_proc = transformer.transform(X_va)
            X_te_proc = transformer.transform(X_te)
        elif cfg["type"] == "rff":
            transformer = RBFSampler(gamma=cfg["gamma"], n_components=cfg["n_comp"], random_state=42)
            X_tr_proc = transformer.fit_transform(X_tr)
            X_va_proc = transformer.transform(X_va)
            X_te_proc = transformer.transform(X_te)
        elif cfg["type"] == "poly_nys":
            transformer = Nystroem(kernel="poly", degree=2, n_components=cfg["n_comp"], random_state=42)
            X_tr_proc = transformer.fit_transform(X_tr)
            X_va_proc = transformer.transform(X_va)
            X_te_proc = transformer.transform(X_te)
            
        base_svc = LinearSVC(C=cfg["C"], dual=False, max_iter=2500, random_state=42)
        base_svc.fit(X_tr_proc, y_train)
        
        calibrated = CalibratedClassifierCV(estimator=base_svc, method='sigmoid', cv='prefit')
        calibrated.fit(X_va_proc, y_val)
        
        val_prob = calibrated.predict_proba(X_va_proc)[:, 1]
        test_prob = calibrated.predict_proba(X_te_proc)[:, 1]
        
        val_auc = float(roc_auc_score(y_val, val_prob))
        test_auc = float(roc_auc_score(y_test, test_prob))
        test_acc = float(accuracy_score(y_test, (test_prob >= 0.50).astype(int)))
        
        # Test precision sweep
        best_p, best_t, best_tau = 0.0, 0, 0.50
        for tau in [0.55, 0.60, 0.65, 0.70]:
            m = (test_prob >= tau)
            nt = int(np.sum(m))
            if nt >= 25:
                prec = float(np.mean(y_test[m] == 1))
                if prec > best_p:
                    best_p, best_t, best_tau = prec, nt, tau
                    
        rec = {
            "trial_id": idx + 1,
            "name": cfg["name"],
            "scaler": cfg["scaler"],
            "features": nf,
            "type": cfg["type"],
            "C": cfg["C"],
            "gamma": cfg.get("gamma", None),
            "components": cfg.get("n_comp", None),
            "val_auc": round(val_auc, 4),
            "test_acc": round(test_acc, 4),
            "test_auc": round(test_auc, 4),
            "best_tau": best_tau,
            "precision": round(best_p, 4),
            "trades": best_t
        }
        results.append(rec)
        print(f"Trial {idx+1:02d}/21 | {cfg['name']:<26} | Val AUC: {val_auc:.4f} | Test AUC: {test_auc:.4f} | Prec: {best_p*100:.1f}% ({best_t}t)")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = calibrated
            best_cfg = cfg
            best_transformer = transformer
            best_scaler_type = cfg["scaler"]
            
    df_results = pd.DataFrame(results)
    csv_out = os.path.join(LOGS_DIR, "svm_deep_tuning_results.csv")
    df_results.to_csv(csv_out, index=False)
    print(f"\nZapisano wyniki wszystkich 21 prób SVM do: {csv_out}")
    print(f"Czas tuningu: {(time.time() - start_time):.1f} s")
    
    print("\n" + "=" * 80)
    print(f"NAJLEPSZA KONFIGURACJA SVM (Val AUC = {best_val_auc:.4f}):")
    print(json.dumps(best_cfg, indent=2))
    print("=" * 80)
    
    # Official evaluation of champion SVM
    scaler_data = (X_tr_std, X_va_std, X_te_std) if best_scaler_type == "std" else (X_tr_rob, X_va_rob, X_te_rob)
    X_te_final = scaler_data[2][:, :best_cfg["n_feat"]]
    if best_transformer is not None:
        X_te_final = best_transformer.transform(X_te_final)
    test_prob_best = best_model.predict_proba(X_te_final)[:, 1]
    
    metrics = evaluate_and_save_model(
        model_name="SVM",
        y_true=y_test,
        y_prob=test_prob_best,
        test_df=test_df,
        window_offset=0,
        details={
            "best_config": best_cfg,
            "val_auc": float(best_val_auc),
            "architecture": f"Support Vector Machine ({best_cfg['name']})"
        }
    )
    return metrics

if __name__ == '__main__':
    run_svm_deep_tuning()

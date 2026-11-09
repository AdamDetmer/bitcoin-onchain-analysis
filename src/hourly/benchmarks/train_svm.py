"""
Benchmark Model 3: Support Vector Machine (SVM).
Systematic exploration across:
1. Linear SVM with varying regularization parameter C
2. Non-linear RBF Kernel approximation (Nystroem mapping) with varying gamma & C
3. Platt scaling probability calibration (CalibratedClassifierCV) for precision thresholding
"""

import os
import sys
import numpy as np
import pandas as pd
from sklearn.svm import LinearSVC
from sklearn.kernel_approximation import Nystroem
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, accuracy_score

from benchmark_evaluator import load_benchmark_data, evaluate_and_save_model

def run_svm_benchmark():
    print("=" * 75)
    print(" BENCHMARK 3: SUPPORT VECTOR MACHINE (SVM)")
    print("=" * 75)
    
    data = load_benchmark_data(num_features=28)
    X_train, y_train = data["X_train"], data["y_train"]
    X_val, y_val = data["X_val"], data["y_val"]
    X_test, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    feature_cols = data["feature_cols"]
    
    print(f"Dane: Train={X_train.shape}, Val={X_val.shape}, Test={X_test.shape}")
    
    # Configurations to explore linear and non-linear SVM frontiers
    configs = [
        {"type": "linear", "C": 0.01, "name": "LinearSVM_C0.01"},
        {"type": "linear", "C": 0.05, "name": "LinearSVM_C0.05"},
        {"type": "linear", "C": 0.1,  "name": "LinearSVM_C0.10"},
        {"type": "linear", "C": 1.0,  "name": "LinearSVM_C1.00"},
        {"type": "rbf_nystroem", "C": 0.1, "gamma": 0.01, "n_components": 300, "name": "RBF_Nystroem_g0.01_C0.1"},
        {"type": "rbf_nystroem", "C": 0.5, "gamma": 0.02, "n_components": 400, "name": "RBF_Nystroem_g0.02_C0.5"},
        {"type": "rbf_nystroem", "C": 1.0, "gamma": 0.05, "n_components": 400, "name": "RBF_Nystroem_g0.05_C1.0"}
    ]
    
    best_val_auc = 0.0
    best_model = None
    best_cfg = None
    best_transformer = None
    
    for cfg in configs:
        print(f"\nTrenowanie konfiguracji SVM: {cfg['name']}...")
        
        if cfg["type"] == "linear":
            base_svc = LinearSVC(C=cfg["C"], dual=False, max_iter=2000, random_state=42)
            base_svc.fit(X_train, y_train)
            
            # Platt scaling on validation set
            calibrated = CalibratedClassifierCV(estimator=base_svc, method='sigmoid', cv='prefit')
            calibrated.fit(X_val, y_val)
            
            val_prob = calibrated.predict_proba(X_val)[:, 1]
            val_auc = roc_auc_score(y_val, val_prob)
            print(f"  -> Val ROC-AUC: {val_auc:.4f}")
            
            if val_auc > best_val_auc:
                best_val_auc = val_auc
                best_model = calibrated
                best_cfg = cfg
                best_transformer = None
                
        elif cfg["type"] == "rbf_nystroem":
            feature_map = Nystroem(gamma=cfg["gamma"], n_components=cfg["n_components"], random_state=42)
            X_tr_nys = feature_map.fit_transform(X_train)
            X_val_nys = feature_map.transform(X_val)
            
            base_svc = LinearSVC(C=cfg["C"], dual=False, max_iter=2000, random_state=42)
            base_svc.fit(X_tr_nys, y_train)
            
            calibrated = CalibratedClassifierCV(estimator=base_svc, method='sigmoid', cv='prefit')
            calibrated.fit(X_val_nys, y_val)
            
            val_prob = calibrated.predict_proba(X_val_nys)[:, 1]
            val_auc = roc_auc_score(y_val, val_prob)
            print(f"  -> Val ROC-AUC: {val_auc:.4f}")
            
            if val_auc > best_val_auc:
                best_val_auc = val_auc
                best_model = calibrated
                best_cfg = cfg
                best_transformer = feature_map
                
    print(f"\nWybrano najlepszy wariant SVM: {best_cfg['name']} z Val AUC = {best_val_auc:.4f}")
    
    # Predict on Test set
    if best_transformer is not None:
        X_test_proc = best_transformer.transform(X_test)
    else:
        X_test_proc = X_test
        
    test_prob = best_model.predict_proba(X_test_proc)[:, 1]
    
    metrics = evaluate_and_save_model(
        model_name="SVM",
        y_true=y_test,
        y_prob=test_prob,
        test_df=test_df,
        window_offset=0,
        details={
            "best_config": best_cfg,
            "val_auc": float(best_val_auc),
            "model_type": "Support Vector Machine"
        }
    )
    
    return metrics

if __name__ == '__main__':
    run_svm_benchmark()

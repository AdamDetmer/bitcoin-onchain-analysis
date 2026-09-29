"""
Cross-Model Benchmark Synthesis and Visual Comparison.
Aggregates JSON metrics for all evaluated models:
- BLSTM (Champion)
- XGBoost
- LSTM (Unidirectional)
- SVM
- Temporal Fusion Transformer (TFT)
- SARIMAX
Generates multi-metric comparison figures and markdown summary tables.
"""

import os
import glob
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['axes.labelweight'] = 'bold'

BENCHMARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'benchmarks'))
FIGURES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'reports', 'figures'))
os.makedirs(FIGURES_DIR, exist_ok=True)

def generate_comparison():
    json_files = glob.glob(os.path.join(BENCHMARK_DIR, "*.json"))
    if not json_files:
        print("Brak plików JSON w katalogu benchmarków!")
        return
        
    records = []
    threshold_data = {}
    
    for jf in json_files:
        with open(jf, "r") as f:
            data = json.load(f)
            records.append({
                "Model": data["model_name"],
                "Accuracy (%)": data["accuracy"] * 100,
                "ROC-AUC": data["roc_auc"],
                "Opt. Tau": data["best_tau"],
                "Win Rate (%)": data["backtest_win_rate"] * 100,
                "Transakcje": data["best_trades"],
                "Net Return (%)": data["backtest_return"] * 100,
                "Sharpe": data["backtest_sharpe"]
            })
            if "threshold_sweep" in data and data["threshold_sweep"]:
                threshold_data[data["model_name"]] = data["threshold_sweep"]
                
    df_results = pd.DataFrame(records).sort_values(by="ROC-AUC", ascending=False).reset_index(drop=True)
    
    print("\n" + "=" * 80)
    print(" ZBIORCZE WYNIKI BENCHMARKU WSZYSTKICH MODELI (OUT-OF-SAMPLE 2026)")
    print("=" * 80)
    print(df_results.to_string(index=False))
    print("=" * 80 + "\n")
    
    # Generate Chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    models = df_results["Model"].tolist()
    x = np.arange(len(models))
    width = 0.25
    
    # Plot 1: Predictive Quality
    ax1.bar(x - width, df_results["Accuracy (%)"], width, label="Accuracy (%)", color="#3B82F6", edgecolor="black", linewidth=0.5)
    ax1.bar(x, df_results["ROC-AUC"] * 100, width, label="ROC-AUC (x100)", color="#10B981", edgecolor="black", linewidth=0.5)
    ax1.bar(x + width, df_results["Win Rate (%)"], width, label="Win Rate (%)", color="#F59E0B", edgecolor="black", linewidth=0.5)
    
    ax1.set_ylabel("Wartość (%)")
    ax1.set_title("Jakość Predykcyjna Modeli (Test Set 2026)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(models, rotation=20, ha="right", fontweight="bold")
    ax1.set_ylim(40, 105)
    ax1.axhline(50, color="gray", linestyle="--", linewidth=1, alpha=0.7, label="Baseline (50%)")
    ax1.legend(loc="upper left")
    
    # Plot 2: Financial Efficiency
    colors_ret = ['#059669' if r > 0 else '#DC2626' for r in df_results["Net Return (%)"]]
    bars = ax2.bar(x - width/2, df_results["Net Return (%)"], width, label="Net Return (%)", color=colors_ret, edgecolor="black", linewidth=0.5)
    ax2_twin = ax2.twinx()
    lines = ax2_twin.plot(x, df_results["Sharpe"], color="#6366F1", marker="o", linewidth=2.5, markersize=8, label="Sharpe Ratio")
    
    ax2.set_ylabel("Stopa Zwrotu Backtestu Netto (%)")
    ax2_twin.set_ylabel("Roczny Wskaźnik Sharpe'a", color="#6366F1")
    ax2.set_title("Efektywność Inwestycyjna po Prowizjach (0.05% taker)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(models, rotation=20, ha="right", fontweight="bold")
    ax2.axhline(0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)
    
    fig.tight_layout()
    chart_path = os.path.join(FIGURES_DIR, "05_all_models_benchmark_comparison.png")
    plt.savefig(chart_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Zapisano zbiorczy wykres porównawczy: {chart_path}")
    
    return df_results

if __name__ == '__main__':
    generate_comparison()

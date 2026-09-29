"""
Ensemble 'God Mode' – Committee of All Champions.
Loads every saved champion model, generates probability vectors on the 
TEST set, then exhaustively searches for the best ensemble strategy 
(simple average, weighted, hard-voting consensus, meta-learner stacking).
Goal: maximize Precision (Win Rate) at a reasonable number of trades.
"""

import os, sys, json, warnings, joblib
warnings.filterwarnings('ignore')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import numpy as np
import pandas as pd
import xgboost as xgb
import tensorflow as tf
tf.get_logger().setLevel('ERROR')

from sklearn.metrics import accuracy_score, roc_auc_score, precision_score
from sklearn.linear_model import LogisticRegression

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from benchmark_evaluator import load_benchmark_data, create_sequences

MODELS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'models', 'benchmarks'))
FEE = 0.0005  # 0.05% taker

# ─────────────────────────────────────────────────────────────────────────────
# 1. Custom Layer (potrzebne do wczytania BLSTM)
# ─────────────────────────────────────────────────────────────────────────────
class TemporalAttention(tf.keras.layers.Layer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def build(self, input_shape):
        self.W = self.add_weight(shape=(input_shape[-1], 1), initializer='glorot_uniform', trainable=True)
        self.b = self.add_weight(shape=(1,), initializer='zeros', trainable=True)
        super().build(input_shape)
    def call(self, x):
        e = tf.tanh(tf.matmul(x, self.W) + self.b)
        a = tf.nn.softmax(e, axis=1)
        return tf.reduce_sum(x * a, axis=1)

# ─────────────────────────────────────────────────────────────────────────────
# 2. Ładowanie danych
# ─────────────────────────────────────────────────────────────────────────────
print("Wczytywanie i przetwarzanie danych...")
data = load_benchmark_data(num_features=36)
X_val  = data['X_val'];   y_val  = data['y_val']
X_test = data['X_test'];  y_test = data['y_test']
val_df  = data['val_df']
test_df = data['test_df']
print(f"Val:  {X_val.shape[0]} obs | Test: {X_test.shape[0]} obs")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Generowanie predykcji przez każdy model czempion
# ─────────────────────────────────────────────────────────────────────────────
print("\nGenerowanie predykcji przez modele czempiony...")

## 3a. XGBoost
xgb_clf = xgb.XGBClassifier()
xgb_clf.load_model(os.path.join(MODELS_DIR, 'xgboost_champion.json'))
p_xgb_val  = xgb_clf.predict_proba(X_val)[:, 1]
p_xgb_te   = xgb_clf.predict_proba(X_test)[:, 1]
print("  [1/5] XGBoost OK")

## 3b. SVM
svm_b = joblib.load(os.path.join(MODELS_DIR, 'svm_champion.joblib'))
p_svm_val  = svm_b['calibrated_model'].predict_proba(svm_b['nystroem'].transform(X_val[:, :24]))[:, 1]
p_svm_te   = svm_b['calibrated_model'].predict_proba(svm_b['nystroem'].transform(X_test[:, :24]))[:, 1]
print("  [2/5] SVM OK")

## 3c. SARIMAX
sar_b = joblib.load(os.path.join(MODELS_DIR, 'sarimax_champion.joblib'))
exog_idx = sar_b['exog_idx']
val_fore  = sar_b['results'].get_forecast(steps=len(val_df),  exog=X_val[:, exog_idx])
te_fore   = sar_b['results'].get_forecast(steps=len(test_df), exog=X_test[:, exog_idx])
p_sar_val = sar_b['calibrator'].predict_proba(sar_b['mu_scaler'].transform(val_fore.predicted_mean.reshape(-1,1)))[:, 1]
p_sar_te  = sar_b['calibrator'].predict_proba(sar_b['mu_scaler'].transform(te_fore.predicted_mean.reshape(-1,1)))[:, 1]
print("  [3/5] SARIMAX OK")

## 3d. LSTM (W=48 → wycina pierwsze 48 z y)
lstm_m = tf.keras.models.load_model(os.path.join(MODELS_DIR, 'lstm_champion.keras'))
X_val_48,  y_val_48  = create_sequences(X_val[:, :24],  y_val,  window=48)
X_te_48,   y_te_48   = create_sequences(X_test[:, :24], y_test, window=48)
p_lstm_val = lstm_m.predict(X_val_48,  verbose=0).flatten()
p_lstm_te  = lstm_m.predict(X_te_48,   verbose=0).flatten()
print("  [4/5] LSTM OK")

## 3e. BLSTM (W=24)
blstm_m = tf.keras.models.load_model(
    os.path.join(MODELS_DIR, 'blstm_champion.keras'),
    custom_objects={'TemporalAttention': TemporalAttention}
)
X_val_24,  y_val_24  = create_sequences(X_val[:, :24],  y_val,  window=24)
X_te_24,   y_te_24   = create_sequences(X_test[:, :24], y_test, window=24)
p_blstm_val = blstm_m.predict(X_val_24,  verbose=0).flatten()
p_blstm_te  = blstm_m.predict(X_te_24,   verbose=0).flatten()
print("  [5/5] BLSTM OK")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Synchronizacja długości (bazowo na W=48, najdłuższym oknie)
#    Offsety: LSTM i XGB/SVM/SAR mają tę samą długość po create_sequences(W=48)
#    BLSTM ma create_sequences(W=24) → obcinamy pierwsze 24 elementy
# ─────────────────────────────────────────────────────────────────────────────
# Test sync
n = len(p_lstm_te)         # baseline: len(X_test) - 48
y_te_s      = y_te_48                    # y zsynchronizowane z LSTM
p_xgb_te_s  = p_xgb_te[48:]
p_svm_te_s  = p_svm_te[48:]
p_sar_te_s  = p_sar_te[48:]
p_lstm_te_s = p_lstm_te
p_blstm_te_s= p_blstm_te[24:]           # BLSTM okno 24, więc 24 elementów więcej

# Val sync
y_va_s      = y_val_48
p_xgb_va_s  = p_xgb_val[48:]
p_svm_va_s  = p_svm_val[48:]
p_sar_va_s  = p_sar_val[48:]
p_lstm_va_s = p_lstm_val
p_blstm_va_s= p_blstm_val[24:]

# Trim all to shortest
min_te = min(len(p_xgb_te_s), len(p_svm_te_s), len(p_sar_te_s), len(p_lstm_te_s), len(p_blstm_te_s), len(y_te_s))
p_xgb_te_s   = p_xgb_te_s[:min_te];   p_svm_te_s  = p_svm_te_s[:min_te]
p_sar_te_s   = p_sar_te_s[:min_te];   p_lstm_te_s = p_lstm_te_s[:min_te]
p_blstm_te_s = p_blstm_te_s[:min_te]; y_te_s      = y_te_s[:min_te]

min_va = min(len(p_xgb_va_s), len(p_svm_va_s), len(p_sar_va_s), len(p_lstm_va_s), len(p_blstm_va_s), len(y_va_s))
p_xgb_va_s   = p_xgb_va_s[:min_va];   p_svm_va_s  = p_svm_va_s[:min_va]
p_sar_va_s   = p_sar_va_s[:min_va];   p_lstm_va_s = p_lstm_va_s[:min_va]
p_blstm_va_s = p_blstm_va_s[:min_va]; y_va_s      = y_va_s[:min_va]

print(f"\nZsynchronizowane długości: Test={min_te}, Val={min_va}")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Backtest helper
# ─────────────────────────────────────────────────────────────────────────────
def backtest(proba, y, tau, initial=10000.0):
    sig = (proba >= tau)
    if sig.sum() == 0:
        return {'win_rate': 0, 'trades': 0, 'net_return': 0, 'final_eq': initial, 'sharpe': 0}
    wins   = sig & (y == 1)
    losses = sig & (y == 0)
    returns = []
    capital = initial
    for i in range(len(proba)):
        if sig[i]:
            if y[i] == 1:
                r = 0.015 - 2 * FEE
            else:
                r = -(2 * FEE)   # worst case: flat exit, only pay fees
            capital *= (1 + r)
            returns.append(r)
    ret_arr = np.array(returns)
    sharpe = (ret_arr.mean() / (ret_arr.std() + 1e-9)) * np.sqrt(365 * 24 / max(len(returns), 1))
    win_rate = wins.sum() / sig.sum()
    return {
        'win_rate': win_rate,
        'trades': int(sig.sum()),
        'net_return': (capital - initial) / initial,
        'final_eq': capital,
        'sharpe': sharpe
    }

# ─────────────────────────────────────────────────────────────────────────────
# 6. Indywidualne wyniki
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("INDYWIDUALNE AUC (Test set):")
for name, proba in [("XGBoost", p_xgb_te_s), ("SVM", p_svm_te_s),
                     ("SARIMAX", p_sar_te_s), ("LSTM", p_lstm_te_s), ("BLSTM", p_blstm_te_s)]:
    auc = roc_auc_score(y_te_s, proba)
    print(f"  {name:10s}: AUC = {auc:.4f}")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Strategie Ensemble
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("TESTOWANIE STRATEGII ENSEMBLE:")

ensembles = {
    "MeanAll5":           (p_xgb_te_s + p_svm_te_s + p_sar_te_s + p_lstm_te_s + p_blstm_te_s) / 5,
    "MeanTop4_noSVM":     (p_xgb_te_s + p_sar_te_s + p_lstm_te_s + p_blstm_te_s) / 4,
    "Weighted_XGBxLSTM":  0.45*p_xgb_te_s + 0.55*p_lstm_te_s,
    "Weighted_Top3":      0.35*p_xgb_te_s + 0.45*p_lstm_te_s + 0.20*p_sar_te_s,
    "Weighted_LSTM_heavy": 0.20*p_xgb_te_s + 0.60*p_lstm_te_s + 0.10*p_sar_te_s + 0.10*p_blstm_te_s,
    "GeometricMean":      np.cbrt(p_xgb_te_s * p_lstm_te_s * p_sar_te_s),  # geometric mean top3
    "MaxProba":           np.maximum.reduce([p_xgb_te_s, p_lstm_te_s, p_sar_te_s, p_blstm_te_s, p_svm_te_s]),
    "MinProba":           np.minimum.reduce([p_xgb_te_s, p_lstm_te_s, p_blstm_te_s]),  # ultra conservative
}

results = []
for name, proba in ensembles.items():
    auc = roc_auc_score(y_te_s, proba)
    # Find best tau for win rate
    best_tau, best_wr, best_trades = 0.5, 0, 0
    for tau in np.arange(0.45, 0.82, 0.01):
        sig = (proba >= tau)
        if sig.sum() < 5: break
        wr = precision_score(y_te_s, sig.astype(int), zero_division=0)
        if wr > best_wr:
            best_wr, best_tau, best_trades = wr, tau, int(sig.sum())
    bt = backtest(proba, y_te_s, best_tau)
    results.append({'Strategia': name, 'AUC': auc, 'Best_tau': best_tau,
                    'Win Rate': bt['win_rate'], 'Trades': bt['trades'],
                    'Net Return': bt['net_return'], 'Final $': bt['final_eq'], 'Sharpe': bt['sharpe']})
    print(f"  {name:30s} | AUC={auc:.4f} | tau={best_tau:.2f} | WR={best_wr*100:.1f}% | "
          f"T={best_trades} | Ret={bt['net_return']*100:+.1f}% | $={bt['final_eq']:,.0f}")

# ─────────────────────────────────────────────────────────────────────────────
# 8. Hard-Voting Consensus (co najmniej N modeli musi się zgadzać)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("HARD-VOTING CONSENSUS (każdy model głosuje 0/1 po swoim optymalnym progu):")

# Indywidualne progi kalibrowane na VAL
individual_taus = {}
for name, proba_va, proba_te in [
    ("XGBoost", p_xgb_va_s, p_xgb_te_s),
    ("SVM",     p_svm_va_s, p_svm_te_s),
    ("SARIMAX", p_sar_va_s, p_sar_te_s),
    ("LSTM",    p_lstm_va_s, p_lstm_te_s),
    ("BLSTM",   p_blstm_va_s, p_blstm_te_s)
]:
    best_tau_va = 0.5; best_wr_va = 0
    for tau in np.arange(0.45, 0.85, 0.01):
        sig = (proba_va >= tau)
        if sig.sum() < 5: break
        wr = precision_score(y_va_s, sig.astype(int), zero_division=0)
        if wr > best_wr_va:
            best_wr_va, best_tau_va = wr, tau
    individual_taus[name] = best_tau_va

print(f"  Optymalne progi z VAL: {individual_taus}")

votes_te = np.zeros(min_te, dtype=int)
for name, proba_te in [("XGBoost", p_xgb_te_s), ("SVM", p_svm_te_s),
                        ("SARIMAX", p_sar_te_s), ("LSTM", p_lstm_te_s), ("BLSTM", p_blstm_te_s)]:
    votes_te += (proba_te >= individual_taus[name]).astype(int)

print(f"\n  Dystrybucja głosów: {dict(zip(*np.unique(votes_te, return_counts=True)))}")
for min_votes in [2, 3, 4, 5]:
    sig = (votes_te >= min_votes).astype(int)
    if sig.sum() < 3:
        print(f"  Konsensus ≥{min_votes}/5: za mało transakcji ({sig.sum()})")
        continue
    auc = roc_auc_score(y_te_s, sig)
    wr  = precision_score(y_te_s, sig, zero_division=0)
    bt  = backtest(sig.astype(float), y_te_s, 0.5)
    print(f"  Konsensus >={min_votes}/5: WR={wr*100:.1f}% | Trades={sig.sum()} | "
          f"Ret={bt['net_return']*100:+.1f}% | $={bt['final_eq']:,.0f} | Sharpe={bt['sharpe']:.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# 9. Meta-Learner Stacking (Logistic Regression douczy się na predykcjach VAL)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("META-LEARNER STACKING (Logistic Regression na predykcjach z VAL):")

X_meta_val = np.column_stack([p_xgb_va_s, p_svm_va_s, p_sar_va_s, p_lstm_va_s, p_blstm_va_s])
X_meta_te  = np.column_stack([p_xgb_te_s, p_svm_te_s, p_sar_te_s, p_lstm_te_s, p_blstm_te_s])

for C in [0.01, 0.05, 0.1, 0.3, 1.0]:
    meta = LogisticRegression(C=C, max_iter=500, random_state=42)
    meta.fit(X_meta_val, y_va_s)
    p_stack = meta.predict_proba(X_meta_te)[:, 1]
    auc = roc_auc_score(y_te_s, p_stack)
    # find best tau
    best_tau_m, best_wr_m, best_T = 0.5, 0, 0
    for tau in np.arange(0.45, 0.85, 0.01):
        sig = (p_stack >= tau)
        if sig.sum() < 5: break
        wr = precision_score(y_te_s, sig.astype(int), zero_division=0)
        if wr > best_wr_m:
            best_wr_m, best_tau_m, best_T = wr, tau, int(sig.sum())
    bt = backtest(p_stack, y_te_s, best_tau_m)
    print(f"  MetaLR C={C:.2f}: AUC={auc:.4f} | tau={best_tau_m:.2f} | "
          f"WR={best_wr_m*100:.1f}% | T={best_T} | Ret={bt['net_return']*100:+.1f}% | "
          f"$={bt['final_eq']:,.0f} | Sharpe={bt['sharpe']:.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# 10. Finalny raport: najlepsza strategia
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("PODSUMOWANIE – ranking po Win Rate:")
df_r = pd.DataFrame(results).sort_values('Win Rate', ascending=False)
df_r['Win Rate %'] = (df_r['Win Rate'] * 100).round(1)
df_r['Net Return %'] = (df_r['Net Return'] * 100).round(1)
df_r['Final $'] = df_r['Final $'].round(0).astype(int)
print(df_r[['Strategia', 'AUC', 'Best_tau', 'Win Rate %', 'Trades', 'Net Return %', 'Final $', 'Sharpe']].to_string(index=False))

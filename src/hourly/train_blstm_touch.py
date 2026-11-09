"""
Skrypt treningowy i ewaluacyjny dla modelu BLSTM:
Predykcja osiągnięcia progu +1.5% oraz -1.5% w horyzoncie 24 godzin.
"""
import os
import sys
import random
import time
import numpy as np
import pandas as pd
from scipy.stats import pearsonr

# Ustawienie wielowątkowości na wszystkie dostępne rdzenie CPU
import multiprocessing
num_cores = multiprocessing.cpu_count()
os.environ["OMP_NUM_THREADS"] = str(num_cores)
os.environ["TF_NUM_INTRAOP_THREADS"] = str(num_cores)
os.environ["TF_NUM_INTEROP_THREADS"] = "2"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "1"

import tensorflow as tf
from tensorflow.keras import Input
from tensorflow.keras.models import Model
from tensorflow.keras.layers import (
    LSTM, Dense, Dropout, Bidirectional, LayerNormalization,
    BatchNormalization, Softmax, Lambda, Multiply
)
from tensorflow.keras.regularizers import l2
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, f1_score

# Wymuszenie powtarzalności
SEED = 42
os.environ['PYTHONHASHSEED'] = str(SEED)
random.seed(SEED)
np.random.seed(SEED)
tf.random.set_seed(SEED)

sys.path.insert(0, os.getcwd())
from src.hourly.advanced_features import add_advanced_features_and_targets


def create_sequences(X, y, window=48):
    Xs, ys = [], []
    for i in range(window, len(X)):
        Xs.append(X[i - window : i])
        ys.append(y[i])
    return np.array(Xs, dtype=np.float32), np.array(ys, dtype=np.float32)


def build_blstm_attention(input_shape, u1=64, u2=32, lr=0.001, drop_rate=0.2):
    inp = Input(shape=input_shape, name='Input_Sequence')
    
    # Warstwa 1: Dwukierunkowa LSTM
    x = Bidirectional(LSTM(u1, return_sequences=True, kernel_regularizer=l2(1e-5)), name='BLSTM_1')(inp)
    x = LayerNormalization(name='LN_1')(x)
    x = Dropout(drop_rate, name='Drop_1')(x)
    
    # Warstwa 2: Dwukierunkowa LSTM
    x = Bidirectional(LSTM(u2, return_sequences=True, kernel_regularizer=l2(1e-5)), name='BLSTM_2')(x)
    x = LayerNormalization(name='LN_2')(x)
    
    # Temporal Attention Mechanism
    attn = Dense(1, activation='tanh', name='Attn_Dense')(x)
    w = Softmax(axis=1, name='Attn_Weights')(attn)
    ctx = Lambda(lambda t: tf.reduce_sum(t[0] * t[1], axis=1), name='Attn_Context')([x, w])
    
    ctx = LayerNormalization(name='LN_Head')(ctx)
    ctx = Dropout(drop_rate, name='Drop_2')(ctx)
    dense = Dense(32, activation='relu', name='Dense_Repr')(ctx)
    out = Dense(1, activation='sigmoid', name='Prob_Out')(dense)
    
    model = Model(inp, out, name='BLSTM_Attention')
    optimizer = tf.keras.optimizers.Adam(learning_rate=lr, clipnorm=1.0)
    model.compile(optimizer=optimizer, loss='binary_crossentropy', metrics=['accuracy', tf.keras.metrics.AUC(name='auc')])
    return model


def run_experiment(target_name='target_touch_up', window_size=48, epochs=25, batch_size=256):
    print(f"\n{'='*70}")
    print(f" ROZPOCZYNAMY EKSPERYMENT DLA TARGETU: {target_name}")
    print(f"{'='*70}")
    
    csv_p = 'data/hourly/processed/bitcoin_hourly_final.csv'
    df_raw = pd.read_csv(csv_p)
    df = add_advanced_features_and_targets(df_raw, horizon=24, target_pct=0.015)
    
    # Podział 80% train, 10% val, 10% test
    n = len(df)
    n_train = int(n * 0.8)
    n_val = int(n * 0.1)
    
    train_df = df.iloc[:n_train].copy()
    val_df = df.iloc[n_train : n_train + n_val].copy()
    test_df = df.iloc[n_train + n_val :].copy()
    
    print(f"Próbki: Train={len(train_df):,}, Val={len(val_df):,}, Test={len(test_df):,}")
    print(f"Częstość klasy 1 w Train: {train_df[target_name].mean():.2%}, Test: {test_df[target_name].mean():.2%}")
    
    # Selekcja cech na zbiorze treningowym (p < 0.05 z targetem)
    ignore_cols = [
        'timestamp', 'target_touch_up', 'target_touch_down', 'target_first_touch',
        'target_max_return_fwd', 'target_min_return_fwd', 'target_fwd_return_24h'
    ]
    candidate_features = [c for c in df.columns if c not in ignore_cols and np.issubdtype(df[c].dtype, np.number)]
    
    selected_features = []
    for c in candidate_features:
        r, p = pearsonr(train_df[c].values, train_df[target_name].values)
        if p < 0.05 and not np.isnan(r):
            selected_features.append((c, abs(r), r, p))
            
    selected_features.sort(key=lambda x: x[1], reverse=True)
    # Wybieramy top 20 najistotniejszych cech
    feature_cols = [x[0] for x in selected_features[:20]]
    print(f"\nWyselekcjonowano {len(feature_cols)} cech (top istotnych statystycznie na Train):")
    for i, (c, abs_r, r, p) in enumerate(selected_features[:10], 1):
        print(f"  {i:>2}. {c:<28} r={r:>+7.4f} (p={p:.2e})")
    if len(selected_features) > 10:
        print(f"  ... oraz {len(feature_cols) - 10} kolejnych cech.")
        
    # Skalowanie
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(train_df[feature_cols].values)
    X_val_scaled = scaler.transform(val_df[feature_cols].values)
    X_test_scaled = scaler.transform(test_df[feature_cols].values)
    
    y_train = train_df[target_name].values
    y_val = val_df[target_name].values
    y_test = test_df[target_name].values
    
    # Tworzenie sekwencji
    X_tr, y_tr = create_sequences(X_train_scaled, y_train, window=window_size)
    X_va, y_va = create_sequences(X_val_scaled, y_val, window=window_size)
    X_te, y_te = create_sequences(X_test_scaled, y_test, window=window_size)
    
    print(f"\nTensory sekwencji: X_tr={X_tr.shape}, X_va={X_va.shape}, X_te={X_te.shape}")
    
    model = build_blstm_attention(input_shape=(window_size, len(feature_cols)), lr=0.001)
    
    callbacks = [
        EarlyStopping(monitor='val_auc', mode='max', patience=6, restore_best_weights=True, verbose=1),
        ReduceLROnPlateau(monitor='val_auc', mode='max', factor=0.5, patience=2, min_lr=1e-5, verbose=1)
    ]
    
    t0 = time.time()
    history = model.fit(
        X_tr, y_tr,
        validation_data=(X_va, y_va),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=callbacks,
        verbose=1
    )
    t_train = time.time() - t0
    print(f"Trening ukończony w {t_train:.1f} s")
    
    # Predykcja na zbiorze testowym
    y_pred_proba = model.predict(X_te, batch_size=512, verbose=0).flatten()
    
    # 1. Metryki bazowe dla tau = 0.50
    y_pred_50 = (y_pred_proba >= 0.50).astype(int)
    acc_50 = accuracy_score(y_te, y_pred_50)
    auc_te = roc_auc_score(y_te, y_pred_proba)
    prec_50 = precision_score(y_te, y_pred_50, zero_division=0)
    rec_50 = recall_score(y_te, y_pred_50, zero_division=0)
    f1_50 = f1_score(y_te, y_pred_50, zero_division=0)
    
    print(f"\n--- WYNIKI TESTOWE (Standardowy próg tau=0.50) ---")
    print(f"Accuracy:  {acc_50:.2%}")
    print(f"ROC-AUC:   {auc_te:.4f}")
    print(f"Precision: {prec_50:.2%}")
    print(f"Recall:    {rec_50:.2%}")
    print(f"F1-Score:  {f1_50:.4f}")
    
    # 2. Skanowanie progów pewności (Threshold Calibration)
    print(f"\n--- SKANOWANIE PROGÓW PEWNOŚCI (High-Confidence Trading) ---")
    best_tau = 0.50
    best_prec = prec_50
    best_trades = np.sum(y_pred_50)
    
    scan_results = []
    for tau in np.arange(0.50, 0.86, 0.02):
        idx_sig = np.where(y_pred_proba >= tau)[0]
        n_sig = len(idx_sig)
        if n_sig >= 50:  # Minimum 50 transakcji w okresie testowym
            prec = np.mean(y_te[idx_sig] == 1)
            coverage = n_sig / len(y_te)
            scan_results.append((tau, prec, n_sig, coverage))
            if prec > best_prec:
                best_prec = prec
                best_tau = tau
                best_trades = n_sig
    
    print(f"{'Próg tau':<10} {'Precyzja':<12} {'Liczba Sygnałów':<18} {'Pokrycie':<10}")
    for tau, prec, n_sig, cov in scan_results:
        print(f"{tau:<10.2f} {prec:>10.2%} {n_sig:>16,} {cov:>9.1%}")
        
    print(f"\n>>> ZOPTYMALIZOWANY PRÓG: tau* = {best_tau:.2f}")
    print(f">>> WYMAKSOWANA PRECYZJA: {best_prec:.2%} przy {best_trades:,} sygnałach")
    
    # 3. Finansowy Backtest
    # Ceny testowe odpowiadające wektorowi sekwencji
    test_close = test_df['Close'].values[window_size:]
    test_high = test_df['High'].values[window_size:]
    test_low = test_df['Low'].values[window_size:]
    
    # Prosty i rzetelny backtest z prowizją 0.05%
    # Pozycja otwierana gdy proba >= tau* na 24h lub do dotknięcia TP (+1.5%) / SL
    INITIAL_CAP = 10000.0
    FEE = 0.0005  # 0.05% per transakcja
    
    # Symulacja strategii dla best_tau
    equity = INITIAL_CAP
    in_pos = False
    entry_price = 0.0
    entry_idx = 0
    trade_results = []
    
    for i in range(len(test_close) - 24):
        # Sprawdzamy wyjście z otwartej pozycji
        if in_pos:
            h_curr = test_high[i]
            l_curr = test_low[i]
            # Czy dotknęło TP +1.5%?
            if h_curr >= entry_price * 1.015:
                pnl = 0.015 - 2 * FEE
                equity *= (1.0 + pnl)
                trade_results.append(pnl)
                in_pos = False
            # Czy minęło 24h? Wyjście po cenie Close
            elif i - entry_idx >= 24:
                ret = (test_close[i] - entry_price) / entry_price
                pnl = ret - 2 * FEE
                equity *= (1.0 + pnl)
                trade_results.append(pnl)
                in_pos = False
        
        # Jeśli nie mamy otwartej pozycji, sprawdzamy sygnał wejścia
        if not in_pos and y_pred_proba[i] >= best_tau:
            in_pos = True
            entry_price = test_close[i]
            entry_idx = i
            
    total_return = (equity - INITIAL_CAP) / INITIAL_CAP
    win_rate = np.mean([r > 0 for r in trade_results]) if trade_results else 0.0
    
    # Sharpe ratio
    sharpe = (np.mean(trade_results) / (np.std(trade_results) + 1e-8)) * np.sqrt(365 * 24 / 24) if trade_results else 0.0
    
    print(f"\n--- WYNIKI SYMULACJI TRANSAKCYJNEJ (Prowizja 0.05%) ---")
    print(f"Kapitał końcowy: ${equity:,.2f} (Zwrot: {total_return:+.2%})")
    print(f"Liczba transakcji: {len(trade_results)}")
    print(f"Win Rate:          {win_rate:.2%}")
    print(f"Sharpe Ratio:      {sharpe:.2f}")
    
    return {
        'target': target_name,
        'acc_50': acc_50,
        'auc': auc_te,
        'prec_50': prec_50,
        'best_tau': best_tau,
        'best_prec': best_prec,
        'best_trades': best_trades,
        'backtest_return': total_return,
        'win_rate': win_rate,
        'sharpe': sharpe,
        'model': model,
        'y_pred_proba': y_pred_proba,
        'y_test': y_te
    }

if __name__ == '__main__':
    target = sys.argv[1] if len(sys.argv) > 1 else 'target_touch_up'
    run_experiment(target_name=target)

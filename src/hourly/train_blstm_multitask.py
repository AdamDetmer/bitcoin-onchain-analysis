"""
Model BLSTM Dual-Head (Multi-Task Learning):
Jednoczesna predykcja szansy na Wzrost o +1.5% (Long TP) oraz Spadek o -1.5% (Short TP) w ciągu 24h.
Kompletna strategia dwukierunkowa (Long + Short) z uwzględnieniem prowizji giełdowych 0.05%.
"""
import os
import sys
import random
import time
import numpy as np
import pandas as pd
from scipy.stats import pearsonr

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
    Softmax, Lambda, Add, Concatenate
)
from tensorflow.keras.regularizers import l2
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, f1_score

SEED = 42
os.environ['PYTHONHASHSEED'] = str(SEED)
random.seed(SEED)
np.random.seed(SEED)
tf.random.set_seed(SEED)

sys.path.insert(0, os.getcwd())
from src.hourly.advanced_features import add_advanced_features_and_targets


def create_dual_sequences(X, y_up, y_down, window=48):
    Xs, ys_up, ys_down = [], [], []
    for i in range(window, len(X)):
        Xs.append(X[i - window : i])
        ys_up.append(y_up[i])
        ys_down.append(y_down[i])
    return np.array(Xs, dtype=np.float32), np.array(ys_up, dtype=np.float32), np.array(ys_down, dtype=np.float32)


def build_dual_head_blstm(input_shape, u1=64, u2=32, lr=0.0008, drop_rate=0.25):
    inp = Input(shape=input_shape, name='Input_Sequence')
    
    # 1. Warstwa 1 BLSTM
    b1 = Bidirectional(LSTM(u1, return_sequences=True, kernel_regularizer=l2(1e-4)), name='BLSTM_1')(inp)
    b1_norm = LayerNormalization(name='LN_1')(b1)
    b1_drop = Dropout(drop_rate, name='Drop_1')(b1_norm)
    
    # 2. Warstwa 2 BLSTM z połączeniem resztkowym (projekcja wejścia)
    b2 = Bidirectional(LSTM(u2, return_sequences=True, kernel_regularizer=l2(1e-4)), name='BLSTM_2')(b1_drop)
    b2_norm = LayerNormalization(name='LN_2')(b2)
    
    # 3. Mechanizm uwagi Temporal Attention Pooling
    attn_score = Dense(1, activation='tanh', name='Attn_Dense')(b2_norm)
    attn_w = Softmax(axis=1, name='Attn_Weights')(attn_score)
    context = Lambda(lambda t: tf.reduce_sum(t[0] * t[1], axis=1), name='Attn_Context')([b2_norm, attn_w])
    
    ctx_norm = LayerNormalization(name='LN_Context')(context)
    ctx_drop = Dropout(drop_rate, name='Drop_Ctx')(ctx_norm)
    
    # Wspólna reprezentacja ukryta
    shared = Dense(64, activation='relu', kernel_regularizer=l2(1e-4), name='Shared_Dense')(ctx_drop)
    shared_norm = LayerNormalization(name='LN_Shared')(shared)
    shared_drop = Dropout(drop_rate, name='Drop_Shared')(shared_norm)
    
    # Głowica UP (+1.5% w 24h)
    up_dense = Dense(32, activation='relu', name='Dense_UP')(shared_drop)
    up_drop = Dropout(0.2, name='Drop_UP')(up_dense)
    out_up = Dense(1, activation='sigmoid', name='prob_up')(up_drop)
    
    # Głowica DOWN (-1.5% w 24h)
    down_dense = Dense(32, activation='relu', name='Dense_DOWN')(shared_drop)
    down_drop = Dropout(0.2, name='Drop_DOWN')(down_dense)
    out_down = Dense(1, activation='sigmoid', name='prob_down')(down_drop)
    
    model = Model(inputs=inp, outputs=[out_up, out_down], name='Dual_Head_BLSTM')
    optimizer = tf.keras.optimizers.Adam(learning_rate=lr, clipnorm=1.0)
    
    model.compile(
        optimizer=optimizer,
        loss={'prob_up': 'binary_crossentropy', 'prob_down': 'binary_crossentropy'},
        loss_weights={'prob_up': 1.0, 'prob_down': 1.0},
        metrics={
            'prob_up': ['accuracy', tf.keras.metrics.AUC(name='auc')],
            'prob_down': ['accuracy', tf.keras.metrics.AUC(name='auc')]
        }
    )
    return model


def run_dual_head_experiment(window_size=48, epochs=25, batch_size=256):
    print(f"\n{'='*75}")
    print(" ROZPOCZYNAMY EKSPERYMENT: DUAL-HEAD MULTI-TASK BLSTM (UP +1.5% & DOWN -1.5%)")
    print(f"{'='*75}")
    
    csv_p = 'data/hourly/processed/bitcoin_hourly_final.csv'
    df_raw = pd.read_csv(csv_p)
    df = add_advanced_features_and_targets(df_raw, horizon=24, target_pct=0.015)
    
    n = len(df)
    n_train = int(n * 0.8)
    n_val = int(n * 0.1)
    
    train_df = df.iloc[:n_train].copy()
    val_df = df.iloc[n_train : n_train + n_val].copy()
    test_df = df.iloc[n_train + n_val :].copy()
    
    print(f"Próbki: Train={len(train_df):,}, Val={len(val_df):,}, Test={len(test_df):,}")
    print(f"Train UP={train_df['target_touch_up'].mean():.2%}, DOWN={train_df['target_touch_down'].mean():.2%}")
    print(f"Test  UP={test_df['target_touch_up'].mean():.2%}, DOWN={test_df['target_touch_down'].mean():.2%}")
    
    # Selekcja cech łącząca sygnały dla UP i DOWN
    ignore_cols = [
        'timestamp', 'target_touch_up', 'target_touch_down', 'target_first_touch',
        'target_max_return_fwd', 'target_min_return_fwd', 'target_fwd_return_24h'
    ]
    candidate_features = [c for c in df.columns if c not in ignore_cols and np.issubdtype(df[c].dtype, np.number)]
    
    feature_scores = {}
    for c in candidate_features:
        r_up, p_up = pearsonr(train_df[c].values, train_df['target_touch_up'].values)
        r_dn, p_dn = pearsonr(train_df[c].values, train_df['target_touch_down'].values)
        score = max(abs(r_up), abs(r_dn))
        if min(p_up, p_dn) < 0.05 and not np.isnan(score):
            feature_scores[c] = (score, r_up, r_dn)
            
    sorted_features = sorted(feature_scores.items(), key=lambda x: x[1][0], reverse=True)
    feature_cols = [x[0] for x in sorted_features[:24]]  # Top 24 cechy
    
    print(f"\nWyselekcjonowano {len(feature_cols)} cech wejściowych o najwyższej sile predykcyjnej:")
    for i, (col, (sc, r_u, r_d)) in enumerate(sorted_features[:10], 1):
        print(f"  {i:>2}. {col:<28} r(UP)={r_u:>+7.4f} | r(DOWN)={r_d:>+7.4f}")
    if len(feature_cols) > 10:
        print(f"  ... oraz {len(feature_cols) - 10} kolejnych cech.")
        
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(train_df[feature_cols].values)
    X_val_scaled = scaler.transform(val_df[feature_cols].values)
    X_test_scaled = scaler.transform(test_df[feature_cols].values)
    
    y_tr_up, y_tr_dn = train_df['target_touch_up'].values, train_df['target_touch_down'].values
    y_va_up, y_va_dn = val_df['target_touch_up'].values, val_df['target_touch_down'].values
    y_te_up, y_te_dn = test_df['target_touch_up'].values, test_df['target_touch_down'].values
    
    X_tr, y_tr_u, y_tr_d = create_dual_sequences(X_train_scaled, y_tr_up, y_tr_dn, window=window_size)
    X_va, y_va_u, y_va_d = create_dual_sequences(X_val_scaled, y_va_up, y_va_dn, window=window_size)
    X_te, y_te_u, y_te_d = create_dual_sequences(X_test_scaled, y_te_up, y_te_dn, window=window_size)
    
    print(f"\nTensory sekwencji: X_tr={X_tr.shape}, X_va={X_va.shape}, X_te={X_te.shape}")
    
    model = build_dual_head_blstm(input_shape=(window_size, len(feature_cols)), lr=0.0008, drop_rate=0.25)
    
    callbacks = [
        EarlyStopping(monitor='val_loss', mode='min', patience=6, restore_best_weights=True, verbose=1),
        ReduceLROnPlateau(monitor='val_loss', mode='min', factor=0.5, patience=2, min_lr=1e-5, verbose=1)
    ]
    
    t0 = time.time()
    history = model.fit(
        X_tr, {'prob_up': y_tr_u, 'prob_down': y_tr_d},
        validation_data=(X_va, {'prob_up': y_va_u, 'prob_down': y_va_d}),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=callbacks,
        verbose=1
    )
    t_train = time.time() - t0
    print(f"Trening ukończony w {t_train:.1f} s")
    
    # Predykcja na zbiorze testowym
    preds = model.predict(X_te, batch_size=512, verbose=0)
    p_up_te = preds[0].flatten()
    p_down_te = preds[1].flatten()
    
    auc_up = roc_auc_score(y_te_u, p_up_te)
    auc_dn = roc_auc_score(y_te_d, p_down_te)
    acc_up_50 = accuracy_score(y_te_u, (p_up_te >= 0.50).astype(int))
    acc_dn_50 = accuracy_score(y_te_d, (p_down_te >= 0.50).astype(int))
    prec_up_50 = precision_score(y_te_u, (p_up_te >= 0.50).astype(int), zero_division=0)
    prec_dn_50 = precision_score(y_te_d, (p_down_te >= 0.50).astype(int), zero_division=0)
    
    print(f"\n===========================================================================")
    print(" PODSUMOWANIE METRYK MODELU DUAL-HEAD BLSTM (Zbiór Testowy, tau=0.50)")
    print(f"===========================================================================")
    print(f"  Head UP (+1.5%):   Accuracy={acc_up_50:.2%} | ROC-AUC={auc_up:.4f} | Precision={prec_up_50:.2%}")
    print(f"  Head DOWN (-1.5%): Accuracy={acc_dn_50:.2%} | ROC-AUC={auc_dn:.4f} | Precision={prec_dn_50:.2%}")
    print(f"  Średnia dokładność (Balanced Accuracy): {(acc_up_50 + acc_dn_50)/2:.2%}")
    print(f"  Średni ROC-AUC:                          {(auc_up + auc_dn)/2:.4f}")
    
    # Skanowanie optymalnych progów pewności dla sygnałów LONG i SHORT
    print(f"\n--- SKANOWANIE PROGÓW PEWNOŚCI DLA SYGNAŁÓW LONG (Kupno przy P(UP) >= tau) ---")
    for tau in [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]:
        idx = np.where(p_up_te >= tau)[0]
        if len(idx) >= 30:
            pr = np.mean(y_te_u[idx] == 1)
            print(f"  tau={tau:.2f} -> Precyzja: {pr:>6.2%} ({len(idx):>4} transakcji)")
            
    print(f"\n--- SKANOWANIE PROGÓW PEWNOŚCI DLA SYGNAŁÓW SHORT (Sprzedaż przy P(DOWN) >= tau) ---")
    for tau in [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]:
        idx = np.where(p_down_te >= tau)[0]
        if len(idx) >= 30:
            pr = np.mean(y_te_d[idx] == 1)
            print(f"  tau={tau:.2f} -> Precyzja: {pr:>6.2%} ({len(idx):>4} transakcji)")
            
    # Pełny symulator transakcyjny (Long + Short)
    test_close = test_df['Close'].values[window_size:]
    test_high = test_df['High'].values[window_size:]
    test_low = test_df['Low'].values[window_size:]
    
    TAU_LONG = 0.65
    TAU_SHORT = 0.65
    FEE = 0.0005  # 0.05% prowizji Binance
    INITIAL_CAP = 10000.0
    
    equity = INITIAL_CAP
    pos_type = 0  # 0: brak, +1: Long, -1: Short
    entry_price = 0.0
    entry_idx = 0
    trades = []
    
    for i in range(len(test_close) - 24):
        # Sprawdzamy wyjście z otwartej pozycji
        if pos_type == 1:  # Long
            if test_high[i] >= entry_price * 1.015:
                pnl = 0.015 - 2 * FEE
                equity *= (1.0 + pnl)
                trades.append(('LONG', pnl, True))
                pos_type = 0
            elif i - entry_idx >= 24:
                ret = (test_close[i] - entry_price) / entry_price
                pnl = ret - 2 * FEE
                equity *= (1.0 + pnl)
                trades.append(('LONG', pnl, pnl > 0))
                pos_type = 0
                
        elif pos_type == -1:  # Short
            if test_low[i] <= entry_price * 0.985:
                pnl = 0.015 - 2 * FEE
                equity *= (1.0 + pnl)
                trades.append(('SHORT', pnl, True))
                pos_type = 0
            elif i - entry_idx >= 24:
                ret = -(test_close[i] - entry_price) / entry_price
                pnl = ret - 2 * FEE
                equity *= (1.0 + pnl)
                trades.append(('SHORT', pnl, pnl > 0))
                pos_type = 0
                
        # Sprawdzamy wejście w nową pozycję
        if pos_type == 0:
            up_sig = p_up_te[i] >= TAU_LONG and p_down_te[i] < 0.40
            down_sig = p_down_te[i] >= TAU_SHORT and p_up_te[i] < 0.40
            
            if up_sig and not down_sig:
                pos_type = 1
                entry_price = test_close[i]
                entry_idx = i
            elif down_sig and not up_sig:
                pos_type = -1
                entry_price = test_close[i]
                entry_idx = i
                
    total_ret = (equity - INITIAL_CAP) / INITIAL_CAP
    pnls = [t[1] for t in trades]
    win_cnt = sum([t[2] for t in trades])
    win_rate = win_cnt / len(trades) if trades else 0.0
    sharpe = (np.mean(pnls) / (np.std(pnls) + 1e-8)) * np.sqrt(365 * 24 / 24) if trades else 0.0
    
    long_trades = [t for t in trades if t[0] == 'LONG']
    short_trades = [t for t in trades if t[0] == 'SHORT']
    
    print(f"\n===========================================================================")
    print(" SYMULACJA STRATEGII DWUKIERUNKOWEJ (LONG + SHORT) Z PROWIZJĄ 0.05%")
    print(f"===========================================================================")
    print(f"  Kapitał początkowy:    ${INITIAL_CAP:,.2f}")
    print(f"  Kapitał końcowy:       ${equity:,.2f} ({total_ret:+.2%})")
    print(f"  Liczba transakcji:     {len(trades)} (Long: {len(long_trades)}, Short: {len(short_trades)})")
    print(f"  Ogólny Win Rate:       {win_rate:.2%}")
    if long_trades:
        print(f"    Win Rate LONG:       {np.mean([t[2] for t in long_trades]):.2%}")
    if short_trades:
        print(f"    Win Rate SHORT:      {np.mean([t[2] for t in short_trades]):.2%}")
    print(f"  Wskaźnik Sharpe'a:     {sharpe:.2f}")
    print(f"===========================================================================\n")
    
    return {
        'auc_up': auc_up,
        'auc_dn': auc_dn,
        'acc_up': acc_up_50,
        'acc_dn': acc_dn_50,
        'trades': len(trades),
        'win_rate': win_rate,
        'total_ret': total_ret,
        'sharpe': sharpe,
        'model': model
    }

if __name__ == '__main__':
    run_dual_head_experiment()

"""
Moduł przygotowania zaawansowanych cech i targetów 1.5% do modeli BLSTM.
Bez wycieku danych (strict zero look-ahead bias).
"""
import numpy as np
import pandas as pd

def add_advanced_features_and_targets(df_in: pd.DataFrame, horizon: int = 24, target_pct: float = 0.015):
    df = df_in.copy()
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
    
    close = df['Close']
    high = df['High']
    low = df['Low']
    vol_btc = df['Volume BTC'] if 'Volume BTC' in df.columns else df['Volume USDT'] / close
    vol_usd = df['Volume USDT'] if 'Volume USDT' in df.columns else df['Volume BTC'] * close

    # 1. Multi-scale price momentum & returns (strictly backward looking)
    for h in [2, 4, 8, 12, 48, 72, 168]:
        df[f'return_{h}h'] = np.log(close / close.shift(h))
    
    # 2. Volatility & True Range features
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    df['atr_14h_rel'] = (true_range.rolling(14).mean() / close)
    df['atr_24h_rel'] = (true_range.rolling(24).mean() / close)
    df['high_low_ratio_1h'] = (high - low) / close
    
    # 3. Position relative to recent rolling extremes
    for w in [24, 72, 168]:
        roll_h = high.rolling(w).max()
        roll_l = low.rolling(w).min()
        rng = (roll_h - roll_l).replace(0, np.nan)
        df[f'pos_in_range_{w}h'] = (close - roll_l) / rng
        df[f'dist_to_high_{w}h'] = (close - roll_h) / close
        df[f'dist_to_low_{w}h'] = (close - roll_l) / close

    # 4. Volume pressure & dynamics
    df['vol_surge_24h'] = vol_usd / (vol_usd.rolling(24).mean() + 1e-8)
    if 'total_whale_volume_btc' in df.columns:
        df['whale_vol_ratio_24h'] = df['whale_vol_sum_24h'] / (vol_btc.rolling(24).sum() + 1e-8)
    
    # 5. Cyclical time features (US & global liquidity session alignment)
    if 'timestamp' in df.columns:
        hours = df['timestamp'].dt.hour
        days = df['timestamp'].dt.dayofweek
        df['hour_sin'] = np.sin(2 * np.pi * hours / 24.0)
        df['hour_cos'] = np.cos(2 * np.pi * hours / 24.0)
        df['day_sin'] = np.sin(2 * np.pi * days / 7.0)
        df['day_cos'] = np.cos(2 * np.pi * days / 7.0)

    # 6. Targets definition for the forward window [t+1, t+horizon]
    n = len(df)
    close_vals = close.values
    high_vals = high.values
    low_vals = low.values
    
    target_touch_up = np.full(n, np.nan)
    target_touch_down = np.full(n, np.nan)
    target_first_touch = np.full(n, np.nan)
    target_max_return_fwd = np.full(n, np.nan)
    target_min_return_fwd = np.full(n, np.nan)
    target_fwd_return_24h = np.full(n, np.nan)
    
    for i in range(n - horizon):
        c0 = close_vals[i]
        up_b = c0 * (1.0 + target_pct)
        dn_b = c0 * (1.0 - target_pct)
        
        h_win = high_vals[i+1 : i+1+horizon]
        l_win = low_vals[i+1 : i+1+horizon]
        
        max_h = np.max(h_win)
        min_l = np.min(l_win)
        
        target_max_return_fwd[i] = (max_h - c0) / c0
        target_min_return_fwd[i] = (min_l - c0) / c0
        target_fwd_return_24h[i] = (close_vals[i+horizon] - c0) / c0
        
        touch_u = (max_h >= up_b)
        touch_d = (min_l <= dn_b)
        target_touch_up[i] = 1.0 if touch_u else 0.0
        target_touch_down[i] = 1.0 if touch_d else 0.0
        
        # First touch logic
        u_idx = np.where(h_win >= up_b)[0]
        d_idx = np.where(l_win <= dn_b)[0]
        first_u = u_idx[0] if len(u_idx) > 0 else 999
        first_d = d_idx[0] if len(d_idx) > 0 else 999
        
        if first_u < first_d:
            target_first_touch[i] = 1.0   # Touched +1.5% first (Long win)
        elif first_d < first_u:
            target_first_touch[i] = 0.0   # Touched -1.5% first (Short win)
        else:
            # Neither touched or same hour -> final 24h close direction
            target_first_touch[i] = 1.0 if close_vals[i+horizon] > c0 else 0.0

    df['target_touch_up'] = target_touch_up
    df['target_touch_down'] = target_touch_down
    df['target_first_touch'] = target_first_touch
    df['target_max_return_fwd'] = target_max_return_fwd
    df['target_min_return_fwd'] = target_min_return_fwd
    df['target_fwd_return_24h'] = target_fwd_return_24h
    
    # Drop rows with NaNs from feature lookback and target forward horizon
    df = df.dropna().reset_index(drop=True)
    return df

if __name__ == '__main__':
    csv_p = 'data/hourly/processed/bitcoin_hourly_final.csv'
    df_raw = pd.read_csv(csv_p)
    df_feat = add_advanced_features_and_targets(df_raw)
    print("New dataset shape:", df_feat.shape)
    print("Target touch UP mean:", df_feat['target_touch_up'].mean())
    print("Target touch DOWN mean:", df_feat['target_touch_down'].mean())
    print("Target first touch mean:", df_feat['target_first_touch'].mean())

"""
config.py — Centralna konfiguracja projektu.
Jeden plik do zmiany wszystkich hiperparametrów i ścieżek.
"""

from pathlib import Path

# ── Ścieżki ───────────────────────────────────────────────────────────────────
ROOT_DIR   = Path(__file__).parent.parent
DATA_RAW   = ROOT_DIR / "data" / "raw"
DATA_PROC  = ROOT_DIR / "data" / "processed"
MODELS_DIR = ROOT_DIR / "models"

CSV_MASTER = DATA_PROC / "bitcoin_master.csv"
CSV_FINAL  = DATA_PROC / "bitcoin_final.csv"

# ── Cechy ─────────────────────────────────────────────────────────────────────
EXOG_COLS = [
    'active_addresses',
    'stablecoin_market_cap',
    'n-transactions',
    'estimated-transaction-volume-usd',
    'hash-rate',
    'whale_proxy_avg_tx',
    'hodl_velocity',
    'ssr_ratio',
    'ln_total_capacity_btc',
    'mvrv_zscore',
    'whale_tx_above_1m',
]

# ── Wspólne hiperparametry treningu ───────────────────────────────────────────
TEST_DAYS  = 180
BATCH_SIZE = 32
EPOCHS     = 150
LR         = 0.001
PATIENCE   = 20
DROPOUT    = 0.2

# ── Hiperparametry per-model ──────────────────────────────────────────────────
MODEL_CONFIGS = {
    'lstm': {
        'window': 60,
        'units':  (64, 32),
    },
    'blstm': {
        'window': 14,
        'units':  (64, 32),
    },
    'conv1d': {
        'window':     30,
        'filters':    (64, 128),
        'kernel':     3,
        'lstm_units': 32,
    },
}

# ── Horyzonty prognozy ────────────────────────────────────────────────────────
HORIZONS = {
    '1 dzień':    1,
    '1 tydzień':  7,
    '1 miesiąc':  30,
    '3 miesiące': 90,
    '6 miesięcy': 180,
}

"""
predict.py — Interfejs predykcji: "predict bitcoin for <horyzont>".

Użycie (CLI):
    python src/predict.py --horizon "1 dzień"
    python src/predict.py --horizon "1 tydzień"
    python src/predict.py --horizon "1 miesiąc"
    python src/predict.py --horizon "3 miesiące"
    python src/predict.py --horizon "6 miesięcy"

Użycie (notebook):
    from predict import predict_all
    df = predict_all('1 miesiąc')
"""

import argparse
import pandas as pd

from data_loader import load_full, make_sequences
from model_registry import get_model, all_names
from config import HORIZONS, TEST_DAYS


def predict_one(model_name: str, n_steps: int) -> pd.Series:
    """
    Wczytuje zapisany model i generuje prognozę n_steps dni wprzód.
    Zwraca pd.Series indeksowaną datami przyszłymi.
    """
    model = get_model(model_name)

    if not model.is_trained():
        raise FileNotFoundError(
            f"Brak wag dla '{model_name}'. Uruchom najpierw: "
            f"python src/train.py --model {model_name}"
        )

    model.load()

    df, available = load_full()
    features = ['price'] + available

    X, _, _, _, _ = make_sequences(df, features, model.window)

    last_window = X[-1].reshape(1, model.window, X.shape[2])
    last_price  = df['price'].iloc[-1]
    last_date   = df.index[-1]

    prices = model.forecast(last_window, last_price, n_steps)

    future_dates = pd.date_range(
        start=last_date + pd.Timedelta(days=1), periods=n_steps
    )
    return pd.Series(prices, index=future_dates, name=model_name.upper())


def predict_all(horizon_name: str) -> pd.DataFrame:
    """
    Generuje prognozy wszystkich wytrenowanych modeli dla podanego horyzontu.

    Przykład:
        df = predict_all('1 miesiąc')
        #            LSTM    BLSTM   CONV1D
        # 2025-02-01  95000   94500   93800
        # ...
    """
    if horizon_name not in HORIZONS:
        raise ValueError(
            f"Nieznany horyzont: '{horizon_name}'. "
            f"Dostępne: {list(HORIZONS.keys())}"
        )

    n_steps = HORIZONS[horizon_name]
    print(f"\nGenerowanie prognozy: {horizon_name} ({n_steps} dni)...")

    df, _      = load_full()
    last_price = df['price'].iloc[-1]
    last_date  = df.index[-1]

    results = {}
    for name in all_names():
        try:
            results[name.upper()] = predict_one(name, n_steps)
            print(f"  {name.upper():<8} OK")
        except FileNotFoundError as e:
            print(f"  {name.upper():<8} POMINIĘTY — {e}")
        except Exception as e:
            print(f"  {name.upper():<8} BŁĄD — {e}")

    df_pred = pd.DataFrame(results)

    # ── Podsumowanie ──────────────────────────────────────────────────────────
    print(f"\n{'─'*52}")
    print(f"  Ostatnia cena:  ${last_price:,.0f}  ({last_date.date()})")
    print(f"  Horyzont:       {horizon_name}  ({n_steps} dni)")
    print(f"{'─'*52}")
    for col in df_pred.columns:
        val  = df_pred[col].iloc[-1]
        chg  = (val / last_price - 1) * 100
        sign = '+' if chg >= 0 else ''
        print(f"  {col:<8}  ${val:>10,.0f}   ({sign}{chg:.1f}%)")
    print(f"{'─'*52}")

    return df_pred


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prognoza ceny BTC')
    parser.add_argument(
        '--horizon',
        choices=list(HORIZONS.keys()),
        default='1 miesiąc',
    )
    args = parser.parse_args()
    predict_all(args.horizon)

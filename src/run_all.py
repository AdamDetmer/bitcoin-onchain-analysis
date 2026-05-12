"""
run_all.py — Odpala trening wszystkich modeli sekwencyjnie,
zbiera wyniki i zapisuje raport do models/training_report.json.

Użycie:
    python src/run_all.py
    python src/run_all.py --models lstm conv1d   # tylko wybrane
    python src/run_all.py --skip blstm           # z pominięciem
"""

import argparse
import json
import time
import traceback
from datetime import datetime
from pathlib import Path

from config import MODELS_DIR, TEST_DAYS
from data_loader import load_full, make_sequences, train_test_split_ts
from model_registry import get_model, all_names

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error


# ── Metryki ───────────────────────────────────────────────────────────────────

def evaluate(model, X_test: np.ndarray, y_test: np.ndarray, df_full, scaler_price) -> dict:
    """
    Oblicza MAE, RMSE, MAPE i DA (Direction Accuracy) na zbiorze testowym.
    Konwertuje log-returny z powrotem na ceny przed liczeniem błędów.
    """
    pred_scaled = model._keras_model.predict(X_test, verbose=0)
    pred_returns = scaler_price.inverse_transform(pred_scaled).flatten()

    real_prices = df_full['price'].iloc[-len(pred_returns) - 1:-1].values
    pred_prices = real_prices * np.exp(pred_returns)
    true_prices = df_full['price'].iloc[-len(pred_returns):].values

    mae  = mean_absolute_error(true_prices, pred_prices)
    rmse = np.sqrt(mean_squared_error(true_prices, pred_prices))
    mape = np.mean(np.abs((true_prices - pred_prices) / true_prices)) * 100

    # Direction Accuracy: czy model trafił kierunek zmiany?
    true_dir = np.sign(np.diff(true_prices))
    pred_dir = np.sign(pred_prices[1:] - true_prices[:-1])
    da = np.mean(true_dir == pred_dir) * 100

    return {
        'mae':  round(float(mae),  2),
        'rmse': round(float(rmse), 2),
        'mape': round(float(mape), 4),
        'da':   round(float(da),   2),
    }


# ── Trening jednego modelu ────────────────────────────────────────────────────

def train_and_evaluate(model_name: str, df_full, available: list) -> dict:
    model    = get_model(model_name)
    features = ['price'] + available

    X, y, scaler_all, scaler_price, dates = make_sequences(df_full, features, model.window)
    X_train, X_test, y_train, y_test, _, _ = train_test_split_ts(X, y, dates, TEST_DAYS)

    t_start = time.time()
    history = model.train(X_train, y_train, scaler_all, scaler_price, features)
    t_train = round(time.time() - t_start, 1)

    metrics = evaluate(model, X_test, y_test, df_full, scaler_price)
    epochs_run = len(history.history['loss'])

    result = {
        'model':      model_name,
        'window':     model.window,
        'n_features': len(features),
        'epochs_run': epochs_run,
        'train_time_sec': t_train,
        'metrics': metrics,
        'status': 'ok',
    }

    print(f"\n  ✓ {model_name.upper():<8}  "
          f"MAE={metrics['mae']:,.0f}  MAPE={metrics['mape']:.2f}%  "
          f"DA={metrics['da']:.1f}%  ({t_train}s, {epochs_run} epok)")

    return result


# ── Główna funkcja ────────────────────────────────────────────────────────────

def run_all(targets: list[str]) -> None:
    print(f"\n{'═'*60}")
    print(f"  Bitcoin Model Training Run")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Modele: {targets}")
    print(f"{'═'*60}")

    df_full, available = load_full()
    print(f"\n  Dane: {len(df_full)} wierszy  |  Cechy: {len(available)}")

    report = {
        'run_at':   datetime.now().isoformat(),
        'models':   {},
        'ranking':  [],
    }

    # ── Trening sekwencyjny ───────────────────────────────────────────────────
    for name in targets:
        print(f"\n{'─'*60}")
        try:
            result = train_and_evaluate(name, df_full, available)
            report['models'][name] = result
        except Exception as e:
            print(f"\n  ✗ {name.upper()} BŁĄD: {e}")
            traceback.print_exc()
            report['models'][name] = {'model': name, 'status': 'error', 'error': str(e)}

    # ── Ranking po MAPE ───────────────────────────────────────────────────────
    ranked = sorted(
        [v for v in report['models'].values() if v.get('status') == 'ok'],
        key=lambda x: x['metrics']['mape'],
    )
    report['ranking'] = [r['model'] for r in ranked]

    # ── Zapis raportu ─────────────────────────────────────────────────────────
    MODELS_DIR.mkdir(exist_ok=True)
    report_path = MODELS_DIR / 'training_report.json'
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    # ── Podsumowanie ──────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"  PODSUMOWANIE")
    print(f"{'═'*60}")
    print(f"  {'Model':<10} {'MAE':>10} {'RMSE':>10} {'MAPE':>8} {'DA':>7} {'Epoki':>7} {'Czas':>8}")
    print(f"  {'─'*10} {'─'*10} {'─'*10} {'─'*8} {'─'*7} {'─'*7} {'─'*8}")

    for i, r in enumerate(ranked):
        m    = r['metrics']
        flag = ' ←' if i == 0 else ''
        print(f"  {r['model'].upper():<10} "
              f"{m['mae']:>10,.0f} "
              f"{m['rmse']:>10,.0f} "
              f"{m['mape']:>7.2f}% "
              f"{m['da']:>6.1f}% "
              f"{r['epochs_run']:>7} "
              f"{r['train_time_sec']:>6.0f}s"
              f"{flag}")

    print(f"\n  Raport zapisany: {report_path}")
    print(f"{'═'*60}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Trening i ewaluacja wszystkich modeli BTC')
    parser.add_argument(
        '--models', nargs='+', choices=all_names(),
        default=None,
        help='Które modele trenować (domyślnie: wszystkie)',
    )
    parser.add_argument(
        '--skip', nargs='+', choices=all_names(),
        default=[],
        help='Które modele pominąć',
    )
    args = parser.parse_args()

    targets = args.models or all_names()
    targets = [m for m in targets if m not in args.skip]

    run_all(targets)

"""
train.py — Trening modeli poza Jupyterem.

Użycie:
    python src/train.py --model lstm
    python src/train.py --model blstm
    python src/train.py --model conv1d
    python src/train.py --model all

Po zakończeniu zapisuje w models/<name>/:
    weights.weights.h5   — wagi
    scaler_all.pkl
    scaler_price.pkl
    meta.json            — window, features, val_loss, epoki
"""

import argparse
from data_loader import load_full, make_sequences, train_test_split_ts, select_top_features
from model_registry import get_model, all_names
from config import TEST_DAYS, TOP_K_FEATURES


def train_one(model_name: str) -> None:
    model = get_model(model_name)

    df, available = load_full()
    best_exog = select_top_features(df, 'price', available, TOP_K_FEATURES)
    features = ['price'] + best_exog

    X, y, scaler_all, scaler_price, dates = make_sequences(df, features, model.window)
    X_train, X_test, y_train, y_test, _, _ = train_test_split_ts(X, y, dates, TEST_DAYS)

    model.train(
        X_train, y_train,
        scaler_all, scaler_price,
        features,
    )


if __name__ == '__main__':
    choices = all_names() + ['all']
    parser  = argparse.ArgumentParser(description='Trening modeli predykcji BTC')
    parser.add_argument('--model', choices=choices, default='all')
    args = parser.parse_args()

    targets = all_names() if args.model == 'all' else [args.model]
    for name in targets:
        train_one(name)

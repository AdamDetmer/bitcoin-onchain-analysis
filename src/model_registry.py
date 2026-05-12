"""
model_registry.py — Rejestr modeli.

Jedyne miejsce gdzie dodajesz nowy model do projektu.
train.py i predict.py korzystają wyłącznie z tego rejestru —
nie wiedzą nic o konkretnych klasach.

Żeby dodać nowy model:
    1. Utwórz src/model_mymodel.py dziedziczący z BitcoinModel
    2. Dodaj wpis do REGISTRY poniżej
    3. Gotowe — train.py i predict.py automatycznie go obsłużą
"""

from model_lstm   import LSTMModel
from model_blstm  import BLSTMModel
from model_conv1d import Conv1DModel

# klucz = nazwa używana w CLI:  python train.py --model lstm
REGISTRY: dict[str, type] = {
    'lstm':   LSTMModel,
    'blstm':  BLSTMModel,
    'conv1d': Conv1DModel,
}


def get_model(name: str):
    """Zwraca instancję modelu po nazwie. Rzuca ValueError dla nieznanej nazwy."""
    if name not in REGISTRY:
        raise ValueError(
            f"Nieznany model: '{name}'. Dostępne: {list(REGISTRY.keys())}"
        )
    return REGISTRY[name]()


def all_names() -> list[str]:
    return list(REGISTRY.keys())

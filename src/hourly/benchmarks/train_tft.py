"""
Benchmark Model 4: Temporal Fusion Transformer (TFT).
Keras 3 compatible architecture components:
1. Gated Residual Networks (GRN) with Gated Linear Units (GLU)
2. Variable Selection Network (VSN) for dynamic feature gating
3. Interpretable Multi-Head Self-Attention over 24-hour sequence
4. Temporal Attention Pooling and Binary Barrier Touch Head
"""

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['OMP_NUM_THREADS'] = '12'
os.environ['TF_NUM_INTRAOP_THREADS'] = '12'
os.environ['TF_NUM_INTEROP_THREADS'] = '2'

import sys
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, regularizers, optimizers, callbacks
from sklearn.metrics import roc_auc_score

from benchmark_evaluator import load_benchmark_data, create_sequences, evaluate_and_save_model

SEED = 42
np.random.seed(SEED)
tf.random.set_seed(SEED)

class GLU(layers.Layer):
    """Gated Linear Unit: x * sigmoid(W x + b)."""
    def __init__(self, units, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        self.dense = layers.Dense(units * 2)

    def call(self, inputs):
        projected = self.dense(inputs)
        return projected[..., :self.units] * tf.sigmoid(projected[..., self.units:])

class GRN(layers.Layer):
    """Gated Residual Network."""
    def __init__(self, units, dropout_rate=0.1, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        self.dense1 = layers.Dense(units, activation='elu')
        self.dense2 = layers.Dense(units)
        self.dropout = layers.Dropout(dropout_rate)
        self.glu = GLU(units)
        self.ln = layers.LayerNormalization()
        self.res_proj = layers.Dense(units)

    def call(self, inputs):
        x = self.dense1(inputs)
        x = self.dense2(x)
        x = self.dropout(x)
        x = self.glu(x)
        res = self.res_proj(inputs)
        return self.ln(x + res)

class VariableSelectionNetwork(layers.Layer):
    """Dynamically weights input features at each time step (Vectorized TFT formulation)."""
    def __init__(self, num_features, units, dropout_rate=0.1, **kwargs):
        super().__init__(**kwargs)
        self.num_features = num_features
        self.units = units
        self.grn_weights = GRN(num_features, dropout_rate)
        self.feature_proj = layers.Dense(units)
        self.post_grn = GRN(units, dropout_rate)
        self.softmax = layers.Softmax(axis=-1)

    def call(self, inputs):
        weights = self.softmax(self.grn_weights(inputs)) # (batch, time, num_features)
        feat_expanded = tf.expand_dims(inputs, axis=-1)
        feat_emb = self.feature_proj(feat_expanded)
        weighted = feat_emb * tf.expand_dims(weights, axis=-1)
        combined = tf.reduce_sum(weighted, axis=2) # (batch, time, units)
        return self.post_grn(combined)

class TemporalAttentionPooling(layers.Layer):
    """Pools sequence representations weighted by temporal attention."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.score_dense = layers.Dense(1)
        self.softmax = layers.Softmax(axis=1)

    def call(self, inputs):
        scores = self.score_dense(inputs) # (batch, time, 1)
        weights = self.softmax(scores)     # (batch, time, 1)
        return tf.reduce_sum(inputs * weights, axis=1) # (batch, units)

def build_tft_model(input_shape, d_model=32, num_heads=4, dropout_rate=0.15, lr=8e-4):
    time_steps, num_features = input_shape
    inputs = layers.Input(shape=input_shape)
    
    # 1. Variable Selection Network
    vsn = VariableSelectionNetwork(num_features, d_model, dropout_rate=dropout_rate)(inputs)
    
    # 2. Multi-Head Self-Attention across temporal horizon
    mha = layers.MultiHeadAttention(num_heads=num_heads, key_dim=d_model // num_heads)(vsn, vsn)
    mha_post = GRN(d_model, dropout_rate=dropout_rate)(mha)
    seq_repr = layers.LayerNormalization()(layers.add([vsn, mha_post]))
    
    # 3. Temporal Pooling with Attention weights across time axis
    pooled = TemporalAttentionPooling()(seq_repr)
    
    # 4. Final Classification Head
    dense_out = GRN(16, dropout_rate=dropout_rate)(pooled)
    outputs = layers.Dense(1, activation='sigmoid')(dense_out)
    
    model = tf.keras.Model(inputs=inputs, outputs=outputs, name="Temporal_Fusion_Transformer")
    model.compile(
        optimizer=optimizers.Adam(learning_rate=lr),
        loss="binary_crossentropy",
        metrics=["accuracy", tf.keras.metrics.AUC(name="auc")]
    )
    return model

def run_tft_benchmark():
    print("=" * 75)
    print(" BENCHMARK 4: TEMPORAL FUSION TRANSFORMER (TFT)")
    print("=" * 75)
    
    window = 24
    data = load_benchmark_data(num_features=24)
    X_train_sc, y_train = data["X_train"], data["y_train"]
    X_val_sc, y_val = data["X_val"], data["y_val"]
    X_test_sc, y_test = data["X_test"], data["y_test"]
    test_df = data["test_df"]
    
    X_tr_seq, y_tr_seq = create_sequences(X_train_sc, y_train, window=window)
    X_val_seq, y_val_seq = create_sequences(X_val_sc, y_val, window=window)
    X_te_seq, y_te_seq = create_sequences(X_test_sc, y_test, window=window)
    
    print(f"Sequences shape: Train={X_tr_seq.shape}, Val={X_val_seq.shape}, Test={X_te_seq.shape}")
    
    configs = [
        {"d_model": 32, "num_heads": 4, "lr": 1e-3, "name": "TFT_d32_h4_lr1e-3"},
        {"d_model": 48, "num_heads": 4, "lr": 6e-4, "name": "TFT_d48_h4_lr6e-4"},
        {"d_model": 64, "num_heads": 8, "lr": 5e-4, "name": "TFT_d64_h8_lr5e-4"}
    ]
    
    best_val_auc = 0.0
    best_model = None
    best_cfg = None
    
    input_shape = (window, X_train_sc.shape[1])
    
    for cfg in configs:
        print(f"\nTrenowanie wariantu TFT: {cfg['name']}...")
        model = build_tft_model(
            input_shape,
            d_model=cfg["d_model"],
            num_heads=cfg["num_heads"],
            dropout_rate=0.15,
            lr=cfg["lr"]
        )
        
        early_stop = callbacks.EarlyStopping(
            monitor="val_auc",
            mode="max",
            patience=7,
            restore_best_weights=True,
            verbose=0
        )
        reduce_lr = callbacks.ReduceLROnPlateau(
            monitor="val_auc",
            mode="max",
            factor=0.5,
            patience=3,
            min_lr=1e-5,
            verbose=0
        )
        
        hist = model.fit(
            X_tr_seq, y_tr_seq,
            validation_data=(X_val_seq, y_val_seq),
            epochs=25,
            batch_size=128,
            callbacks=[early_stop, reduce_lr],
            verbose=0
        )
        
        val_pred = model.predict(X_val_seq, batch_size=256, verbose=0).flatten()
        val_auc = roc_auc_score(y_val_seq, val_pred)
        best_epoch = int(np.argmax(hist.history.get("val_auc", [0])) + 1)
        print(f"  -> Best Val AUC: {val_auc:.4f} (epoka {best_epoch})")
        
        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_model = model
            best_cfg = cfg
            
    print(f"\nWybrano najlepszy wariant TFT: {best_cfg['name']} z Val AUC = {best_val_auc:.4f}")
    
    # Evaluate best model on out-of-sample Test set
    test_prob = best_model.predict(X_te_seq, batch_size=256, verbose=0).flatten()
    
    metrics = evaluate_and_save_model(
        model_name="TFT",
        y_true=y_te_seq,
        y_prob=test_prob,
        test_df=test_df,
        window_offset=window - 1,
        details={
            "best_config": best_cfg,
            "val_auc": float(best_val_auc),
            "architecture": "Temporal Fusion Transformer (TFT)",
            "window_size": window
        }
    )
    
    return metrics

if __name__ == '__main__':
    run_tft_benchmark()

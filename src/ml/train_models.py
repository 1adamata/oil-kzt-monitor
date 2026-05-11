"""
Trains two models on historical relationship metrics:
1. XGBoost regressor — predicts tomorrow's spread z-score
2. Random Forest classifier — predicts whether tomorrow is anomalous
"""
import os
import pickle
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, mean_absolute_error
from xgboost import XGBRegressor
from sklearn.model_selection import TimeSeriesSplit

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DB_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market_data",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
}

MODEL_DIR = Path("src/ml/models")


def load_features(symbol: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("""
        SELECT bucket_time, oil_logret, kzt_logret
        FROM md.curated_aligned_bars
        WHERE oil_symbol = %s AND horizon = '1d'
        ORDER BY bucket_time
    """, (symbol,))
    bars = pd.DataFrame(cur.fetchall(), columns=["date", "oil_logret", "kzt_logret"])

    cur.execute("""
        SELECT bucket_time, metric, horizon, value
        FROM md.relationship_metrics
        WHERE oil_symbol = %s
        ORDER BY bucket_time
    """, (symbol,))
    metrics_raw = pd.DataFrame(cur.fetchall(), columns=["date", "metric", "horizon", "value"])
    conn.close()

    metrics = metrics_raw.pivot_table(
        index="date", columns=["metric", "horizon"], values="value"
    ).reset_index()
    metrics.columns = ["date"] + [f"{m}_{h}" for m, h in metrics.columns[1:]]

    df = bars.merge(metrics, on="date", how="inner")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Lag features
    df["zscore_30d_lag1"] = df["spread_zscore_30d"].shift(1)
    df["zscore_30d_lag2"] = df["spread_zscore_30d"].shift(2)
    df["zscore_momentum"] = df["spread_zscore_30d"] - df["spread_zscore_30d"].shift(1)

    # Targets
    df["target_zscore"] = df["spread_zscore_30d"].shift(-1)
    df["target_anomaly"] = (df["target_zscore"].abs() > 2.5).astype(int)

    return df.dropna()


def get_features_targets(df: pd.DataFrame):
    feature_cols = [
        "oil_logret", "kzt_logret",
        "pearson_corr_30d", "pearson_corr_60d",
        "beta_30d", "beta_60d",
        "spread_zscore_30d", "spread_zscore_60d",
        "zscore_30d_lag1", "zscore_30d_lag2",
        "zscore_momentum",
    ]
    X = df[feature_cols]
    y_reg = df["target_zscore"]
    y_clf = df["target_anomaly"]
    return X, y_reg, y_clf


def train(symbol: str):
    print(f"\nTraining models for {symbol}...")
    df = load_features(symbol)
    print(f"  {len(df)} samples after feature engineering")

    X, y_reg, y_clf = get_features_targets(df)

    # Time series split — never use future data to train
    tscv = TimeSeriesSplit(n_splits=3)

    # --- XGBoost Regressor ---
    print("  Training XGBoost regressor...")
    xgb = XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1,
                       random_state=42, verbosity=0)
    mae_scores = []
    for train_idx, test_idx in tscv.split(X):
        xgb.fit(X.iloc[train_idx], y_reg.iloc[train_idx])
        preds = xgb.predict(X.iloc[test_idx])
        mae_scores.append(mean_absolute_error(y_reg.iloc[test_idx], preds))
    print(f"  XGBoost MAE (avg across folds): {np.mean(mae_scores):.4f}")

    # Train final model on all data
    xgb.fit(X, y_reg)

    # --- Random Forest Classifier ---
    print("  Training Random Forest classifier...")
    rf = RandomForestClassifier(n_estimators=100, max_depth=4,
                                random_state=42, class_weight="balanced")
    for train_idx, test_idx in tscv.split(X):
        rf.fit(X.iloc[train_idx], y_clf.iloc[train_idx])
        preds = rf.predict(X.iloc[test_idx])
    print("  Classification report (last fold):")
    print(classification_report(y_clf.iloc[test_idx], preds,
                                target_names=["normal", "anomalous"],
                                zero_division=0))

    # Train final model on all data
    rf.fit(X, y_clf)

    # Save models
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_DIR / f"{symbol}_xgb_regressor.pkl", "wb") as f:
        pickle.dump(xgb, f)
    with open(MODEL_DIR / f"{symbol}_rf_classifier.pkl", "wb") as f:
        pickle.dump(rf, f)
    print(f"  Models saved to {MODEL_DIR}/")


def main():
    for symbol in ["brent", "wti"]:
        train(symbol)
    print("\nDone.")


if __name__ == "__main__":
    main()
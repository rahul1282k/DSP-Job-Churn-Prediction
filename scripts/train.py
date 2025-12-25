import os
import argparse
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import classification_report, accuracy_score
from sklearn.ensemble import RandomForestClassifier


def load_data(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found at: {csv_path}")
    return pd.read_csv(csv_path)


def build_pipeline(cat_features, num_features) -> Pipeline:
    # OneHot for categoricals, scaling for numerics
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_features),
            ("num", StandardScaler(), num_features),
        ],
        remainder="drop"
    )

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        random_state=42,
        class_weight="balanced_subsample",
        n_jobs=-1
    )

    return Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("model", model),
    ])


def main(args):
    print("[INFO] Loading dataset...")
    df = load_data(args.csv)

    # ---- CONFIG ----
    target_col = args.target

    if target_col not in df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. Available columns: {list(df.columns)}"
        )

    # Drop identifier-like columns (IMPORTANT: fixes your API error)
    drop_cols = [target_col]
    if "enrollee_id" in df.columns:
        drop_cols.append("enrollee_id")

    # Features / labels
    X = df.drop(columns=drop_cols)
    y = df[target_col]

    # Detect feature types
    cat_features = X.select_dtypes(include=["object"]).columns.tolist()
    num_features = X.select_dtypes(exclude=["object"]).columns.tolist()

    print(f"[INFO] Rows: {len(df)} | Features: {X.shape[1]}")
    print(f"[INFO] Dropped columns: {drop_cols}")
    print(f"[INFO] Categorical features ({len(cat_features)}): {cat_features}")
    print(f"[INFO] Numerical features ({len(num_features)}): {num_features}")

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=42,
        stratify=y if y.nunique() > 1 else None
    )

    pipeline = build_pipeline(cat_features, num_features)

    print("[INFO] Training model...")
    pipeline.fit(X_train, y_train)

    print("[INFO] Evaluating model...")
    y_pred = pipeline.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    print(f"[RESULT] Accuracy: {acc:.4f}")
    print("[RESULT] Classification report:\n", classification_report(y_test, y_pred))

    # Ensure output dir exists
    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Saving model to: {args.output}")
    joblib.dump(pipeline, args.output)

    print("[SUCCESS] Model training completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Job Churn Prediction Model")

    parser.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Path to job.csv file"
    )

    parser.add_argument(
        "--output",
        type=str,
        default="airflow/data/artifacts/model.joblib",
        help="Path to save trained model (.joblib)"
    )

    parser.add_argument(
        "--target",
        type=str,
        default="target",
        help="Target column name in CSV (default: target)"
    )

    parser.add_argument(
        "--test_size",
        type=float,
        default=0.2,
        help="Test split size (default: 0.2)"
    )

    args = parser.parse_args()
    main(args)

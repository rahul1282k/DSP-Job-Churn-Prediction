from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os, json, pandas as pd, numpy as np, joblib
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import create_engine, text
from fastapi.responses import FileResponse
from fastapi import HTTPException


DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

DATA_PATH = "/app/data/job.csv"
ART_DIR = "/app/data/artifacts"
os.makedirs(ART_DIR, exist_ok=True)

engine = create_engine(DB_URL, future=True)

MODEL_PATH = os.path.join(ART_DIR, "model.joblib")
FEATURES_PATH = os.path.join(ART_DIR, "features.json")

class PredictPayload(BaseModel):
    records: List[Dict[str, Any]]
    source: Optional[str] = "webapp"

def _train_model():
    if not os.path.exists(DATA_PATH):
        raise RuntimeError(f"Training data not found at {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    
    if "target" not in df.columns:
        raise RuntimeError("Column 'target' not found in training data")

    if "enrollee_id" in df.columns:
        df = df.drop(columns=["enrollee_id"])

    df = df[~df["target"].isna()]

    y = df["target"].astype(int)
    X = df.drop(columns=["target"])

    cat_cols = [c for c in X.columns if X[c].dtype == "object"]
    num_cols = [c for c in X.columns if c not in cat_cols]

    numeric_transformer = Pipeline(
        steps=[("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]
    )
    categorical_transformer = Pipeline(
        steps=[("imputer", SimpleImputer(strategy="most_frequent")),
               ("onehot", OneHotEncoder(handle_unknown="ignore"))]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, num_cols),
            ("cat", categorical_transformer, cat_cols),
        ]
    )

    model = RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        class_weight="balanced",
    )

    clf = Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])
    clf.fit(X, y)

    joblib.dump(clf, MODEL_PATH)

    with open(FEATURES_PATH, "w") as f:
        json.dump({"feature_columns": list(X.columns)}, f)

    return clf, list(X.columns)

def _load_model():
    if os.path.exists(MODEL_PATH) and os.path.exists(FEATURES_PATH):
        clf = joblib.load(MODEL_PATH)
        with open(FEATURES_PATH) as f:
            meta = json.load(f)
        return clf, meta["feature_columns"]

    return _train_model()

app = FastAPI(title="Job Target Prediction API", version="1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(payload: PredictPayload):
    if not payload.records:
        return {"predictions": []}

    clf, feature_cols = _load_model()

    df = pd.DataFrame(payload.records)

    if "enrollee_id" in df.columns:
        df = df.drop(columns=["enrollee_id"])

    for col in feature_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[feature_cols]

    # FIX: DO NOT FILL WITH ZERO
    df = df.replace([np.inf, -np.inf], np.nan)

    proba = clf.predict_proba(df)

    classes = list(clf.named_steps["model"].classes_)
    idx1 = classes.index(1) if 1 in classes else 0

    p1 = proba[:, idx1]
    preds = (p1 >= 0.5).astype(int)

    results = [
        {"prediction": int(y), "probability": float(p)}
        for y, p in zip(preds, p1)
    ]

    with engine.begin() as con:
        for feats, pred in zip(payload.records, results):
            con.execute(
                text("INSERT INTO predictions(source, model_version, features, prediction) VALUES (:s,:v,:f,:p)"),
                {"s": payload.source, "v": "v1", "f": json.dumps(feats), "p": json.dumps(pred)},
            )

    return {"predictions": results,"features": payload.records, "model_version": "v1", "n": len(results)}

@app.get("/past-predictions")
def past_predictions(start: Optional[str] = None, end: Optional[str] = None,
                     source: Optional[str] = None, limit: int = 200):

    q = "SELECT created_at, source, model_version, features, prediction FROM predictions WHERE 1=1"
    params = {}

    if start:
        q += " AND created_at >= :start"
        params["start"] = start

    if end:
        q += " AND created_at <= :end"
        params["end"] = end

    if source and source != "all":
        q += " AND source = :source"
        params["source"] = source

    q += " ORDER BY created_at DESC LIMIT :limit"
    params["limit"] = limit

    with engine.begin() as con:
        rows = con.execute(text(q), params).mappings().all()

    return {"rows": [dict(r) for r in rows]}

@app.get("/reports/{report_name}")
def get_report(report_name: str):
    """
    Serve HTML data-quality reports stored in /app/data/reports.
    Allows Teams to open them via a public URL.
    """
    report_path = f"/app/data/reports/{report_name}"

    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Report not found")

    return FileResponse(report_path, media_type="text/html")

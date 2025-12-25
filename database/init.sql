-- ============================================================
--  MASTER SQL FILE FOR YOUR PIPELINE
--  Creates ALL tables required by:
--    ✔ dag_ingestion.py
--    ✔ dag_prediction.py
--    ✔ FastAPI model service (predictions)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

----------------------------------------------------------------
-- 0) Model Predictions Table
--     Used by FastAPI /predict and /past-predictions
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP DEFAULT NOW(),
    source TEXT NOT NULL,
    model_version TEXT NOT NULL,
    features JSONB NOT NULL,
    prediction JSONB NOT NULL
);

----------------------------------------------------------------
-- 1) Data Quality Statistics Table
--    Used in dag_ingestion.py → save_statistics()
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_stats (
    id SERIAL PRIMARY KEY,
    file_name TEXT,
    total_rows INTEGER,
    valid_rows INTEGER,
    invalid_rows INTEGER,
    criticality TEXT,
    issues JSONB,
    report_path TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);  

----------------------------------------------------------------
-- 2) Prediction Logs Table
--    (Optional extra logging, kept from your earlier version)
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prediction_logs (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    n_rows INTEGER NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

----------------------------------------------------------------
-- 3) Ingested Files Tracking
--    Needed for:
--        dag_ingestion.py → split_and_save_data()
--        dag_prediction.py → check_for_new_data()
-- =============================================================
--    INSERT INTO ingested_files(filename)
--    VALUES (…) ON CONFLICT DO NOTHING;
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingested_files (
    filename TEXT PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT NOW()
);

----------------------------------------------------------------
-- 4) Alert Logs
--    Used by send_alerts() to log data quality alerts
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_logs (
    id SERIAL PRIMARY KEY,
    filename TEXT,
    criticality TEXT,
    issues JSONB,
    report_path TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

----------------------------------------------------------------
-- 5) Predicted Files
--    Used by prediction DAG to avoid re-predicting same file
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predicted_files (
    filename TEXT PRIMARY KEY,
    predicted_at TIMESTAMP DEFAULT NOW()
);

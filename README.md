# Job Churn Prediction – Data Science in Production (E2E)

End‑to‑end **ML-powered application** for predicting whether a candidate will **change job (1)** or **not (0)**.

This project implements the “Data Science in Production” course architecture using:

- **Streamlit** (UI: on-demand single + batch prediction + past predictions)
- **FastAPI** (model API: serve + persist + retrieve predictions)
- **PostgreSQL + SQLAlchemy** (persistence for predictions + ingestion stats + processed files)
- **Airflow** (scheduled ingestion DAG + scheduled prediction DAG)
- **Great Expectations** (data quality validation + HTML report)
- **Grafana** (monitoring dashboards)

---

## Architecture

- **Streamlit Webapp** → calls → **FastAPI** → writes/reads → **PostgreSQL**
- **Airflow Ingestion DAG (every 1 min)**:
  - reads a random file from `airflow/data/raw_data/`
  - validates quality (Great Expectations)
  - stores **statistics** in Postgres (counts/ratios, not raw bad rows)
  - generates an **HTML report** + optional Teams alert
  - routes the ingested file into `good_data/` and/or `bad_data/` (splits mixed files)
- **Airflow Prediction DAG (every 2 min)**:
  - finds **new** files in `good_data/` (without moving/renaming them)
  - calls the API once per batch and stores predictions

---

## Repository layout

```
.
├── api/                         # FastAPI service
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── webapp/                      # Streamlit UI (multi-page)
│   ├── Home.py                  # single prediction
│   ├── pages/
│   │   ├── 1_Batch_Predict.py   # batch prediction
│   │   └── 2_Past_Predictions.py
│   ├── Dockerfile
│   └── requirements.txt
├── airflow/
│   ├── dags/
│   │   ├── dag_ingestion.py
│   │   └── dag_prediction.py
│   ├── data/
│   │   ├── job.csv              # source dataset
│   │   ├── raw_data/            # split files feeding ingestion
│   │   ├── good_data/           # validated rows/files (used by prediction job)
│   │   ├── bad_data/            # invalid rows/files
│   │   ├── reports/             # HTML GE-like reports
│   │   └── predicted_files/     # local marker files (optional)
│   └── Dockerfile
├── database/
│   └── init.sql                 # DB schema
├── grafana/                     # dashboards/provisioning
├── scripts/
│   └── split_dataset.py         # splits dataset into N files + injects errors
├── error_generation.ipynb       # notebook: demonstrate injected error types
├── docker-compose.yaml
└── .env
```

---

## Prerequisites

- Docker + Docker Compose v2
- (Optional) Python 3.10+ to run scripts locally

---

## Quick start

### 1) Generate “streaming” raw files

This script splits the main dataset into **N files** (not rows-per-file) and injects **7 data-quality error types**.

```bash
python scripts/split_dataset.py   --csv airflow/data/job.csv   --out airflow/data/raw_data   --n_files 20
```

### 2) Start the full stack

```bash
docker compose up -d --build
```

### 3) Open services

- Airflow: http://localhost:8080  (admin / admin)
- FastAPI docs: http://localhost:8000/docs
- Streamlit: http://localhost:8501
- Grafana: http://localhost:3000  (admin / admin)

---

## Streamlit webapp

### Single prediction (Home)
- Fill the form with feature values
- Click **Predict**
- The returned dataframe shows **input features + prediction + probability**

### Batch prediction
- Upload a CSV with inference features (target column can be present; it is ignored)
- The webapp sends the **data content** to the API (not a file path)
- Results are shown as a dataframe with predictions appended

### Past predictions
- Select start/end date
- Select source:
  - `webapp`
  - `scheduled`
  - `all`

---

## FastAPI endpoints

Base URL: `http://localhost:8000`

- `GET /health`
- `POST /predict`
  - used by Streamlit + scheduled prediction job
  - supports **single & multi prediction**
  - saves predictions + used features in Postgres
- `GET /past-predictions?start=YYYY-MM-DD&end=YYYY-MM-DD&source=webapp|scheduled|all`
  - returns stored predictions with features
- `GET /reports/{report_name}`
  - serves generated HTML ingestion reports (used in Teams alerts)

---

## Airflow DAGs

### 1) Ingestion DAG (`dag_ingestion`) – every 1 minute

Tasks:
- `read_data`: picks a random CSV from `raw_data/`
- `validate_data`: runs Great Expectations checks and sets criticality (low/medium/high)
- `save_statistics`: writes ingestion metrics & issue counts to DB
- `send_alerts`: generates an HTML report and optionally sends a Teams alert
- `split_and_save`: routes data into:
  - `good_data/` (valid rows)
  - `bad_data/` (invalid rows)
  - if mixed, splits into two files

### 2) Prediction DAG (`dag_prediction`) – every 2 minutes

Tasks:
- `check_for_new_data`: checks for new files in `good_data/`
  - if none, raises `AirflowSkipException` so the **DAG run is skipped**
- `make_predictions`: reads new files and calls the API once per batch

**Important:** the job does not move/rename files in `good_data/`.
It tracks processed files via Postgres table `predicted_files` and a “last timestamp” marker.

---

## Injected data quality error types (7)

Generated by `scripts/split_dataset.py`:
1. Missing values in required columns
2. `inf` / `-inf` in numeric columns
3. Invalid categorical value (e.g., gender)
4. Invalid categorical value (e.g., experience)
5. Impossible numeric values (e.g., negative `training_hours`)
6. String value in numeric column (`training_hours = "wrong_value"`)
7. Extreme outlier numeric value (`city_development_index = 1000`)

Demo notebook: `error_generation.ipynb`

---

## Monitoring (Grafana)

Dashboards query Postgres and refresh frequently:
- **Ingestion monitoring**: invalid ratio, criticality counts, issues over time, etc.
- **Prediction monitoring**: prediction distribution, avg prediction/probability, feature averages over time (drift proxy)

---

## Environment variables

See `.env` and `docker-compose.yaml`. Common variables:
- `DATABASE_URL` (API + Airflow)
- `API_URL` (Streamlit + Airflow prediction DAG)
- `API_PUBLIC_URL` (used to build clickable report links)
- `TEAMS_WEBHOOK` (optional) Teams notifications

---

## Known rubric mismatches / TODOs (course checklist)

If your instructor is strict, double-check these points:

1) **Ingestion DAG parallelism**
   - Rubric expects `save_statistics`, `send_alerts`, `split_and_save` to run **in parallel** after validation.
   - Current wiring runs `split_and_save` **after** `save_statistics` and `send_alerts`.
   - File: `airflow/dags/dag_ingestion.py`

2) **Missing required column error type**
   - Ingestion validation checks for missing columns, but the generator currently does not create “missing column” files.
   - Add one error type that drops a required column in some chunks, so you can demo it.
   - File: `scripts/split_dataset.py`

3) **True drift (training vs serving)**
   - Dashboards show serving feature averages (useful), but not a stored training baseline comparison.
   - If required, persist training baselines (mean/std/quantiles) and plot deltas/PSI/KS.

4) **Grafana alerts**
   - Ensure at least one alert rule is configured and demonstrable (e.g., invalid_pct > threshold; model predicts only one class).

---

## Resetting the stack

```bash
docker compose down -v
docker compose up -d --build
```

---

## License
Educational project.

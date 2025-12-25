# Job E2E (Great Expectations, Airflow, FastAPI, Streamlit, Postgres, Grafana)

## 1. Prepare raw data
```bash
python scripts/split_dataset.py --csv airflow/data/job.csv --out airflow/data/raw_data --n_files 20
```

## 2. Start stack
```bash
docker compose up -d --build
```

Then open:
- Airflow: http://localhost:8080  (user: admin / admin)
- API: http://localhost:8000/docs
- Streamlit: http://localhost:8501
- Grafana: http://localhost:3000  (admin / admin)

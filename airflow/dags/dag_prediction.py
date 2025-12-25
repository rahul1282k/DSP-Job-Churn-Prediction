import os
import pandas as pd
import requests
from datetime import datetime
import json

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from sqlalchemy import create_engine, text

# ----------------------------
# FOLDERS
# ----------------------------
GOOD_DATA = "/opt/airflow/data/good_data"
PREDICTED_DIR = "/opt/airflow/data/predicted_files"
TRACK_FILE = "/opt/airflow/data/last_prediction_timestamp.txt"

os.makedirs(PREDICTED_DIR, exist_ok=True)

# ----------------------------
# CONFIG
# ----------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
)

API_URL = "http://job-supervised-final-api-1:8000/predict"

default_args = {"owner": "airflow"}


# ----------------------------
# LOAD LAST TIMESTAMP
# ----------------------------
def load_last_ts():
    if not os.path.exists(TRACK_FILE):
        with open(TRACK_FILE, "w") as f:
            f.write("0")
        return 0.0
    with open(TRACK_FILE, "r") as f:
        txt = f.read().strip() or "0"
        return float(txt)


# ----------------------------
# SAVE LAST TIMESTAMP
# ----------------------------
def save_last_ts(ts):
    with open(TRACK_FILE, "w") as f:
        f.write(str(ts))


with DAG(
    dag_id="job_prediction_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["prediction"],
) as dag:

    # ------------------------------------------------------------
    # check_for_new_data
    # ------------------------------------------------------------
    @task
    def check_for_new_data() -> dict:

        last_ts = load_last_ts()
        print(f"Last processed timestamp = {last_ts}")

        all_files = [
            f for f in os.listdir(GOOD_DATA)
            if f.endswith(".csv")
        ]

        if not all_files:
            raise AirflowSkipException("No CSV files present in good_data folder.")

        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            db_files = {
                row[0]
                for row in conn.execute(text("SELECT filename FROM predicted_files")).fetchall()
            }

        new_files = []
        latest_timestamp = last_ts

        for fname in all_files:
            fpath = os.path.join(GOOD_DATA, fname)
            mtime = os.path.getmtime(fpath)

            # Condition 1: file not in DB → new
            # Condition 2: file modified after last run → new
            if fname not in db_files or mtime > last_ts:
                new_files.append(fname)

            if mtime > latest_timestamp:
                latest_timestamp = mtime

        if not new_files:
            raise AirflowSkipException("No NEW files detected (timestamp + DB check).")

        print(f"New files for prediction = {new_files}")
        print(f"New latest timestamp = {latest_timestamp}")

        return {"files": new_files, "latest_ts": latest_timestamp}

    # ------------------------------------------------------------
    # make_predictions
    # ------------------------------------------------------------
    @task
    def make_predictions(info: dict):

        files = info["files"]
        latest_ts = info["latest_ts"]

        engine = create_engine(DATABASE_URL)
        results = []

        for fname in files:
            file_path = os.path.join(GOOD_DATA, fname)
            df = pd.read_csv(file_path)

            if "target" in df.columns:
                df_infer = df.drop(columns=["target"])
            else:
                df_infer = df

            payload = {
                "records": df_infer.to_dict(orient="records"),
                "source": "scheduled",
            }

            try:
                r = requests.post(API_URL, json=payload, timeout=60)
                r.raise_for_status()
                data = r.json()

                pred_list = data["predictions"]

                output_name = fname.replace(".csv", "_predicted.csv")
                output_path = os.path.join(PREDICTED_DIR, output_name)

                pd.DataFrame(pred_list).to_csv(output_path, index=False)

                print(f"✔ Saved prediction file: {output_path}")

                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO predicted_files(filename)
                            VALUES (:f)
                            ON CONFLICT DO NOTHING
                        """),
                        {"f": fname},
                    )

                results.append({"file": fname, "status": "ok", "output": output_name})

            except Exception as e:
                results.append({"file": fname, "status": "error", "error": str(e)})

        # update timestamp AFTER successful predictions
        save_last_ts(latest_ts)

        return results

    # ------------------------------------------------------------
    # DAG Flow
    # ------------------------------------------------------------
    info = check_for_new_data()
    make_predictions(info)

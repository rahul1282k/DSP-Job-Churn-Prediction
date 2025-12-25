import os
import random
import json
from datetime import datetime

import pandas as pd
import numpy as np
import great_expectations as ge
import requests
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# ---------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------
DATA_ROOT = "/opt/airflow/data"
RAW_DATA = f"{DATA_ROOT}/raw_data"
GOOD_DATA = f"{DATA_ROOT}/good_data"
BAD_DATA = f"{DATA_ROOT}/bad_data"
REPORTS_DIR = f"{DATA_ROOT}/reports"

os.makedirs(GOOD_DATA, exist_ok=True)
os.makedirs(BAD_DATA, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# ---------------------------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK_URL", "")

API_PUBLIC_URL = os.getenv("API_PUBLIC_URL", "http://localhost:8000")

default_args = {"owner": "airflow"}


# ---------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------
with DAG(
    dag_id="job_ingestion_pipeline",
    schedule="*/1 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion"],
) as dag:

    # -----------------------------------------------------------------
    # READ ONE RANDOM RAW FILE
    # -----------------------------------------------------------------
    @task
    def read_data() -> str:
        files = [f for f in os.listdir(RAW_DATA) if f.endswith(".csv")]
        if not files:
            raise AirflowSkipException("No raw data available.")
        selected = random.choice(files)
        return os.path.join(RAW_DATA, selected)

    # -----------------------------------------------------------------
    # VALIDATE DATA (manual + Great Expectations)
    # -----------------------------------------------------------------
    @task
    def validate_data(file_path: str) -> dict:

        df = pd.read_csv(file_path)
        total = len(df)

        # ---- numeric cleanup ----
        for col in ["city_development_index", "training_hours"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.replace([np.inf, -np.inf], np.nan)

        required_cols = [
            "gender", "city_development_index", "training_hours",
            "relevent_experience", "target"
        ]

        missing_cols = [c for c in required_cols if c not in df.columns]

        # --------------------- MANUAL INVALID MASK --------------------
        invalid = pd.Series(False, index=df.index)

        if missing_cols:
            invalid |= True
        else:
            invalid |= df["city_development_index"].isna()
            invalid |= df["training_hours"].isna()
            invalid |= df["city_development_index"].between(0, 1) == False
            invalid |= df["training_hours"] < 0
            invalid |= ~df["gender"].isin(["Male", "Female", "Other"])
            invalid |= ~df["relevent_experience"].isin(
                ["Has relevent experience", "No relevent experience"]
            )
            invalid |= ~df["target"].isin([0, 1])

        # ----------------------- GREAT EXPECTATIONS -------------------
        ge_df = ge.from_pandas(df)
        ge_results = {}

        # Numeric expectations
        if "city_development_index" in df.columns:
            ge_results["city_dev_between_0_1"] = ge_df.expect_column_values_to_be_between(
                "city_development_index", min_value=0.0, max_value=1.0
            )
            ge_results["city_dev_not_null"] = ge_df.expect_column_values_to_not_be_null(
                "city_development_index"
            )

        if "training_hours" in df.columns:
            ge_results["training_hours_non_negative"] = ge_df.expect_column_values_to_be_between(
                "training_hours", min_value=0, max_value=1000
            )
            ge_results["training_hours_not_null"] = ge_df.expect_column_values_to_not_be_null(
                "training_hours"
            )

        # Categorical expectations
        if "gender" in df.columns:
            ge_results["gender_in_set"] = ge_df.expect_column_values_to_be_in_set(
                "gender", ["Male", "Female", "Other"]
            )

        if "relevent_experience" in df.columns:
            ge_results["relevent_exp_in_set"] = ge_df.expect_column_values_to_be_in_set(
                "relevent_experience",
                ["Has relevent experience", "No relevent experience"],
            )

        if "target" in df.columns:
            ge_results["target_binary"] = ge_df.expect_column_values_to_be_in_set(
                "target", [0, 1]
            )

        # ------------------- GE SUMMARY -----------------------
        issues = {}
        for name, res in ge_results.items():
            unexpected = res.get("result", {}).get("unexpected_count", 0)
            if unexpected > 0:
                issues[name] = int(unexpected)

        # --------- GE UNEXPECTED INDICES MARK INVALID ROWS ----
        for name, res in ge_results.items():
            idx_list = res.get("result", {}).get("unexpected_index_list", [])
            if idx_list:
                invalid[idx_list] = True

        invalid_rows = int(invalid.sum())
        valid_rows = total - invalid_rows

        # ------------------- CRITICALITY (NEW LOGIC) -------------------
        invalid_pct = invalid_rows / total if total > 0 else 1

        if missing_cols:
            criticality = "high"

        elif invalid_pct >= 0.80:
            criticality = "high"

        elif invalid_pct >= 0.30:
            criticality = "medium"

        elif invalid_pct > 0:
            criticality = "low"

        else:
            criticality = "low"

        # --------------------- HTML REPORT ----------------------------
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        fname = os.path.basename(file_path)
        report_path = f"{REPORTS_DIR}/report_{fname}_{ts}.html"

        df_report = df.copy()
        df_report["row_status"] = invalid.map(lambda x: "INVALID" if x else "VALID")

        with open(report_path, "w") as f:
            f.write("<html><body>")
            f.write(f"<h2>Data Quality Report – {fname}</h2>")
            f.write(f"<p><b>Criticality:</b> {criticality}</p>")
            f.write(f"<p>Total Rows: {total}</p>")
            f.write(f"<p>Valid Rows: {valid_rows}</p>")
            f.write(f"<p>Invalid Rows: {invalid_rows}</p>")

            if missing_cols:
                f.write("<p><b>Missing columns:</b> " + ", ".join(missing_cols) + "</p>")

            f.write("<h3>Great Expectations Summary</h3>")
            if issues:
                f.write("<ul>")
                for k, v in issues.items():
                    f.write(f"<li>{k}: {v} unexpected values</li>")
                f.write("</ul>")
            else:
                f.write("<p>All Great Expectations checks passed ✓</p>")

            f.write("<h3>Data with Row Status</h3>")
            f.write(df_report.to_html())
            f.write("</body></html>")

        report_filename = os.path.basename(report_path)
        report_url = f"{API_PUBLIC_URL}/reports/{report_filename}"

        return {
            "file_path": file_path,
            "filename": fname,
            "total": total,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "criticality": criticality,
            "report": report_path,
            "report_url": report_url,
            "invalid_mask": invalid.tolist(),
            "missing_cols": missing_cols,
            "issues": issues,
        }

    # -----------------------------------------------------------------
    # SAVE STATS
    # -----------------------------------------------------------------
    @task
    def save_statistics(info: dict):

        engine = create_engine(DATABASE_URL)
        issues_json = json.dumps(info.get("issues", {}))

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO data_quality_stats
                    (file_name, total_rows, valid_rows, invalid_rows,
                     criticality, issues, report_path, created_at)
                    VALUES
                    (:fname, :total, :valid, :invalid,
                     :crit, CAST(:issues AS jsonb), :report, NOW())
                """),
                {
                    "fname": info["filename"],
                    "total": info["total"],
                    "valid": info["valid_rows"],
                    "invalid": info["invalid_rows"],
                    "crit": info["criticality"],
                    "issues": issues_json,
                    "report": info["report"],
                },
            )

        return "stats_saved"

    # -----------------------------------------------------------------
    # SEND ALERTS
    # -----------------------------------------------------------------
    @task
    def send_alerts(info: dict):

        if not TEAMS_WEBHOOK:
            return "NO_WEBHOOK_SET"

        issues = info.get("issues", {})

        if issues:
            summary_lines = [
                f"- {name}: {count} unexpected values"
                for name, count in issues.items()
            ]
            summary_text = "\n".join(summary_lines[:6])
        else:
            summary_text = "No Great Expectations issues (all checks passed)."

        message = (
            f"**Data Quality Alert**\n\n"
            f"**File:** {info['filename']}\n"
            f"**Criticality:** {info['criticality']}\n"
            f"**Valid Rows:** {info['valid_rows']}\n"
            f"**Invalid Rows:** {info['invalid_rows']}\n\n"
            f"**Summary of errors:**\n{summary_text}\n\n"
            f"**Report:** [Open Report]({info['report_url']})"
        )

        try:
            requests.post(TEAMS_WEBHOOK, json={"text": message}, timeout=10)
        except Exception:
            return "alert_failed"

        return "alert_sent"

    # -----------------------------------------------------------------
    # SPLIT GOOD/BAD DATA
    # -----------------------------------------------------------------
    @task
    def split_and_save(info: dict):

        df = pd.read_csv(info["file_path"])
        mask = pd.Series(info["invalid_mask"])

        good = df[~mask]
        bad = df[mask]

        fname = info["filename"]

        if len(bad) == 0:
            good.to_csv(f"{GOOD_DATA}/{fname}", index=False)
        elif len(good) == 0:
            bad.to_csv(f"{BAD_DATA}/{fname}", index=False)
        else:
            good.to_csv(f"{GOOD_DATA}/{fname}", index=False)
            bad.to_csv(f"{BAD_DATA}/BAD_{fname}", index=False)

        os.remove(info["file_path"])

        return "split_done"

    # -----------------------------------------------------------------
    # DAG FLOW
    # -----------------------------------------------------------------
    filepath = read_data()
    info = validate_data(filepath)

    stats = save_statistics(info)
    alerts = send_alerts(info)

    [stats, alerts] >> split_and_save(info)

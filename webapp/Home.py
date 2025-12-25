import streamlit as st
import pandas as pd
import numpy as np
import requests
import os

st.set_page_config(page_title="Job Target Prediction", layout="wide")

API = os.getenv("API_URL", "http://api:8000")

st.title("Prediction – Job Change")
st.markdown(
    "Fill in the fields below. The model predicts whether the person will "
    "**change job (1)** or **not (0)**."
)

# ----------------------------------------------------
# Load reference data (job.csv) to build dropdowns
# ----------------------------------------------------
REF_CSV = "job.csv"

ref_df = None
try:
    ref_df = pd.read_csv(REF_CSV)
except Exception:
    # App will still work, but will fall back to hard-coded options
    ref_df = None

def get_cat_options(col, fallback):
    """
    Get sorted unique values for a categorical column from job.csv.
    If job.csv is not available or empty for that col, use fallback.
    """
    if ref_df is not None and col in ref_df.columns:
        vals = sorted(ref_df[col].dropna().unique().tolist())
        if len(vals) > 0:
            return vals
    return fallback

def get_numeric_default(col, fallback):
    """
    Use median of column from job.csv as a default if available.
    """
    if ref_df is not None and col in ref_df.columns:
        try:
            val = float(ref_df[col].median())
            return val
        except Exception:
            return fallback
    return fallback

# ----------------------------------------------------
# Feature list (no target)
# ----------------------------------------------------
FEATURES = [
    "enrollee_id",
    "city",
    "city_development_index",
    "gender",
    "relevent_experience",
    "enrolled_university",
    "education_level",
    "major_discipline",
    "experience",
    "company_size",
    "company_type",
    "last_new_job",
    "training_hours",
]

NUMERIC_COLUMNS = ["enrollee_id", "city_development_index", "training_hours"]

# ----------------------------------------------------
# Categorical options from your job.csv
# (with sensible fallbacks)
# ----------------------------------------------------
GENDER_OPTIONS = get_cat_options(
    "gender",
    ["Male", "Female", "Other"]
)

RELEVANT_EXP_OPTIONS = get_cat_options(
    "relevent_experience",
    ["Has relevent experience", "No relevent experience"]
)

ENROLLED_UNI_OPTIONS = get_cat_options(
    "enrolled_university",
    ["no_enrollment", "Full time course", "Part time course"]
)

EDUCATION_LEVEL_OPTIONS = get_cat_options(
    "education_level",
    ["Primary School", "High School", "Graduate", "Masters", "Phd"]
)

MAJOR_DISCIPLINE_OPTIONS = get_cat_options(
    "major_discipline",
    ["STEM", "Business Degree", "Arts", "Humanities", "No Major", "Other"]
)

EXPERIENCE_OPTIONS = get_cat_options(
    "experience",
    ["<1", "1", "2", "3", "4", "5", "6", "7", "8", "9",
     "10", "11", "12", "13", "14", "15", "16", "17", "18",
     "19", "20", ">20"]
)

COMPANY_SIZE_OPTIONS = get_cat_options(
    "company_size",
    ["<10", "50-99", "100-500", "500-999", "1000-4999", "5000-9999", "10000+"]
)

COMPANY_TYPE_OPTIONS = get_cat_options(
    "company_type",
    ["Pvt Ltd", "Funded Startup", "Early Stage Startup", "Public Sector", "NGO", "Other"]
)

LAST_NEW_JOB_OPTIONS = get_cat_options(
    "last_new_job",
    ["never", "1", "2", "3", "4", ">4"]
)

CITY_OPTIONS = get_cat_options("city", [])

CATEGORICAL_MAP = {
    "gender": GENDER_OPTIONS,
    "relevent_experience": RELEVANT_EXP_OPTIONS,
    "enrolled_university": ENROLLED_UNI_OPTIONS,
    "education_level": EDUCATION_LEVEL_OPTIONS,
    "major_discipline": MAJOR_DISCIPLINE_OPTIONS,
    "experience": EXPERIENCE_OPTIONS,
    "company_size": COMPANY_SIZE_OPTIONS,
    "company_type": COMPANY_TYPE_OPTIONS,
    "last_new_job": LAST_NEW_JOB_OPTIONS,
    "city": CITY_OPTIONS,
}

# ----------------------------------------------------
# UI – input form
# ----------------------------------------------------
inputs = {}
st.subheader("Input features")

col1, col2 = st.columns(2)

for i, feature in enumerate(FEATURES):
    with (col1 if i % 2 == 0 else col2):
        # Numeric inputs
        if feature in NUMERIC_COLUMNS:
            if feature == "enrollee_id":
                default = int(get_numeric_default("enrollee_id", 10000))
                inputs[feature] = st.number_input(
                    "enrollee_id",
                    min_value=1,
                    step=1,
                    value=default,
                )
            elif feature == "city_development_index":
                default = float(get_numeric_default("city_development_index", 0.8))
                inputs[feature] = st.number_input(
                    "city_development_index",
                    min_value=0.0,
                    max_value=1.0,
                    step=0.01,
                    format="%.2f",
                    value=default,
                )
            elif feature == "training_hours":
                default = int(get_numeric_default("training_hours", 40))
                inputs[feature] = st.number_input(
                    "training_hours",
                    min_value=0,
                    step=1,
                    value=default,
                )

        # Categorical dropdowns
        elif feature in CATEGORICAL_MAP:
            options = CATEGORICAL_MAP[feature]
            if options:
                inputs[feature] = st.selectbox(feature, options)
            else:
                # If no options found, fallback to free text
                inputs[feature] = st.text_input(feature, value="")

        # Fallback
        else:
            inputs[feature] = st.text_input(feature, value="")

st.markdown("---")

# ----------------------------------------------------
# Predict button
# ----------------------------------------------------
if st.button("Predict"):
    df_input = pd.DataFrame([inputs])

    # Clean up data for JSON
    df_input = df_input.replace([np.inf, -np.inf], np.nan)
    df_input = df_input.where(pd.notnull(df_input), None)

    try:
        resp = requests.post(
            f"{API}/predict",
            json={"records": df_input.to_dict(orient="records"), "source": "webapp"},
            timeout=60,
        )
        resp.raise_for_status()
        out = resp.json()

        # We assume API returns:
        # {
        #   "predictions": [
        #       {"prediction": 0/1, "probability": float, ...}
        #   ],
        #   "model_version": "v1",
        #   "n": 1
        # }
        pred_obj = out["predictions"][0]
        pred_value = int(pred_obj.get("prediction", 0))  # 0 or 1
        prob = pred_obj.get("probability", None)
        version = out.get("model_version", "unknown")

        st.success("Prediction received")
        # Main output: 0 or 1
        st.metric("Prediction (0 = Stay, 1 = Job Change)", pred_value)

        # Optional probability display if present
        if prob is not None:
            st.caption(f"Estimated probability of job change: {prob * 100:.2f}%")

        st.caption(f"Model version: {version}")

        # Show the row that was sent
        st.markdown("### Input used for this prediction")
        st.dataframe(df_input)

    except Exception as e:
        st.error(f"API error: {e}")

st.markdown("---")
st.info("Use the other pages for batch predictions and past predictions.")

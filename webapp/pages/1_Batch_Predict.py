import streamlit as st
import pandas as pd
import requests
import os
import numpy as np

API = os.getenv("API_URL", "http://api:8000")

st.header("Batch Prediction – Job Change")

st.markdown(
    "Upload a CSV file containing the **same feature columns as job.csv**, "
    "without the target column (or with target – it will be ignored)."
)

file = st.file_uploader("Upload CSV file", type=["csv"])

if st.button("Predict batch") and file is not None:
    try:
        df = pd.read_csv(file)

        # Drop target if present (training label)
        df_features = df.drop(columns=["target"], errors="ignore")

        # Clean data
        df_features = df_features.replace([np.inf, -np.inf], np.nan)
        df_features = df_features.where(pd.notnull(df_features), None)

        response = requests.post(
            f"{API}/predict",
            json={"records": df_features.to_dict(orient="records"), "source": "webapp"},
            timeout=300,
        )
        response.raise_for_status()
        out = response.json()

        if "predictions" in out:
            pred_df = pd.DataFrame(out["predictions"])
            # Join original df (including target if present) + predictions
            result = pd.concat([df.reset_index(drop=True), pred_df], axis=1)

            st.success("Batch predictions received")
            st.dataframe(result)
        else:
            st.error("API response does not contain 'predictions' key.")
            st.write(out)

    except Exception as e:
        st.error(f"Error during batch prediction: {e}")

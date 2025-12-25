import streamlit as st
import pandas as pd
import requests
import os
import json

API = os.getenv("API_URL", "http://api:8000")

st.header("Past Predictions")

st.markdown("""
Use this page to explore predictions stored by the system.

- **webapp**: predictions made via this Streamlit app  
- **scheduled**: predictions made by the automated prediction job  
- **all**: both sources  
""")

# -----------------------------
# Input filters
# -----------------------------

col1, col2, col3 = st.columns(3)

start = col1.date_input("Start date", None)
end = col2.date_input("End date", None)
source = col3.selectbox("Prediction source", ["all", "webapp", "scheduled"], index=0)

# -----------------------------
# Load past predictions
# -----------------------------
if st.button("Load"):

    params = {}

    if start:
        params["start"] = start.strftime("%Y-%m-%d")
    if end:
        params["end"] = end.strftime("%Y-%m-%d")
    if source:
        params["source"] = source

    try:
        response = requests.get(f"{API}/past-predictions", params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        rows = data.get("rows", [])

        if not rows:
            st.info("No predictions found for the selected filters.")
        else:
            # Convert to DataFrame
            df = pd.DataFrame(rows)

            # -------------------------
            # Expand JSON columns safely
            # -------------------------

            # Expand features column into separate columns
            df_features = df["features"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
            df_features = pd.json_normalize(df_features)

            # Expand prediction JSON
            df_pred = df["prediction"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
            df_pred = pd.json_normalize(df_pred)

            # Combine everything
            df_clean = pd.concat(
                [
                    df.drop(columns=["features", "prediction"]),
                    df_features.add_prefix("feat_"),
                    df_pred.add_prefix("pred_"),
                ],
                axis=1
            )

            st.success("Predictions loaded successfully.")
            st.dataframe(df_clean)

    except Exception as e:
        st.error(f"Error loading predictions: {e}")

import argparse
import os
import random
import numpy as np
import pandas as pd

ERROR_RATE = 0.15

def inject_errors_job_dataset(df_chunk):
    """
    Inject realistic bad rows for the JOB dataset.
    Bad rows CAN contain NaN / inf / wrong types.
    Good rows remain clean.
    """
    df = df_chunk.copy()
    n = len(df)

    if n == 0:
        return df

    rows_to_corrupt = random.sample(range(n), max(1, int(n * ERROR_RATE)))

    for idx in rows_to_corrupt:
        error_type = random.choice([
            "nan_value",
            "inf_value",
            "bad_gender",
            "bad_experience",
            "negative_hours",
            "string_in_numeric",
            "huge_outlier",
        ])

        # ERROR 1 — Introduce NaN in critical numeric fields
        if error_type == "nan_value":
            col = random.choice(["city_development_index", "training_hours"])
            if col in df.columns:
                df.loc[idx, col] = np.nan

        # ERROR 2 — Introduce INF
        elif error_type == "inf_value":
            col = random.choice(["city_development_index", "training_hours"])
            if col in df.columns:
                df.loc[idx, col] = np.inf

        # ERROR 3 — Invalid gender
        elif error_type == "bad_gender":
            if "gender" in df.columns:
                df.loc[idx, "gender"] = "UnknownGender"

        # ERROR 4 — Invalid relevant experience category
        elif error_type == "bad_experience":
            if "relevent_experience" in df.columns:
                df.loc[idx, "relevent_experience"] = "Experience??"

        # ERROR 5 — Negative value where impossible
        elif error_type == "negative_hours":
            if "training_hours" in df.columns:
                df.loc[idx, "training_hours"] = -10

        # ERROR 6 — String injected in a numeric column
        elif error_type == "string_in_numeric":
            if "training_hours" in df.columns:
                df.loc[idx, "training_hours"] = "wrong_value"

        # ERROR 7 — Extreme unrealistic numeric value
        elif error_type == "huge_outlier":
            if "city_development_index" in df.columns:
                df.loc[idx, "city_development_index"] = 1000

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-path", required=True)
    parser.add_argument("--raw-data-dir", required=True)
    parser.add_argument("--n-files", type=int, required=True)
    args = parser.parse_args()

    os.makedirs(args.raw_data_dir, exist_ok=True)

    # Load original dataset
    df = pd.read_csv(args.dataset_path)

    # 🔵 GOOD ROW SANITIZATION
    # Remove INF / -INF
    df = df.replace([np.inf, -np.inf], np.nan)

    # Drop rows with critical missing values
    df = df.dropna(subset=["gender", "training_hours", "city_development_index"])

    # Standardize gender
    df["gender"] = df["gender"].replace({
        "male": "Male",
        "female": "Female",
        "other": "Other"
    })

    # Cast numerics properly
    df["city_development_index"] = pd.to_numeric(df["city_development_index"], errors="coerce")
    df["training_hours"] = pd.to_numeric(df["training_hours"], errors="coerce")

    # Drop any rows that still contain NaN after cleaning
    df = df.dropna()

    # Shuffle
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    # Split dataset
    chunks = np.array_split(df, args.n_files)

    # Create corrupted raw files
    for i, chunk in enumerate(chunks, start=1):
        corrupted = inject_errors_job_dataset(chunk)

        filename = f"job_part_{i:04d}.csv"
        output_path = os.path.join(args.raw_data_dir, filename)

        corrupted.to_csv(output_path, index=False)
        print(f"[OK] Created raw file with mixed good+bad rows: {output_path}")


if __name__ == "__main__":
    main()

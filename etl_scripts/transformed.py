import pandas as pd
import logging
import os
from datetime import datetime

# --- CONFIG ---
# NOTE: Replace this path with the actual full path of your RAW CSV file
RAW_CSV_PATH = "D:/CDE Learnings Journey/SMIT CDE LEARNING B2 2025/Project 08 - SP500 Stock Data Orchestration using Airflow/sp500_raw_20251118_231424.csv" 
# --------------

logging.basicConfig(level=logging.INFO)

def _now_ts():
    """Generates a timestamp string for file naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _write_csv(df: pd.DataFrame, prefix: str) -> str:
    """Writes a DataFrame to a CSV file in the current directory."""
    filename = f"{prefix}_{_now_ts()}.csv"
    path = os.path.join(os.getcwd(), filename)
    df.to_csv(path, index=False)
    logging.info("Transformed CSV written: %s", path)
    return path

def transform_data(raw_path: str):
    """
    Reads raw stock data, transforms it, and saves the output.
    """
    if not os.path.exists(raw_path):
        logging.error(f"Raw CSV not found: {raw_path}")
        return None

    df = pd.read_csv(raw_path)
    logging.info("Raw CSV shape: %s | columns: %s", df.shape, df.columns.tolist())

    # 1. Rename Datetime → Date
    df = df.rename(columns={"Datetime": "Date"})
    df = df.sort_values(["Symbol", "Date"]).reset_index(drop=True)

    # 2. Calculate % change and absolute change, grouped by Symbol
    df["close_change"] = df.groupby("Symbol")["Close"].diff().fillna(0.0)
    df["close_pct_change"] = df.groupby("Symbol")["Close"].pct_change().fillna(0.0) * 100

    # 3. Force numeric (Good practice before loading)
    for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume", "close_change", "close_pct_change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 4. Select final columns
    final_cols = [
        "Date", "Symbol", "Open", "High", "Low", "Close",
        "Adj Close", "Volume", "close_change", "close_pct_change"
    ]
    final_df = df[final_cols].copy()

    # 5. RENAME TO MATCH SNOWFLAKE (UPPERCASE)
    rename_to_uppercase = {
        "Date": "DATE",
        "Symbol": "SYMBOL",
        "Open": "OPEN",
        "High": "HIGH",
        "Low": "LOW",
        "Close": "CLOSE",
        "Adj Close": "ADJ_CLOSE",
        "Volume": "VOLUME"
    }
    final_df.rename(columns=rename_to_uppercase, inplace=True)
    
    logging.info("Transformed shape: %s | columns: %s", final_df.shape, final_df.columns.tolist())
    
    # 6. Save Transformed Data
    transformed_path = _write_csv(final_df, "sp500_transformed")
    return transformed_path

if __name__ == "__main__":
    if RAW_CSV_PATH == "D:/CDE Learnings Journey/SMIT CDE LEARNING B2 2025/Project 08 - SP500 Stock Data Orchestration using Airflow/sp500_raw_20251118_231424.csv":
        logging.warning("Please update the RAW_CSV_PATH variable with your actual file path before running.")
    
    transformed_file = transform_data(RAW_CSV_PATH)
    if transformed_file:
        logging.info(f"Milestone 4 Complete: Transformed data saved to {transformed_file}")
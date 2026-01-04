"""
DAG: sp500_yfinance_to_s3_to_snowflake
Daily ETL: Fetch S&P500 data → Transform → Upload to S3 → Load into Snowflake
"""

import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from pendulum import yesterday

# ============================================================================
# CONSTANTS & CONFIG
# ============================================================================

S3_BUCKET          = "sp500-data-lake-project"
S3_PREFIX          = "stock/"
AWS_CONN_ID        = "AKIAUOORZQY22JRXHAPM"
SNOWFLAKE_CONN_ID  = "snowflake_conn"
SNOWFLAKE_TABLE    = "AIRFLOW_DATA.airflow.STOCK_DATA"
SNOWFLAKE_STAGE    = "STOCKS_STAGE"
DATA_DIR           = "/opt/airflow/data"

default_args = {
    "owner": "affan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _write_csv(df: pd.DataFrame, prefix: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = f"{prefix}_{_now_ts()}.csv"
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False)
    logging.info("CSV written: %s", path)
    return path

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def fetch_yfinance_data(**context):
    """Pull current-day data for all S&P500 tickers."""
    logging.info("Fetching S&P 500 tickers from Wikipedia…")

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch Wikipedia: {e}")

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    df = pd.read_html(StringIO(str(table)))[0]

    ticker_col = "Symbol" if "Symbol" in df.columns else "Ticker"
    symbols = df[ticker_col].astype(str).str.replace(".", "-", regex=False).tolist()

    rows = []
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="1d", auto_adjust=False)
            if not hist.empty:
                r = hist.iloc[-1]
                rows.append({
                    "Datetime": r.name.tz_localize(None).strftime("%Y-%m-%d"),
                    "Symbol": sym,
                    "Open": r["Open"],
                    "High": r["High"],
                    "Low": r["Low"],
                    "Close": r["Close"],
                    "Adj Close": r.get("Adj Close", r["Close"]),
                    "Volume": int(r["Volume"]),
                })
        except Exception as exc:
            logging.warning("Failed %s: %s", sym, exc)

    df_data = pd.DataFrame(rows)
    raw_path = _write_csv(df_data, "sp500_raw")

    context["ti"].xcom_push(key="raw_csv_path", value=raw_path)
    logging.info("Fetched %d symbols → %s", len(df_data), raw_path)
    return raw_path


def transform_data(**context):
    """Clean and transform data."""
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="fetch_yfinance_data", key="raw_csv_path")

    if not raw_path or not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV missing: {raw_path}")

    df = pd.read_csv(raw_path)
    df = df.rename(columns={"Datetime": "Date"})
    df = df.sort_values(["Symbol", "Date"]).reset_index(drop=True)

    df["close_change"] = df.groupby("Symbol")["Close"].diff().fillna(0.0)
    df["close_pct_change"] = df.groupby("Symbol")["Close"].pct_change().fillna(0.0) * 100
    df["DAILY_RANGE"] = df["High"] - df["Low"]
    df["DAILY_RANGE_PCT"] = (df["DAILY_RANGE"] / df["Close"]) * 100

    rename_map = {
        "Date": "DATE", "Symbol": "SYMBOL", "Open": "OPEN", "High": "HIGH",
        "Low": "LOW", "Close": "CLOSE", "Adj Close": "ADJ_CLOSE", "Volume": "VOLUME",
        "close_change": "CLOSE_CHANGE", "close_pct_change": "CLOSE_PCT_CHANGE",
        "DAILY_RANGE": "DAILY_RANGE", "DAILY_RANGE_PCT": "DAILY_RANGE_PCT",
    }

    df_final = df.rename(columns=rename_map)
    transformed_path = _write_csv(df_final, "sp500_transformed")

    ti.xcom_push(key="transformed_csv_path", value=transformed_path)
    logging.info("Transformed data saved: %s", transformed_path)
    return transformed_path


def upload_transformed_to_s3(**context):
    """Upload transformed CSV to S3."""
    ti = context["ti"]
    local_path = ti.xcom_pull(task_ids="transform_data", key="transformed_csv_path")

    if not local_path or not os.path.exists(local_path):
        raise FileNotFoundError(f"Missing transformed CSV: {local_path}")

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_key = f"{S3_PREFIX}transformed/sp500_transformed_{_now_ts()}.csv"

    s3_hook.load_file(filename=local_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)

    s3_url = f"s3://{S3_BUCKET}/{s3_key}"
    ti.xcom_push(key="s3_key", value=s3_key)
    logging.info("Uploaded to S3: %s", s3_url)
    return s3_url

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="sp500_yfinance_to_s3_to_snowflake",
    description="Daily S&P 500 → S3 → Snowflake ETL",
    default_args=default_args,
    schedule="@daily",
    start_date=yesterday(),
    catchup=False,
    tags=["finance", "etl", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch = PythonOperator(
        task_id="fetch_yfinance_data",
        python_callable=fetch_yfinance_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    upload = PythonOperator(
        task_id="upload_transformed_to_s3",
        python_callable=upload_transformed_to_s3,
    )

    load = SQLExecuteQueryOperator(
        task_id="load_to_snowflake",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        COPY INTO {{ params.table }} (
            DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE,
            VOLUME, CLOSE_CHANGE, CLOSE_PCT_CHANGE, DAILY_RANGE, DAILY_RANGE_PCT
        )
        FROM '@{{ params.stage }}/{{ ti.xcom_pull(task_ids="upload_transformed_to_s3", key="s3_key") }}'
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '', '\\N')
            TRIM_SPACE = TRUE
        )
        ON_ERROR = 'CONTINUE';
        """,
        params={"table": SNOWFLAKE_TABLE, "stage": SNOWFLAKE_STAGE},
    )

    end = EmptyOperator(task_id="end")

    start >> fetch >> transform >> upload >> load >> end
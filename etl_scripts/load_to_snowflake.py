import pandas as pd
import logging
import os
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# --- CONFIG (UPDATE THESE VALUES) ---
SNOWFLAKE_USER = "shaheerjamal09"       # <-- REQUIRED
SNOWFLAKE_PASSWORD = "##ShaheerJamalSnowflake9900##" # <-- REQUIRED
SNOWFLAKE_ACCOUNT = "SALXQMO-AXB32967" # e.g., 'xyz12345.us-east-1' <-- REQUIRED
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"           # Your default warehouse
SNOWFLAKE_DATABASE = "SP500"             
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_TABLE = "MARKET_DB"               # Target table name

# NOTE: Replace this path with the actual full path of your TRANSFORMED CSV file
TRANSFORMED_CSV_PATH = "D:/CDE Learnings Journey/SMIT CDE LEARNING B2 2025/Project 08 - SP500 Stock Data Orchestration using Airflow/sp500_transformed_20251119_000030.csv" 
# ------------------------------------

logging.basicConfig(level=logging.INFO)

def load_to_snowflake(transformed_path: str):
    """
    Reads transformed data and loads it into Snowflake using the write_pandas utility.
    """
    if not os.path.exists(transformed_path):
        logging.error(f"Transformed CSV not found: {transformed_path}")
        return

    # 1. Read Transformed Data
    df = pd.read_csv(transformed_path)
    logging.info("Loading DataFrame shape: %s | columns: %s", df.shape, df.columns.tolist())

    try:
        # 2. Establish Connection
        conn = connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        logging.info("Snowflake connection successful. Starting data load...")
        
        # 3. Execute Load using write_pandas
        # This handles staging and copying data efficiently
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            df, 
            table_name=SNOWFLAKE_TABLE, 
            database=SNOWFLAKE_DATABASE, 
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=True, # Important if your table names/columns are quoted or uppercase
            overwrite=False          # Set to True if you want to replace the table content
        )
        
        conn.close()
        
        logging.info(f"Loaded {nrows} rows successfully into {SNOWFLAKE_TABLE}.")
        logging.info(f"Total chunks uploaded: {nchunks}")
        
    except Exception as e:
        logging.error(f"Failed to connect to or load data into Snowflake: {e}")

if __name__ == "__main__":
    if TRANSFORMED_CSV_PATH.startswith("D:"):
        # You should also ensure the DDL (CREATE TABLE) is run in Snowflake before this step!
        load_to_snowflake(TRANSFORMED_CSV_PATH)
    else:
        logging.warning("Please update the TRANSFORMED_CSV_PATH and Snowflake credentials before running.")
import pandas as pd
import requests
import logging
import io
import yfinance as yf # NEW: Yahoo Finance library
from datetime import datetime
import os # NEW: File system operations

# ==================== HELPERS ====================
def _now_ts():
    """Generates a timestamp string for file naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _write_csv(df: pd.DataFrame, prefix: str) -> str:
    """Writes a DataFrame to a CSV file in the current directory."""
    filename = f"{prefix}_{_now_ts()}.csv"
    # Save directly to the current working directory for local testing
    path = os.path.join(os.getcwd(), filename)
    df.to_csv(path, index=False)
    logging.info("Raw CSV written: %s", path)
    return path
# ===============================================

logging.basicConfig(level=logging.INFO)

def fetch_and_save_raw_data():
    logging.info("Starting S&P 500 Data Pipeline: Extract & Fetch...")
    
    # ------------------ 1. EXTRACT SYMBOLS (Milestone 1 Success) ------------------
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        all_tables = pd.read_html(io.StringIO(response.text))
        
        sp500_df = None
        for df in all_tables:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            df.columns = df.columns.astype(str).str.strip().str.title() 
            
            if 'Symbol' in df.columns:
                sp500_df = df
                break
        
        if sp500_df is None:
            logging.error("Could not find the S&P 500 companies table.")
            return

        symbols = sp500_df['Symbol'].astype(str).str.replace(".", "-", regex=False).tolist()
        logging.info(f"Found {len(symbols)} symbols from Wikipedia.")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch the Wikipedia page: {e}")
        return
    
    # ------------------ 2. FETCH STOCK DATA (Milestone 2 Core) ------------------
    data = []
    failed_symbols = []
    
    for i, symbol in enumerate(symbols):
        if i % 50 == 0:
             logging.info(f"Processing symbol {i+1}/{len(symbols)}: {symbol}")
        
        try:
            ticker = yf.Ticker(symbol)
            # Fetch only the latest trading day's data
            hist = ticker.history(period="1d", auto_adjust=True)
            
            if not hist.empty:
                row = hist.iloc[0]
                data.append({
                    "Datetime": row.name.strftime("%Y-%m-%d"),
                    "Symbol": symbol,
                    "Open": row["Open"],
                    "High": row["High"],
                    "Low": row["Low"],
                    "Close": row["Close"],
                    # 'Adj Close' is usually the same as 'Close' when auto_adjust=True
                    "Adj Close": row["Close"], 
                    "Volume": int(row["Volume"]),
                })
        except Exception as e:
            logging.warning(f"Failed to fetch data for {symbol}: {e}")
            failed_symbols.append(symbol)

    df = pd.DataFrame(data)
    logging.info(f"Successfully fetched and consolidated data for {len(df)} companies.")

    # ------------------ 3. SAVE RAW DATA (Milestone 2 Completion) ------------------
    if not df.empty:
        raw_path = _write_csv(df, "sp500_raw")
        logging.info(f"Milestone 2 Complete: Raw data saved to {raw_path}")
    else:
        logging.error("DataFrame is empty. No data saved.")
        
    if failed_symbols:
        logging.info(f"Note: Failed to fetch for {len(failed_symbols)} symbols: {failed_symbols[:5]}...")


if __name__ == "__main__":
    fetch_and_save_raw_data()
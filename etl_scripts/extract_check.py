import pandas as pd
import requests
import logging
import io 

logging.basicConfig(level=logging.INFO)

def check_wikipedia_extraction():
    logging.info("Attempting to extract S&P 500 table from Wikipedia...")
    
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        
        all_tables = pd.read_html(io.StringIO(response.text))
        
        sp500_df = None
        symbol_column_name = None
        
        # 1. Iterate through tables to find the one with the 'Symbol' column
        for df in all_tables:
            # --- FIX: Ensure columns are flat strings before stripping/titling ---
            
            # 1a. Flatten MultiIndex if it exists (e.g., if headers are nested)
            if isinstance(df.columns, pd.MultiIndex):
                # Join the levels of the MultiIndex columns into a single string
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # 1b. Convert columns to string and then clean them
            # We use .astype(str) to prevent the error if pandas interpreted a column name as non-string
            df.columns = df.columns.astype(str).str.strip().str.title() 
            # --------------------------------------------------------------------
            
            # 1c. Check if a 'Symbol' column exists
            if 'Symbol' in df.columns:
                sp500_df = df
                symbol_column_name = 'Symbol'
                break
        
        if sp500_df is None:
            logging.error("Could not find the S&P 500 companies table containing a 'Symbol' column.")
            return

        # 2. Extract and format symbols
        symbols = sp500_df[symbol_column_name].astype(str).str.replace(".", "-", regex=False).tolist()
        
        logging.info("Extraction successful! Table found and symbols extracted.")
        logging.info(f"Total symbols found: {len(symbols)}")
        
        print("\n--- Extracted DataFrame Head ---")
        print(sp500_df.head())
        print("\n--- First 10 Formatted Symbols ---")
        print(symbols[:10])
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch the page or read HTML: {e}")
        return

# Run the check
check_wikipedia_extraction()
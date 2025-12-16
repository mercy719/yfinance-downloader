import yfinance as yf
import pandas as pd
import os
import schedule
import time
from datetime import datetime
import pytz
import warnings

# Suppress warnings for cleaner logs
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

def download_and_append(tickers):
    print(f"[{datetime.now()}] Starting data download job...")
    for ticker in tickers:
        filename = f"{ticker}_1m_data.csv"
        print(f"Processing {ticker}...")
        
        try:
            # Check if file exists to determine logic
            file_exists = os.path.exists(filename)
            
            # Download recent data (last 5 days to be safe and cover weekends/holidays gaps)
            # 1m data is available for 7 days max.
            print(f"Downloading data for {ticker}...")
            new_data = yf.download(ticker, period="5d", interval="1m", progress=False)
            
            if new_data.empty:
                print(f"No data found for {ticker}")
                continue
                
            # Flatten columns if MultiIndex (common in new yfinance)
            if isinstance(new_data.columns, pd.MultiIndex):
                # new_data.columns is likely (Price, Ticker)
                # We want just Price. e.g. Close, High, Low...
                try:
                    new_data.columns = new_data.columns.get_level_values(0)
                except Exception:
                    pass

            if file_exists:
                print(f"Appending to existing {filename}...")
                # Read existing data
                try:
                    # Try reading with standard index
                    existing_data = pd.read_csv(filename, index_col=0, parse_dates=True)
                    
                    # If the existing file has the weird 3-line header from previous yfinance version/export,
                    # pandas read_csv might have messed up the index or headers.
                    # We will attempt to concat and let pandas handle index alignment.
                    
                    # Concatenate with new data
                    combined = pd.concat([existing_data, new_data])
                    
                    # Drop duplicates based on index (Datetime)
                    # We keep the last occurrence (newest data usually preferred if overlap with corrections, 
                    # though typically they are same).
                    combined = combined[~combined.index.duplicated(keep='last')]
                    
                    # Sort just in case
                    combined.sort_index(inplace=True)
                    
                    combined.to_csv(filename)
                    print(f"Successfully updated {filename}. Rows: {len(combined)}")
                except Exception as e:
                    print(f"Error reading/merging {filename}: {e}. Saving new data to {filename}.new as backup.")
                    new_data.to_csv(f"{filename}.new")
            else:
                print(f"Creating new {filename}...")
                new_data.to_csv(filename)
                
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

def job():
    print(f"[{datetime.now()}] Running scheduled job...")
    download_and_append(["QQQ", "SPY"])

if __name__ == "__main__":
    print("Dockerized Downloader Started.")
    
    # Run once immediately on startup to ensure we have data
    job()
    
    # Schedule to run daily. 
    # Market closes at 4 PM ET. 
    # To be safe (data availability), let's run at 22:00 UTC (5 PM EST / 6 PM EDT).
    run_time = "22:00"
    schedule.every().day.at(run_time).do(job)
    print(f"Scheduled to run daily at {run_time} UTC.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from .database import init_database
from .fetcher import fetch_and_upload
from .indicators import run_python_indicator_pipeline

if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("POLYGON_API_KEY")
    DB_URL = os.getenv("DB_URL")

    # --- Configuration ---
    RESET_DATABASE = False             # True = Wipes everything, re-downloads 2 years of data
    RECALCULATE_INDICATORS = True    # True = Re-runs the math on existing raw data 

    if not API_KEY or not DB_URL:
        print("Error: Missing env variables.")
        sys.exit(1)

    if RESET_DATABASE:
        print("\n=== STARTING 2-YEAR DATABASE RESET ===")
        init_database(DB_URL)

        end_date = datetime.today()
        start_date = end_date - timedelta(days=730)
        dates_to_fetch = pd.bdate_range(start=start_date, end=end_date)

        print(f"\n[PHASE 1] Fetching {len(dates_to_fetch)} days of raw market data...")
        for date_obj in dates_to_fetch:
            target_date = date_obj.strftime("%Y-%m-%d")
            print(f"\n--- Processing Raw Data: {target_date} ---")
            fetch_and_upload(target_date, DB_URL, API_KEY)
            print("Sleeping for 13 seconds to avoid rate limits...")
            time.sleep(13)

        print("\n[PHASE 2] Bulk calculating all indicators...")
        run_python_indicator_pipeline(DB_URL, target_date=None)

        print("\n=== RESET COMPLETE ===")
        
    elif RECALCULATE_INDICATORS:
        print("\n=== RECALCULATING ALL INDICATORS FROM EXISTING RAW DATA ===")
        # Because target_date is None, this will automatically truncate the 
        # daily_indicators table before bulk-inserting the new calculations.
        run_python_indicator_pipeline(DB_URL, target_date=None)
        print("\n=== RECALCULATION COMPLETE ===")

    else:
        # Standard Daily Run
        TARGET_DATE = datetime.today().strftime("%Y-%m-%d")
        print(f"\n=== RUNNING DAILY update FOR {TARGET_DATE} ===")

        fetch_and_upload(TARGET_DATE, DB_URL, API_KEY)
        run_python_indicator_pipeline(DB_URL, target_date=TARGET_DATE)

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


def init_database(db_url):
    """
    Executes a SQL file to initialize the database schema.
    WARNING: If using init_schema.sql, this will DROP the existing table.
    """
    sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "init_schema.sql")

    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    try:
        engine = create_engine(db_url)
        with engine.begin() as conn:
            with open(sql_file_path, "r") as file:
                sql_script = file.read()
                # Execute the SQL script
                conn.execute(text(sql_script))
        print(f"Successfully initialized database schema using {sql_file_path}")
    except Exception as e:
        print(f"Database Initialization Error: {e}")


def get_entire_market_ohlcv(date, api_key):
    """
    Fetches daily OHLCV for the entire US stock market for a specific date.
    Returns the raw unfiltered data containing thousands of tickers.
    """
    client = RESTClient(api_key)

    try:
        print(f"Fetching entire market data from Polygon for {date}...")
        all_market_data = client.get_grouped_daily_aggs(date)

        if not all_market_data:
            return None

        print(
            f"\n--- Successfully pulled {len(all_market_data)} tickers for {date} ---"
        )

        # Convert Polygon Agg objects into a list of dictionaries
        data_dicts = []
        for agg in all_market_data:
            data_dicts.append(
                {
                    "ticker": getattr(agg, "ticker", None),
                    "open": getattr(agg, "open", None),
                    "high": getattr(agg, "high", None),
                    "low": getattr(agg, "low", None),
                    "close": getattr(agg, "close", None),
                    "volume": getattr(agg, "volume", None),
                    "vwap": getattr(agg, "vwap", None),
                    "timestamp": getattr(agg, "timestamp", None),
                    "transactions": getattr(agg, "transactions", None),
                }
            )

        # Create and return the DataFrame
        df = pd.DataFrame(data_dicts)

        # Convert timestamp from unix milliseconds to datetime
        if "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

        return df

    except Exception as e:
        print(f"Client Error: {e}")
        return None


def upload_to_postgres(df, table_name, db_url):
    """
    Uploads a pandas DataFrame to a PostgreSQL database.
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(db_url)

        print(f"\nUploading {len(df)} rows to PostgreSQL table '{table_name}'...")

        # We strictly use 'append' now, assuming the table was properly created
        # via the init_schema.sql script.
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

        print("Upload successful!")

    except IntegrityError:
        print("\nUpload Failed: Duplicate entries detected.")
        print("You have already uploaded data for this date and ticker combination.")
        print("The Primary Key constraint successfully prevented duplicate rows.")
    except Exception as e:
        print(f"\nDatabase Upload Error: {e}")


def run_elt_pipeline(target_date, db_url):
    """
    Executes the ELT SQL script to calculate indicators directly in the database.
    """
    sql_file_path = os.path.join(
        os.path.dirname(__file__), "sql", "calculate_indicators.sql"
    )

    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    try:
        print(f"\nRunning ELT pipeline to calculate indicators for {target_date}...")
        engine = create_engine(db_url)
        affected_rows = 0

        with engine.begin() as conn:
            with open(sql_file_path, "r") as file:
                sql_script = file.read()

                # Execute the SQL script, passing the target date to the bind parameter
                result = conn.execute(text(sql_script), {"target_date": target_date})
                # Extract the rowcount before the transaction commits/closes
                affected_rows = result.rowcount

        if affected_rows > 0:
            print(
                f"Successfully calculated and inserted/updated indicators for {affected_rows} tickers!"
            )
        else:
            print(
                "No new values were inserted or updated."
            )

    except Exception as e:
        print(f"ELT Pipeline Error: {e}")


def fetch_and_upload(target_date, db_url, api_key):
    # 1. Fetch Market Data
    entire_market_data = get_entire_market_ohlcv(target_date, api_key)

    if entire_market_data is not None and not entire_market_data.empty:
        # We explicitly add the trading day date as a column.
        entire_market_data["market_date"] = pd.to_datetime(target_date).date()

        print("\nPreview of DataFrame:")
        print(entire_market_data.head())
        print(f"\nDataFrame Shape: {entire_market_data.shape}")

        # 2. Upload Raw Market Data
        upload_to_postgres(
            df=entire_market_data, table_name="daily_market_data", db_url=db_url
        )

    else:
        print(f"No market data found for {target_date}.")


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("POLYGON_API_KEY")
    DB_URL = os.getenv("DB_URL")

    # --- Configuration ---
    RESET_DATABASE = False

    if not API_KEY:
        print("Error: 'POLYGON_API_KEY' not found.")
        print(
            "Please create a .env file and add: POLYGON_API_KEY=your_actual_api_key_here"
        )
        sys.exit(1)

    if not DB_URL:
        print("Error: 'DB_URL' not found.")
        print(
            "Please add to .env: DB_URL=postgresql://user:password@localhost:5432/your_db_name"
        )
        sys.exit(1)

    # 0. Initialize Database (Optional step for starting over)
    if RESET_DATABASE:
        print("\n--- Starting Database Reset ---")
        init_database(DB_URL)

        print("\n--- Database Reset Complete ---")
        print("\n--- Starting 1-Year Backfill ---")

        # Calculate start and end dates
        end_date = datetime.today()
        start_date = end_date - timedelta(days=365)

        # Generate a list of business days (Mon-Fri) to skip weekends
        dates_to_fetch = pd.bdate_range(start=start_date, end=end_date)

        print(
            f"\n--- Starting 1-Year Backfill from {start_date.date()} to {end_date.date()} ---"
        )
        print(f"Total potential trading days to process: {len(dates_to_fetch)}")

        for date_obj in dates_to_fetch:
            target_date = date_obj.strftime("%Y-%m-%d")
            print(f"\n=======================================================")
            print(f"Processing date: {target_date}")
            print(f"=======================================================")

            fetch_and_upload(target_date, DB_URL, API_KEY)
            run_elt_pipeline(target_date, DB_URL)

            print(f"Sleeping for 13 seconds to avoid rate limits...")
            time.sleep(13)

    else:
        # Standard daily run
        TARGET_DATE = datetime.today().strftime("%Y-%m-%d")
        TARGET_DATE = "2026-04-08"
        fetch_and_upload(TARGET_DATE, DB_URL, API_KEY)
        run_elt_pipeline(TARGET_DATE, DB_URL)

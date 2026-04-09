import os
import pandas as pd
from polygon import RESTClient
from dotenv import load_dotenv


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
            print(f"\nNo market data found for {date}.")
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


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("POLYGON_API_KEY")

    TARGET_DATE = "2025-04-07"

    if not API_KEY:
        print("Error: 'POLYGON_API_KEY' not found.")
        print(
            "Please create a .env file in the same directory and add: POLYGON_API_KEY=your_actual_api_key_here"
        )
    else:
        entire_market_data = get_entire_market_ohlcv(TARGET_DATE, API_KEY)

        if entire_market_data is not None and not entire_market_data.empty:
            print("\nPreview of DataFrame:")
            print(entire_market_data.head())
            print(f"\nDataFrame Shape: {entire_market_data.shape}")

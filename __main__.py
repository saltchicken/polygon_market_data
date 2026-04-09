import os
from polygon import RESTClient
from dotenv import load_dotenv


def get_multiple_ohlcv_with_client(tickers, date, api_key):
    """
    Fetches daily OHLCV for multiple tickers using the official polygon-api-client library.
    Uses the Grouped Daily endpoint to get data for all stocks in a single API call,
    which is highly efficient and avoids rate limits.
    """
    client = RESTClient(api_key)

    try:
        print(f"Fetching market data from Polygon for {date}...")
        # Make ONE call to get all stocks for the given date
        all_market_data = client.get_grouped_daily_aggs(date)

        # Filter the results locally for the specific stocks requested
        my_data = [stock for stock in all_market_data if stock.ticker in tickers]

        if not my_data:
            print(f"\nNo data found for the requested tickers on {date}.")
            return []

        print(f"\n--- OHLCV Data on {date} ---")
        for stock in my_data:
            print(f"\n[{stock.ticker}]")
            print(f"Open:   ${stock.open}")
            print(f"High:   ${stock.high}")
            print(f"Low:    ${stock.low}")
            print(f"Close:  ${stock.close}")
            print(f"Volume: {stock.volume:,}")

        return my_data

    except Exception as e:
        print(f"Client Error: {e}")
        return None


if __name__ == "__main__":
    # Load environment variables from the .env file
    load_dotenv()
    API_KEY = os.getenv("POLYGON_API_KEY")

    # Target Stocks and Date (Format MUST be YYYY-MM-DD)
    TARGET_TICKERS = ["AAPL", "MSFT", "TSLA"]
    TARGET_DATE = "2025-04-07"

    if not API_KEY:
        print("Error: 'POLYGON_API_KEY' not found.")
        print(
            "Please create a .env file in the same directory and add: POLYGON_API_KEY=your_actual_api_key_here"
        )
    else:
        get_multiple_ohlcv_with_client(TARGET_TICKERS, TARGET_DATE, API_KEY)

# Prerequisites:
# pip install requests polygon-api-client

import requests
from polygon import RESTClient


# ==========================================
# METHOD 1: Using the standard 'requests' library
# ==========================================
def get_ohlcv_with_requests(ticker, date, api_key):
    """
    Fetches daily OHLCV using the raw REST API endpoint.
    """
    # The Daily Open/Close Endpoint
    url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={api_key}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        print(f"\n--- [Requests Method] OHLCV for {ticker} on {date} ---")
        print(f"Open:   ${data.get('open')}")
        print(f"High:   ${data.get('high')}")
        print(f"Low:    ${data.get('low')}")
        print(f"Close:  ${data.get('close')}")
        print(f"Volume: {data.get('volume'):,}")

        return data

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        # A 404 error usually means the market was closed on that day (e.g., weekend/holiday)
        if (
            hasattr(e, "response")
            and e.response is not None
            and e.response.status_code == 404
        ):
            print(
                "Note: The market may have been closed on this date, or the ticker is invalid."
            )
        return None


# ==========================================
# METHOD 2: Using the Official Polygon Python Client
# ==========================================
def get_ohlcv_with_client(ticker, date, api_key):
    """
    Fetches daily OHLCV using the official polygon-api-client library.
    """
    client = RESTClient(api_key)

    try:
        # Calls the Daily Open/Close endpoint automatically
        data = client.get_daily_open_close_agg(ticker, date)

        print(f"\n--- [Polygon Client Method] OHLCV for {ticker} on {date} ---")
        print(f"Open:   ${data.open}")
        print(f"High:   ${data.high}")
        print(f"Low:    ${data.low}")
        print(f"Close:  ${data.close}")
        print(f"Volume: {data.volume:,}")

        return data

    except Exception as e:
        print(f"Client Error: {e}")
        return None


if __name__ == "__main__":
    # 1. Get your free API key at https://polygon.io/dashboard
    # 2. Replace 'YOUR_API_KEY' with your actual key
    API_KEY = "YOUR_API_KEY"

    # Target Stock and Date (Format MUST be YYYY-MM-DD)
    TARGET_TICKER = "AAPL"
    TARGET_DATE = "2024-04-05"

    if API_KEY == "YOUR_API_KEY":
        print("Please replace 'YOUR_API_KEY' with your actual Polygon.io API key.")
    else:
        # Run Method 1
        get_ohlcv_with_requests(TARGET_TICKER, TARGET_DATE, API_KEY)

        # Run Method 2
        get_ohlcv_with_client(TARGET_TICKER, TARGET_DATE, API_KEY)

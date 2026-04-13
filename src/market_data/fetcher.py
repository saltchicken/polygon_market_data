import pandas as pd
from polygon import RESTClient
from sqlalchemy import create_engine, text
from .database import upload_to_postgres


def get_entire_market_ohlcv(date, api_key):
    """Fetches daily OHLCV for the entire US stock market for a specific date."""
    client = RESTClient(api_key)

    try:
        all_market_data = client.get_grouped_daily_aggs(date)
        if not all_market_data:
            return None

        print(f"--- Successfully pulled {len(all_market_data)} tickers for {date} ---")

        data_dicts = [
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
            for agg in all_market_data
        ]

        df = pd.DataFrame(data_dicts)
        if "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df

    except Exception as e:
        print(f"Client Error: {e}")
        return None


def fetch_and_upload(target_date, db_url, api_key):
    entire_market_data = get_entire_market_ohlcv(target_date, api_key)
    if entire_market_data is not None and not entire_market_data.empty:
        entire_market_data["market_date"] = pd.to_datetime(target_date).date()

        # Clean up data: drop invalid rows and remove duplicate tickers
        # (Polygon sometimes returns multiple entries for the same ticker, which crashes COPY)
        entire_market_data = entire_market_data.dropna(subset=["ticker"])
        entire_market_data = entire_market_data.sort_values(
            "volume", ascending=False
        ).drop_duplicates(subset=["ticker"])

        # Ensure idempotency: Delete existing data for this date so re-runs don't fail
        # due to UniqueViolation constraints.
        try:
            engine = create_engine(db_url)
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM daily_market_data WHERE market_date = :dt"),
                    {"dt": target_date},
                )
        except Exception as e:
            print(f"Warning: Could not clear existing data for {target_date}: {e}")

        upload_to_postgres(
            df=entire_market_data, table_name="daily_market_data", db_url=db_url
        )
    else:
        print(f"No market data found for {target_date}.")

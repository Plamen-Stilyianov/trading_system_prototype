import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import httpx

logger = logging.getLogger("TradingEngine.DataLoader")


class HistoricalDataLoader:
    """
    Ingests and normalizes historical financial market datasets (OHLCV candles)
    from local storage systems (CSV files) or streaming cloud broker APIs.
    """

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None, use_paper: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key

        # Determine the broker API target endpoint base (Alpaca structure example)
        if use_paper:
            self.base_url = "https://alpaca.markets"
            self.data_url = "https://alpaca.markets"
        else:
            self.base_url = "https://alpaca.markets"
            self.data_url = "https://alpaca.markets"

    def load_from_csv(self, file_path: str) -> pd.DataFrame:
        """
        Loads and cleans a historical asset matrix from a local CSV file.
        Expected columns: timestamp/date, open, high, low, close, volume
        """
        logger.info(f"Attempting to read local data from track path: {file_path}")

        if not os.path.exists(file_path):
            logger.error(f"Data loading failed: Target file path does not exist: {file_path}")
            return pd.DataFrame()

        try:
            # Read and sanitize structural formats
            df = pd.read_csv(file_path)
            df.columns = [col.lower().strip() for col in df.columns]

            # Enforce proper indexing properties over date variables
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)
            elif "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)

            # Cast data values to precise numerical representations
            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df.dropna(subset=["close"], inplace=True)
            df.sort_index(inplace=True)

            logger.info(f"Successfully processed CSV file array. Loaded matrix rows count: {len(df)}")
            return df

        except Exception as e:
            logger.error(f"Fatal error parsing structured file contents into matrix frame: {str(e)}")
            return pd.DataFrame()

    async def fetch_from_broker(
            self, symbol: str, timeframe: str = "5Min", start_date: str = "2026-01-01", end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Asynchronously streams historical candle parameters down from the broker's cloud database.

        :param symbol: Target equity asset ticker string (e.g. 'AAPL')
        :param timeframe: Bar intervals format ('1Min', '5Min', '1Day')
        :param start_date: Start date string (YYYY-MM-DD)
        :param end_date: Optional end date string (defaults to current system time)
        """
        if not self.api_key or not self.secret_key:
            logger.error("API authentication credentials missing. Aborting cloud data request sequence.")
            return pd.DataFrame()

        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Initiating historical API query for {symbol} ({timeframe}) from {start_date} to {end_date}...")

        # Structure headers mapping perfectly to exchange authentication rules
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }

        # Structure standardized payload endpoint parameters
        params = {
            "start": f"{start_date}T00:00:00Z",
            "end": f"{end_date}T23:59:59Z",
            "timeframe": timeframe,
            "limit": 10000
        }

        endpoint = f"{self.data_url}/stocks/{symbol}/bars"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint, headers=headers, params=params, timeout=10.0)

                if response.status_code != 200:
                    logger.error(
                        f"Exchange network connection rejected: Status {response.status_code} | {response.text}")
                    return pd.DataFrame()

                payload = response.json()
                bars_list = payload.get("bars", [])

                if not bars_list:
                    logger.warning(f"Network request completed successfully, but zero data records exist for {symbol}.")
                    return pd.DataFrame()

                # Normalize raw JSON response structures straight into a Pandas matrix
                df = pd.DataFrame(bars_list)

                # Standardize column naming conventions to map to research/backtester.py
                df.rename(columns={
                    "t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"
                }, inplace=True)

                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)
                df = df[["open", "high", "low", "close", "volume"]]

                logger.info(f"Successfully loaded cloud data matrix from broker endpoint. Total records: {len(df)}")
                return df

        except Exception as e:
            logger.error(f"An unexpected error occurred during execution layer web streaming: {str(e)}")
            return pd.DataFrame()

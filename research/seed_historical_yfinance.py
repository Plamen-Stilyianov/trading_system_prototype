import os
import sqlite3
import logging
import yfinance as yf
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DataPipeline.SafeSeeder")

DB_PATH = "logs/trading_data.db"
TARGET_SYMBOL = "AAPL"


def populate_historical_if_empty(symbol: str):
    """
    Checks the database first. Extracts and seeds historical bars from Yahoo Finance
    ONLY if the historical_ticks table contains no records for the targeted symbol.
    """
    logger.info(f"🗄️ Verifying storage engine partition integrity at: {DB_PATH}")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. Structural table schema setup
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS historical_ticks
                       (
                           id         INTEGER PRIMARY KEY AUTOINCREMENT,
                           timestamp  TEXT UNIQUE,
                           symbol     TEXT,
                           last_price REAL,
                           volume     INTEGER
                       )
                       """)
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Structural schema validation failed: {str(e)}")
        return

    # 2. DATA EXISTENCE SAFETY CHECK
    try:
        cursor.execute("SELECT COUNT(*) FROM historical_ticks WHERE symbol = ?", (symbol,))
        row_count = cursor.fetchone()[0]

        if row_count > 0:
            logger.info(
                f"🛑 [SAFE STOP] Historical database already contains {row_count} data bars for '{symbol}'. Skipping download to protect existing records.")
            conn.close()
            return

        logger.info(f"🔍 Ledger is empty for '{symbol}'. Initiating data collection sequence...")
    except Exception as e:
        logger.error(f"❌ Failed to verify existing data rows: {str(e)}")
        conn.close()
        return

    # 3. Extract historical data from yfinance
    logger.info(f"📥 Requesting historical arrays from Yahoo Finance for '{symbol}' (60 Days @ 5m intervals)...")
    try:
        df = yf.download(tickers=symbol, period="60d", interval="5m", auto_adjust=True)
        if df.empty:
            logger.error(f"❌ yfinance returned an empty dataset.")
            conn.close()
            return
    except Exception as e:
        logger.error(f"❌ Network extraction layer encountered a failure: {str(e)}")
        conn.close()
        return

    # 4. Clean, flatten, and transform data streams
    try:
        # ─── FIXED: FLATTEN MULTIINDEX TUPLES BEFORE PARSING ───
        if isinstance(df.columns, pd.MultiIndex):
            # If the column header is a tuple like ('Close', 'AAPL'), extract just the string component 'Close'
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

        df = df.reset_index()

        # Isolate the index column keys regardless of case profile alterations
        time_col = next((col for col in df.columns if str(col).lower() in ["datetime", "date", "timestamp"]), None)
        close_col = next((col for col in df.columns if "close" in str(col).lower()), None)
        volume_col = next((col for col in df.columns if "volume" in str(col).lower()), None)

        if not time_col or not close_col or not volume_col:
            raise KeyError(f"Could not resolve key mappings. Discovered columns: {list(df.columns)}")

        insert_records = []
        for _, row in df.iterrows():
            # Convert pandas Timestamp objects cleanly to an ISO text string
            ts_str = str(row[time_col])
            price = float(row[close_col])
            vol = int(row[volume_col])
            insert_records.append((ts_str, symbol, price, vol))

    except Exception as e:
        logger.error(f"❌ Data payload processing failure: {str(e)}")
        conn.close()
        return

    # 5. Safe Bulk Insertion Block
    try:
        cursor.executemany("""
                           INSERT OR IGNORE INTO historical_ticks (timestamp, symbol, last_price, volume)
                           VALUES (?, ?, ?, ?)
                           """, insert_records)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM historical_ticks WHERE symbol = ?", (symbol,))
        total_rows = cursor.fetchone()[0]
        logger.info(f"🎉 Success! Database table populated with {total_rows} data bars for '{symbol}'.")
    except Exception as e:
        logger.error(f"❌ SQLite database write failure: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    # ─── 🚀 CLOUD-HARDENED DYNAMIC SEEDER PASSTHROUGH ───
    # Instead of hardcoding TARGET_SYMBOL = "AAPL", fetch your dynamic array rows matrix
    try:
        # Connect directly to your live state instance arrays or load the configuration JSON parameters
        tracked_basket = state_manager.tracked_symbols
    except Exception:
        # Reliable baseline fallback if executed entirely out of application context
        tracked_basket = ["AAPL", "SPY", "BTC-USD"]

    logger.info(f"🧬 Cloud Database Initialization Sequence activated for basket: {tracked_basket}")
    for asset_token in tracked_basket:
        populate_historical_if_empty(symbol=asset_token)

import os
import logging
import sqlite3
import pandas as pd
import yfinance as yf

# Configure structured telemetry logs early so we catch any errors!
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("TradingEngine.HistoricalSeeder")

# ─── 🛡️ SECURED DYNAMIC PARAMETERS GATE ───
try:
    from config.settings import settings
    DEFAULT_WATCHLIST_FALLBACK = list(settings.DEFAULT_WATCHLIST) if settings.DEFAULT_WATCHLIST else ["AAPL", "NVDA", "SPY"]
except Exception as config_err:
    logger.warning(f"⚠️ AppSettings load deferred during data injection (using system baselines): {config_err}")
    DEFAULT_WATCHLIST_FALLBACK = ["AAPL", "NVDA", "SPY"]

DB_PATH = "/app_storage/trading_data.db"


def seed_historical_market_data():
    """Reads your dynamic configuration watchlist and downloads lookback bars from Yahoo Finance."""
    logger.info("🧬 Starting automated historical data seeder pipeline...")

    # ✅ Uses our hardened configuration properties fallback baseline
    target_symbols = DEFAULT_WATCHLIST_FALLBACK
    logger.info(f"📋 Watchlist selected for historical injection: {target_symbols}")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")

            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS historical_ticks
                           (
                               id         INTEGER PRIMARY KEY AUTOINCREMENT,
                               timestamp  TEXT,
                               symbol     TEXT,
                               last_price REAL,
                               volume     INTEGER,
                               UNIQUE (timestamp, symbol)
                           )
                           """)

            for symbol in target_symbols:
                symbol = str(symbol).strip().upper()
                logger.info(f"🔍 Auditing ledger logs database records for ticker: '{symbol}'")

                cursor.execute("SELECT COUNT(*) FROM historical_ticks WHERE symbol = ?", (symbol,))
                if cursor.fetchone()[0] > 0:
                    logger.info(f"🛑 [SAFE SKIP] Table already contains records for '{symbol}'. Skipping download.")
                    continue

                logger.info(f"📡 Downloading 1 month of 5m bars from Yahoo Finance for: {symbol}")
                df = yf.download(symbol, period="1mo", interval="5m")

                if df.empty:
                    logger.warning(f"⚠️ Yahoo Finance API returned empty dataframe for symbol: {symbol}")
                    continue

                logger.info(f"⚙️ Raw Columns Detected: {df.columns.tolist()}")

                # ─── 🛡️ THE DEFINITIVE MULTI-LEVEL HEADERS FLATTENER ───
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[0] for col in df.columns]

                logger.info(f"💾 Ingesting {len(df)} price rows into SQLite for '{symbol}'...")

                success_count = 0
                for idx, row in df.iterrows():
                    ts_str = idx.strftime("%Y-%m-%d %H:%M:%S") if hasattr(idx, "strftime") else str(idx)

                    try:
                        price_val = float(row['Close'])
                        volume_val = int(row['Volume'])
                    except KeyError:
                        price_val = float(row.get('Adj Close', row.get('Open', 0.0)))
                        volume_val = int(row.get('Volume', 0))

                    if price_val == 0.0:
                        continue

                    try:
                        cursor.execute("""
                                       INSERT OR IGNORE INTO historical_ticks (timestamp, symbol, last_price, volume)
                                       VALUES (?, ?, ?, ?)
                                       """, (ts_str, symbol, price_val, volume_val))
                        success_count += 1
                    except Exception:
                        continue

                logger.info(f"✅ Committed {success_count} data bars cleanly for '{symbol}'.")

            conn.commit()
            logger.info("📊 [LEDGER INJECTION SUCCESS] Historical seeder pipeline completed cleanly.")

    except Exception as fatal_err:
        logger.error(f"❌ Critical failure running history download matrix: {str(fatal_err)}")


if __name__ == "__main__":
    seed_historical_market_data()

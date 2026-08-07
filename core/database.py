# ==============================================================================
# 💾 PERSISTENCE ARCHITECTURE SYSTEM INTERFACE CONFIGURATIONS
# ==============================================================================
import os
import sqlite3
import logging
import aiosqlite
from datetime import datetime
from typing import Dict, Any, Tuple, List

logger = logging.getLogger("TradingEngine.Database")

# Hard-enforces absolute directory mapping straight down into your unmasked storage mount bridge [INDEX]
DB_PATH = "/app_storage/trading_data.db"


class TradingDatabase:
    """
    Manages an asynchronous, non-blocking SQLite persistent storage engine.
    Fully integrated to synchronize across isolated container volumes in WAL mode [INDEX].
    """

    def __init__(self) -> None:
        # ✅ FIX 1: Explicitly anchors the path variable to instance property scopes [INDEX]
        self.db_path: str = DB_PATH

    async def initialize_db(self) -> None:
        """Creates the relational storage schema tables if they do not exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        async with aiosqlite.connect(self.db_path) as db:
            # Forces SQLite into Write-Ahead Logging mode to prevent transaction drops [INDEX]
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout = 5000;")

            # 1. LIVE TRADING TICKS (Kept lightweight for the UI)
            await db.execute("""
                 CREATE TABLE IF NOT EXISTS market_ticks (
                     id         INTEGER PRIMARY KEY AUTOINCREMENT,
                     timestamp  TEXT,
                     symbol     TEXT,
                     last_price REAL,
                     volume     INTEGER
                 )
             """)

            # 2. DEEP HISTORICAL TRAINING DATA FOR XGBOOST WARMUP
            # ✅ FIX 2: Swapped to a composite UNIQUE constraint so multi-ticker histories stream simultaneously! [INDEX]
            await db.execute("""
                 CREATE TABLE IF NOT EXISTS historical_ticks (
                     id         INTEGER PRIMARY KEY AUTOINCREMENT,
                     timestamp  TEXT,
                     symbol     TEXT,
                     last_price REAL,
                     volume     INTEGER,
                     UNIQUE(timestamp, symbol)
                 )
             """)

            # 3. TRANSACTIONAL ORDER RECEIPTS LEDGER TABLE
            # ✅ FIX 3: Cast quantity parameter to REAL to fully protect cryptocurrency fractions [INDEX]
            await db.execute("""
                 CREATE TABLE IF NOT EXISTS trade_receipts (
                     id              INTEGER PRIMARY KEY AUTOINCREMENT,
                     order_id        TEXT UNIQUE,
                     timestamp       TEXT,
                     symbol          TEXT,
                     action          TEXT,
                     quantity        REAL, 
                     execution_price REAL,
                     status          TEXT
                 )
             """)

            # 4. PERSISTENT SINGLE INDICATOR CONFIGURATIONS
            await db.execute("""
                 CREATE TABLE IF NOT EXISTS system_parameters (
                     key        TEXT PRIMARY KEY,
                     value      TEXT NOT NULL,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                 )
             """)

            # 5. PERSISTENT STATEFUL ASSET REGISTRY CHECKBOXES
            await db.execute("""
                 CREATE TABLE IF NOT EXISTS workspace_asset_registry (
                     symbol     TEXT PRIMARY KEY,
                     is_enabled INTEGER NOT NULL,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                 )
             """)

            await db.commit()

            # Programmatic verification step checks
            async with db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';") as cursor:
                rows = await cursor.fetchall()
                active_tables = [row[0] for row in rows]

        logger.info(f"💾 Async SQLite Persistent Engine initialized safely in WAL mode at {self.db_path}")
        logger.info(f"📂 Verified Active Database Tables List: {active_tables}")

    async def load_saved_session(self) -> Tuple[Dict[str, str], Dict[str, bool]]:
        """Asynchronously queries the database to extract the last day's session parameters."""
        param_dict: Dict[str, str] = {}
        saved_registry: Dict[str, bool] = {}

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # 1. Load indicator numbers and confidence levels
                async with db.execute("SELECT key, value FROM system_parameters") as cursor:
                    rows = await cursor.fetchall()
                    for key, val in rows:
                        param_dict[key] = val

                # 2. Load enabled/disabled stock checkbox configurations
                async with db.execute("SELECT symbol, is_enabled FROM workspace_asset_registry") as cursor:
                    rows = await cursor.fetchall()
                    for symbol, is_enabled in rows:
                        saved_registry[symbol] = bool(is_enabled)

            logger.info(f"🔌 Session state recovered! Extracted {len(param_dict)} parameters and {len(saved_registry)} assets.")
        except Exception as e:
            logger.error(f"Async database fetch failed while loading workspace session: {str(e)}")

        return param_dict, saved_registry

    async def save_session_state(self, parameters: List[Tuple[str, str]], registry_map: Dict[str, bool]) -> None:
        """Asynchronously writes the entire UI configuration panel state to the disk tables."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Upsert primitive lookback numbers and threshold levels
                await db.executemany("""
                     INSERT INTO system_parameters (key, value, updated_at)
                     VALUES (?, ?, CURRENT_TIMESTAMP)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                                                    updated_at=CURRENT_TIMESTAMP
                 """, parameters)

                # Upsert individual asset tracking flags (1 = Enabled / 0 = Disabled)
                for symbol, is_enabled in registry_map.items():
                    await db.execute("""
                         INSERT INTO workspace_asset_registry (symbol, is_enabled, updated_at)
                         VALUES (?, ?, CURRENT_TIMESTAMP)
                         ON CONFLICT(symbol) DO UPDATE SET is_enabled=excluded.is_enabled,
                                                           updated_at=CURRENT_TIMESTAMP
                     """, (symbol.upper(), 1 if is_enabled else 0))

                await db.commit()
                logger.info("💾 Workspace configurations safely serialized to disk database via aiosqlite.")
        except Exception as e:
            logger.error(f"Database write failure on workspace session persistence execution: {str(e)}")

    async def save_tick(self, tick: Dict[str, Any]) -> None:
        """Asynchronously writes a streaming candle update row to the disk partition."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO market_ticks (timestamp, symbol, last_price, volume) VALUES (?, ?, ?, ?)",
                    (tick.get("timestamp", datetime.now().isoformat()), tick["symbol"], tick["last_price"], tick["volume"])
                )
                await db.commit()
        except Exception as e:
            logger.error(f"Database write failure on tick telemetry: {str(e)}")

    async def save_receipt(self, receipt: dict) -> bool:
        """Pipes completed execution receipts from the order manager cleanly into disk database tables."""
        try:
            order_id = str(receipt.get("order_id", receipt.get("id", f"MOCK_{int(datetime.now().timestamp())}")))
            symbol = str(receipt.get("symbol", "UNKNOWN")).strip().upper()
            action = str(receipt.get("action", receipt.get("side", "BUY"))).strip().upper()

            # ✅ FIX 4: Changed mapping to float to natively protect fractional share sizes! [INDEX]
            quantity = float(receipt.get("executed_qty", receipt.get("quantity", 0.0)))
            exec_price = float(receipt.get("execution_price", receipt.get("price", 0.0)))
            status = str(receipt.get("status", "FILLED")).strip().upper()

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT OR REPLACE INTO trade_receipts 
                    (order_id, timestamp, symbol, action, quantity, execution_price, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (order_id, timestamp, symbol, action, quantity, exec_price, status))
                await db.commit()

            logger.info(f"📊 [LEDGER ROW COMMITTED] Transaction receipt {order_id} written to trade_receipts table.")
            return True
        except Exception as ledger_err:
            logger.error(f"❌ Failed to append trade ticket matrix to host platter storage: {str(ledger_err)}")
            return False


# Global database pool singleton instantiation instance [INDEX]
db_engine = TradingDatabase()

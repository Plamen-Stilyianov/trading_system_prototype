import os
import logging
import aiosqlite
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger("TradingEngine.Database")
DB_PATH = "logs/trading_data.db"  # Saves into the persistent shared volume mount


class TradingDatabase:
    """Manages an asynchronous, non-blocking SQLite persistent storage engine."""

    async def initialize_db(self):
        """Creates the relational storage schema tables if they do not exist."""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        async with aiosqlite.connect(DB_PATH) as db:
            # 1. Historical Candlestick Tick Telemetry Table
            await db.execute("""
                             CREATE TABLE IF NOT EXISTS market_ticks
                             (
                                 id
                                 INTEGER
                                 PRIMARY
                                 KEY
                                 AUTOINCREMENT,
                                 timestamp
                                 TEXT,
                                 symbol
                                 TEXT,
                                 last_price
                                 REAL,
                                 volume
                                 INTEGER
                             )
                             """)
            # 2. Transactional Trade Order Receipts Ledger Table
            await db.execute("""
                             CREATE TABLE IF NOT EXISTS trade_receipts
                             (
                                 id
                                 INTEGER
                                 PRIMARY
                                 KEY
                                 AUTOINCREMENT,
                                 order_id
                                 TEXT
                                 UNIQUE,
                                 timestamp
                                 TEXT,
                                 symbol
                                 TEXT,
                                 action
                                 TEXT,
                                 quantity
                                 INTEGER,
                                 execution_price
                                 REAL,
                                 status
                                 TEXT
                             )
                             """)
            await db.commit()
        logger.info(f"💾 Async SQLite Persistent Engine initialized safely at {DB_PATH}")

    async def save_tick(self, tick: Dict[str, Any]):
        """Asynchronously writes a streaming candle update row to the disk partition."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO market_ticks (timestamp, symbol, last_price, volume) VALUES (?, ?, ?, ?)",
                    (tick.get("timestamp", datetime.now().isoformat()), tick["symbol"], tick["last_price"],
                     tick["volume"])
                )
                await db.commit()
        except Exception as e:
            logger.error(f"Database write failure on tick telemetry: {str(e)}")

    async def save_receipt(self, receipt: Dict[str, Any]):
        """Asynchronously updates or inserts an official execution order receipt ledger item."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("""
                    INSERT OR REPLACE INTO trade_receipts 
                    (order_id, timestamp, symbol, action, quantity, execution_price, status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    receipt["order_id"], receipt.get("timestamp", datetime.now().isoformat()),
                    receipt["symbol"], receipt["action"], receipt["executed_qty"],
                    receipt["execution_price"], receipt["status"]
                ))
                await db.commit()
        except Exception as e:
            logger.error(f"Database write failure on transaction ledger record: {str(e)}")


# Global database pool singleton instantiation instance
db_engine = TradingDatabase()

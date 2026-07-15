import logging
import threading
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger("TradingEngine.StateManager")


class StateManager:
    """
    A thread-safe, centralized telemetry storage engine.
    Manages global execution flags, live balances, active positions,
    and systemic execution logging histories.
    """

    def __init__(self):
        # Operational Thread Locks guarding structural mutations
        self._lock = threading.Lock()

        # Engine Master Controls
        self.is_engine_active: bool = False

        # Live Balance & Equity Telemetry
        self.balance: float = 100000.00
        self.equity: float = 100000.00
        self.daily_pnl: float = 0.00
        self.initial_day_balance: float = 100000.00

        # Structural Collections
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.market_data: Dict[str, Dict[str, Any]] = {}
        self._system_logs: List[Dict[str, Any]] = []

    def set_engine_activity(self, active: bool) -> None:
        """Globally flips the execution switch (Start/Stop button toggles)."""
        with self._lock:
            self.is_engine_active = active
            self._add_log_entry(
                "SYSTEM",
                f"Global engine operational switch updated to: {'ENABLED' if active else 'DISABLED'}"
            )

    def update_account(self, account_info: Dict[str, Any]) -> None:
        """Updates core structural portfolio values directly from broker payloads."""
        with self._lock:
            self.balance = account_info.get("cash_balance", self.balance)
            self.equity = account_info.get("portfolio_value", self.equity)
            self.daily_pnl = self.equity - self.initial_day_balance

    def update_market_data(self, tick: Dict[str, Any]) -> None:
        """Caches real-time ticker feeds to keep track of current pricing across threads."""
        symbol = tick.get("symbol")
        if not symbol:
            return
        with self._lock:
            self.market_data[symbol] = {
                "last_price": tick.get("last_price", 0.0),
                "timestamp": tick.get("timestamp", datetime.now().isoformat()),
                "volume": tick.get("volume", 0)
            }
            # Dynamically recalculate unrealized floating components if positions exist
            if symbol in self.positions:
                pos = self.positions[symbol]
                current_price = tick.get("last_price", pos["entry_price"])
                pos["current_price"] = current_price
                pos["unrealized_pnl"] = (current_price - pos["entry_price"]) * pos["qty"]

    def update_position_state(self, symbol: str, qty: int, entry_price: float) -> None:
        """Modifies active portfolio lists immediately following broker order confirmations."""
        with self._lock:
            if qty <= 0:
                # Target inventory position completely closed
                if symbol in self.positions:
                    closed_pos = self.positions.pop(symbol)
                    realized = (entry_price - closed_pos["entry_price"]) * closed_pos["qty"]
                    self._add_log_entry("ORDER", f"Position closed for {symbol}. Realized P&L: ${realized:,.2f}")
            else:
                # Add or adjust active holding parameters
                self.positions[symbol] = {
                    "symbol": symbol,
                    "qty": qty,
                    "entry_price": entry_price,
                    "current_price": entry_price,
                    "unrealized_pnl": 0.0,
                    "updated_at": datetime.now().isoformat()
                }
                self._add_log_entry("ORDER", f"Position updated for {symbol}: {qty} shares @ ${entry_price:,.2f}")

    def log_event(self, category: str, message: str) -> None:
        """Thread-safe public gateway to record runtime actions inside telemetry history."""
        with self._lock:
            self._add_log_entry(category, message)

    def _add_log_entry(self, category: str, message: str) -> None:
        """Internal uncaught helper appending structural logging tracks to list arrays."""
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": category.upper(),  # SYSTEM, ORDER, AI, RISK
            "message": message
        }
        self._system_logs.append(entry)
        # Cap internal tracking arrays to 1000 lines to avoid container memory inflation
        if len(self._system_logs) > 1000:
            self._system_logs.pop(0)

    def get_summary_metrics(self) -> Dict[str, Any]:
        """Generates clean operational telemetry payloads for visual analytics components."""
        with self._lock:
            return {
                "cash_balance": self.balance,
                "portfolio_value": self.equity,
                "daily_pnl": self.daily_pnl,
                "roi_percentage": (self.daily_pnl / self.initial_day_balance) * 100 if self.initial_day_balance else 0.0,
                "active_positions_count": len(self.positions)
            }

    def get_recent_logs(self, tail_lines: int = 20) -> List[Dict[str, Any]]:
        """Returns the final slices of engine actions to populate the UI scrolling panel."""
        with self._lock:
            return self._system_logs[-tail_lines:]


# Instantiated Singleton instance to share absolute module state globally across the microservice
state_manager = StateManager()

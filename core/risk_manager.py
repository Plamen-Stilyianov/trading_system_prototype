import logging
from typing import Dict, Any
from core.state_manager import state_manager

logger = logging.getLogger("TradingEngine.RiskManager")


class RiskManager:
    """
    Acts as a pre-trade circuit breaker.
    Validates order size constraints and monitors maximum total portfolio drawdown
    limits before passing instructions down to the broker network layers.
    """

    def __init__(self, max_drawdown_pct: float = 5.0, max_order_size_pct: float = 2.0):
        self.max_drawdown_pct: float = max_drawdown_pct
        self.max_order_size_pct: float = max_order_size_pct

    def validate_order_risk(self, signal: Dict[str, Any]) -> bool:
        """
        Evaluates an order signal against strict risk threshold rules.
        :return: True if the trade is within safe boundaries; False if rejected.
        """
        symbol = signal["symbol"]
        action = signal["action"]
        qty = signal["quantity"]

        # 1. Fetch live portfolio valuations from the thread-safe telemetry cache
        metrics = state_manager.get_summary_metrics()
        portfolio_value = metrics["portfolio_value"]
        daily_pnl_pct = metrics["roi_percentage"]

        # 2. Rule 1: Portfolio Drawdown Circuit Breaker
        if daily_pnl_pct <= -self.max_drawdown_pct:
            state_manager.log_event(
                "RISK",
                f"🚨 SYSTEM BLOCK: Daily drawdown threshold (-{self.max_drawdown_pct}%) breached. Order rejected."
            )
            return False

        # 3. Rule 2: Maximum Position Allocation Sizing Guard
        last_tick_price = state_manager.market_data.get(symbol, {}).get("last_price", 0.0)

        if last_tick_price > 0.0:
            estimated_order_value = qty * last_tick_price
            max_allowed_order_value = portfolio_value * (self.max_order_size_pct / 100.0)

            if estimated_order_value > max_allowed_order_value:
                state_manager.log_event(
                    "RISK",
                    f"⚠️ ORDER REJECTED: Estimated size (${estimated_order_value:,.2f}) "
                    f"exceeds maximum single order allocation limit (${max_allowed_order_value:,.2f})."
                )
                return False

        return True

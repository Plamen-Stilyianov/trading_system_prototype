import logging
from typing import Dict, Any, Optional
from config.settings import settings

logger = logging.getLogger("TradingEngine.RiskManager")


class RiskManager:
    """
    Enforces real-time asset position control metrics and strategic risk mitigation checks.
    Acts as an isolation firewall between ML trading signals and live exchange routers.
    """

    def __init__(self, max_drawdown_pct: float = 0.05, **kwargs) -> None:
        # Load baseline parameters dynamically from structural configuration settings
        self.max_drawdown_pct: float = max_drawdown_pct
        self.max_portfolio_risk_pct: float = 0.10  # Max 10% total portfolio allocation
        self.per_trade_risk_pct: float = 0.02  # Max 2% capital allocation per execution slot
        self.confidence_floor: float = settings.ML_CONFIDENCE_THRESHOLD  # e.g., 0.65 threshold

    async def validate_signal(self, signal_payload: Dict[str, Any], account_metrics: Dict[str, Any]) -> bool:
        """
        Executes an entry constraint matrix check to authorize or block trade signals.
        """
        symbol: str = signal_payload.get("symbol", settings.TARGET_SYMBOL)
        action: str = signal_payload.get("action", "HOLD")
        confidence: float = signal_payload.get("confidence", 0.0)

        # Extract liquid asset financial data parameters safely
        available_cash: float = float(account_metrics.get("cash", 0.0))
        portfolio_value: float = float(account_metrics.get("portfolio_value", 0.0))

        if action == "HOLD":
            return False

        logger.debug(f"[RISK] Analyzing {action} signal for {symbol} (Confidence: {confidence:.2f})")

        # Check 1: Machine Learning Confidence Barrier Protection
        if confidence < self.confidence_floor:
            logger.warning(
                f"[BLOCK] Signal confidence {confidence:.2f} sits below strategy floor limit: {self.confidence_floor}")
            return False

        # Check 2: Absolute Liquidity Capital Safety Check
        if available_cash <= 0:
            logger.warning(f"[BLOCK] Capital dry-out detected. Available balance: ${available_cash:.2f}")
            return False

        # Check 3: Dynamic Sizing Cap Boundaries Enforced (Max 2% of total portfolio value)
        max_allowed_allocation = portfolio_value * self.per_trade_risk_pct
        entry_price = float(signal_payload.get("price", 0.0))
        
        # Smart Router: Scale sizing down for expensive assets like Bitcoin to prevent instantly breaching risk ceilings
        if entry_price > 1000.0:
            calculated_qty = round(max_allowed_allocation / entry_price, 4)
            target_allocation = calculated_qty * entry_price
            logger.info(f"[RISK-SCALING] High asset price detected. Dynamically adjusted execution quantity to: {calculated_qty} units")
        else:
            target_allocation = entry_price * settings.DEFAULT_QTY

        if target_allocation > max_allowed_allocation:
            logger.warning(
                f"[BLOCK] Order sizing ${target_allocation:.2f} exceeds risk metric cap: ${max_allowed_allocation:.2f}")
            return False

        logger.info(f"[AUTHORIZE] Risk compliance pass cleared for {action} {symbol}.")
        return True

    def calculate_trailing_stop(self, entry_price: float, current_price: float, position_type: str) -> float:
        """
        Dynamically adjusts stop-loss thresholds to lock in algorithmic trend profits.
        """
        stop_pct = 0.015  # Tight 1.5% stop-loss threshold restriction profile
        if position_type == "long":
            return max(entry_price * (1.0 - stop_pct), current_price * (1.0 - stop_pct))
        else:
            return min(entry_price * (1.0 + stop_pct), current_price * (1.0 + stop_pct))

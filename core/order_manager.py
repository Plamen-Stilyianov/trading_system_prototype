import logging
import asyncio  # ✅ FIX 1: Added missing async framework core import
from typing import Dict, Any
from core.state_manager import state_manager
from core.broker_client import BrokerClient
from core.risk_manager import RiskManager
from core.database import db_engine

logger = logging.getLogger("TradingEngine.OrderManager")


class OrderManager:
    """
    Validates structural signals, routes trades to active execution brokers,
    and coordinates transactional states with the thread-safe telemetry store.
    Integrates a strict pre-trade risk management validation layer and async logging.
    """

    def __init__(self, broker_client: BrokerClient):
        self.broker: BrokerClient = broker_client
        # Instantiate Risk Engine: Locked to 5.0% max daily drawdown and 2.0% max allocation per trade
        self.risk_engine: RiskManager = RiskManager(max_drawdown_pct=5.0, max_order_size_pct=2.0)

    async def route_order(self, signal: Dict[str, Any]) -> bool:
        """
        Processes trade signals. Validates structural checks, filters against
        pre-trade risk parameters, routes executions, and commits filled loops.
        """
        symbol = signal["symbol"]
        action = signal["action"]
        qty = signal["quantity"]

        # 1. Pre-Trade Structural Safety Check
        if qty <= 0:
            logger.warning(f"Order rejected. Invalidation: Quantity parameter must be positive. Given: {qty}")
            return False

        state_manager.log_event("RISK", f"Pre-trade structural validation passed for {action} {qty} {symbol}.")

        # 2. Pre-Trade Quantitative Risk Parameters Check
        # Intercepts signals and screens against max allocation and daily portfolio drawdown caps
        if not self.risk_engine.validate_order_risk(signal):
            logger.warning(f"Pre-trade Risk Violation: Order for {qty} {symbol} was blocked by the risk manager.")
            return False

        # 3. Secure Execution Layer Handshake Routing
        try:
            # Transfer transactional dictionary across the network connection pool
            fill_receipt = await self.broker.execute_order_payload(signal)

            if fill_receipt and fill_receipt.get("status") == "FILLED":
                exec_price = fill_receipt["execution_price"]
                actual_qty = fill_receipt["executed_qty"]

                # Deduce new active inventory layout based on trade actions
                if action == "BUY":
                    # Update local state holding records
                    state_manager.update_position_state(symbol, actual_qty, exec_price)
                elif action == "SELL":
                    # For a basic prototype model, a sell maps to flattening inventory
                    state_manager.update_position_state(symbol, 0, exec_price)

                state_manager.log_event(
                    "ORDER",
                    f"Execution Filled! ID: {fill_receipt['order_id']} | {action} {actual_qty} {symbol} @ ${exec_price}"
                )

                # ✅ FIX 2: Moved database tracking write task here AFTER receipt is generated
                asyncio.create_task(db_engine.save_receipt(fill_receipt))

                return True

        except Exception as e:
            logger.error(f"Critical execution fault processing broker order route: {str(e)}")
            state_manager.log_event("SYSTEM", f"Critical Order Routing Failure: {str(e)}")

        return False

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd


class BaseStrategy(ABC):
    """
    Abstract Base Class enforcing the blueprint for all trading strategies.
    Any AI-generated strategy must inherit from this class and implement
    its core entry points to communicate seamlessly with the system.
    """

    def __init__(self, strategy_id: str, parameters: Dict[str, Any]):
        """
        Initializes the strategy with essential parameters.

        :param strategy_id: Unique string identifier for tracking performance.
        :param parameters: Dictionary containing risk parameters, lookbacks, multipliers, etc.
        """
        self.strategy_id: str = strategy_id
        self.parameters: Dict[str, Any] = parameters
        self.is_enabled: bool = False  # Controlled via the Streamlit Start/Stop button

    def toggle_state(self, enabled: bool) -> None:
        """Updates the operational state of the strategy via the Web UI."""
        self.is_enabled = enabled

    @abstractmethod
    def generate_features(self, historical_data: pd.DataFrame) -> pd.DataFrame:
        """
        Accepts historical market matrix data and calls ML pipelines or mathematical
        TA-Lib indicators to generate a structured feature data frame.

        :param historical_data: Pandas DataFrame containing OHLCV bars.
        :return: Extended DataFrame with model features or indicator values.
        """
        pass

    @abstractmethod
    def on_market_tick(self, tick_data: Dict[str, Any], current_position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Primary engine entry point for real-time streaming market updates.
        This is where AI-generated entry and exit logic processes new price data.

        :param tick_data: Real-time price dictionary containing timestamp, symbol, bid, ask, volume.
        :param current_position: Dictionary showing active exposure, entry price, and unrealized P&L.
        :return: Optional trade signal dictionary structured for core/order_manager.py, or None.

        Expected output dictionary format:
        {
            "strategy_id": "AI_MOMENTUM_V1",
            "symbol": "AAPL",
            "action": "BUY" | "SELL" | "HOLD",
            "order_type": "MARKET" | "LIMIT",
            "quantity": 10,
            "limit_price": Optional[float]
        }
        """
        pass

    @abstractmethod
    def on_interval_check(self, current_positions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Periodic operational check (e.g., triggered on every 5-minute candle close or fixed loop interval).
        Used by the AI to run evaluations, risk reassessments, or portfolio rebalancing.

        :param current_positions: Dictionary containing all open positions across the system.
        :return: Optional trade signal dictionary, or None.
        """
        pass

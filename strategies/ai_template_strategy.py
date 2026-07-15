import logging
from typing import Dict, Any, Optional
import pandas as pd

# Explicitly register the pandas extension hooks into the workspace memory
import pandas_ta_classic as ta

# Internal absolute project architecture imports
from strategies.base_strategy import BaseStrategy
from ml_pipeline.inference_engine import InferenceEngine

logger = logging.getLogger("TradingEngine.AI_Strategy")


class AITemplateStrategy(BaseStrategy):
    """
    A concrete implementation of BaseStrategy.
    Uses pandas-ta-classic to generate indicators purely in native Python.
    No underlying C-compilers or .tar.gz assets required.
    """

    def __init__(self, strategy_id: str, parameters: Dict[str, Any]):
        """Initializes components and loads the ML weights model."""
        super().__init__(strategy_id, parameters)

        # Pull threshold variables from config parameters dictionary
        self.rsi_period: int = parameters.get("rsi_period", 14)
        self.rsi_overbought: float = parameters.get("rsi_overbought", 70.0)
        self.rsi_oversold: float = parameters.get("rsi_oversold", 30.0)
        self.ml_confidence_threshold: float = parameters.get("ml_threshold", 0.65)

        # Instantiate your ML Inference framework engine
        self.ml_engine = InferenceEngine(model_name="xgboost_v1.pkl")

    def generate_features(self, historical_data: pd.DataFrame) -> pd.DataFrame:
        """
        Accepts raw historical price frames and appends technical analysis metrics.
        Utilizes the native .ta extension injected by pandas-ta-classic.
        """
        if historical_data.empty or len(historical_data) < self.rsi_period:
            return historical_data

        # Explicit copy modification to guard pandas slice warnings
        df = historical_data.copy()

        # 1. Compute technical indicators purely in Python via Pandas extension hooks
        df["rsi"] = df.ta.rsi(length=self.rsi_period)
        df["sma_fast"] = df.ta.sma(length=9)
        df["sma_slow"] = df.ta.sma(length=21)

        return df

    def on_market_tick(self, tick_data: Dict[str, Any], current_position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processes real-time streaming pricing updates."""
        if not self.is_enabled:
            return None

        symbol = tick_data.get("symbol")
        last_price = tick_data.get("last_price", 0.0)
        has_position = current_position.get("qty", 0) > 0

        try:
            if has_position:
                if last_price < current_position.get("stop_loss_price", 0.0):
                    return self._create_signal(symbol, action="SELL", qty=current_position["qty"])
        except Exception as e:
            logger.error(f"Error executing live tick calculations inside {self.strategy_id}: {str(e)}")

        return None

    def on_interval_check(self, current_positions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Triggers on data bars (e.g., 5-minute candle close).
        Combines pandas-ta indicators and ML inferences to generate orders.
        """
        if not self.is_enabled:
            return None

        symbol = self.parameters.get("target_symbol", "AAPL")
        position_details = current_positions.get(symbol, {"qty": 0})
        has_position = position_details.get("qty", 0) > 0

        # Ingest historical array feeds (Passed mock empty state or filled by runner)
        raw_df = pd.DataFrame()
        if raw_df.empty:
            return None

        engineered_df = self.generate_features(raw_df)
        latest_row = engineered_df.iloc[-1]

        current_rsi = latest_row["rsi"]
        ml_prediction_prob = self.ml_engine.predict_next_move(latest_row)

        # BUY LOGIC: Oversold conditions paired with highly confident ML direction metrics
        if not has_position:
            if current_rsi < self.rsi_oversold and ml_prediction_prob >= self.ml_confidence_threshold:
                logger.info(
                    f"[{self.strategy_id}] BUY signal identified for {symbol}. RSI: {current_rsi:.2f}, ML Prob: {ml_prediction_prob:.2f}")
                return self._create_signal(symbol, action="BUY", qty=self.parameters.get("default_qty", 100))

        # SELL LOGIC: Reaching overbought standard parameters
        elif has_position:
            if current_rsi > self.rsi_overbought:
                logger.info(f"[{self.strategy_id}] SELL exit signal identified for {symbol}. RSI: {current_rsi:.2f}")
                return self._create_signal(symbol, action="SELL", qty=position_details["qty"])

        return None

    def _create_signal(self, symbol: str, action: str, qty: int) -> Dict[str, Any]:
        """Utility construction helper mapping output frames back to core/order_manager.py"""
        return {
            "strategy_id": self.strategy_id,
            "symbol": symbol,
            "action": action,
            "order_type": "MARKET",
            "quantity": qty,
            "limit_price": None
        }

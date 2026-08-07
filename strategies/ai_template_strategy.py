import os
import logging
import sqlite3
from typing import Dict, Any, Optional
import pandas as pd

# ✅ FIX 1: Corrected Pandas-TA classic functional package mappings
import pandas_ta_classic as ta

from strategies.base_strategy import BaseStrategy
from ml_pipeline.inference_engine import InferenceEngine
from core.state_manager import state_manager

logger = logging.getLogger("TradingEngine.AI_Strategy")


class AITemplateStrategy(BaseStrategy):
    """
    A concrete implementation of BaseStrategy.
    Extracts historical ticks from the SQLite ledger, computes technical indicators
    via pandas-ta-classic, and validates trades using the XGBoost inference layer.
    """

    def __init__(self, strategy_id: str, parameters: Dict[str, Any]):
        """Initializes components and loads the ML weights model."""
        super().__init__(strategy_id, parameters)

        self.rsi_period: int = int(parameters.get("rsi_period", 14))
        self.rsi_overbought: float = float(parameters.get("rsi_overbought", 70.0))
        self.rsi_oversold: float = float(parameters.get("rsi_oversold", 30.0))
        self.ml_confidence_threshold: float = float(parameters.get("ml_confidence_threshold", 0.65))

        # Instantiate your ML Inference framework engine
        self.ml_engine = InferenceEngine(model_name="xgboost_v1.pkl")

    def generate_features(self, historical_data: pd.DataFrame) -> pd.DataFrame:
        """
        Accepts raw historical price frames and appends technical analysis metrics.
        """
        if historical_data.empty or len(historical_data) < self.rsi_period:
            return historical_data

        df = historical_data.copy()
        close_series = df["close"]

        # ✅ FIX 3: Re-aligned computations to use valid pandas-ta-classic functional calls
        df["rsi"] = ta.rsi(close_series, length=self.rsi_period)
        df["sma_fast"] = ta.sma(close_series, length=9)
        df["sma_slow"] = ta.sma(close_series, length=21)

        macd_df = ta.macd(close_series)
        df["macd"] = macd_df.iloc[:, 0] if macd_df is not None else 0.0

        bb_df = ta.bbands(close_series)
        df["bbl"] = bb_df.iloc[:, 0] if bb_df is not None else 0.0
        df["bbu"] = bb_df.iloc[:, 2] if bb_df is not None else 0.0

        return df

    def on_market_tick(self, tick_data: Dict[str, Any], current_position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processes real-time streaming pricing updates for emergency stops."""
        if not self.is_enabled or tick_data is None or not isinstance(tick_data, dict):
            return None

        symbol = tick_data.get("symbol")
        last_price = float(tick_data.get("last_price", 0.0))
        has_position = current_position.get("qty", 0) > 0

        try:
            if has_position:
                if last_price < float(current_position.get("stop_loss_price", 0.0)):
                    logger.warning(f"🚨 [STOP LOSS] Last price {last_price} dropped below floor. Liquidating {symbol}.")
                    return self._create_signal(symbol, action="SELL", qty=current_position["qty"])
        except Exception as e:
            logger.error(f"Error executing live tick calculations inside {self.strategy_id}: {str(e)}")

        return None

    def on_interval_check(self, current_positions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Triggers on data bars.
        Queries local database logs, computes technical metrics, and runs machine learning inference.
        """
        if not self.is_enabled:
            return None

        # Dynamically pulls active asset registry mappings from parameter settings
        active_watchlist = self.parameters.get("active_watchlist", ["AAPL", "NVDA", "SPY"])

        for symbol in active_watchlist:
            symbol = str(symbol).strip().upper()
            position_details = current_positions.get(symbol, {"qty": 0})
            has_position = position_details.get("qty", 0) > 0

            raw_df = pd.DataFrame()

            try:
                with sqlite3.connect("/app_storage/trading_data.db") as conn:
                    # ✅ FIX: Swapped sorting fields from 'id' to 'timestamp' to ensure full transactional alignment! [INDEX]
                    query = f"SELECT timestamp, symbol, last_price, volume FROM historical_ticks WHERE symbol = '{symbol}' ORDER BY timestamp DESC LIMIT 100"
                    raw_df = pd.read_sql_query(query, conn)

                    if not raw_df.empty:
                        raw_df["close"] = raw_df["last_price"]
                        raw_df = raw_df.iloc[::-1].reset_index(drop=True)
            except Exception as e:
                logger.error(f"❌ [{self.strategy_id}] Failed to extract historical rows for {symbol}: {str(e)}")
                continue

            # Safety Gate: Ensure this specific ticker has captured enough records to generate indicators
            if raw_df.empty or len(raw_df) < 30:
                continue

            # ─── 🤖 STEP 2: TECHNICAL FEATURE GENERATION ───
            logger.info(
                f"📊 [STRATEGY DB BREAKTHROUGH] Processing {len(raw_df)} rows from historical_ticks for {symbol}.")
            engineered_df = self.generate_features(raw_df)
            latest_row = engineered_df.iloc[-1]
            current_rsi = latest_row["rsi"]

            if pd.isna(current_rsi) or pd.isna(latest_row["sma_fast"]) or pd.isna(latest_row["sma_slow"]):
                logger.debug(f"⚠️ Indicators contain uncalculated metrics for {symbol}. Skipping loop iteration.")
                continue

            # ─── 🛡️ STEP 3: CONSTRUCT THE CHRONOLOGICAL TRAINED FEATURE VECTOR ───
            ml_features = pd.Series({
                "rsi": float(latest_row["rsi"]),
                "sma_fast": float(latest_row["sma_fast"]),
                "sma_slow": float(latest_row["sma_slow"]),
                "macd": float(latest_row["macd"]),
                "bbl": float(latest_row["bbl"]),
                "bbu": float(latest_row["bbu"])
            })

            # Request structural directional probability from your type-safe XGBoost engine layer
            ml_prediction_prob = self.ml_engine.predict_next_move(ml_features)

            logger.info(
                f"🧠 [STRATEGY AI MATRIX] {symbol} ──► Calculated RSI: {current_rsi:.2f} | XGBoost Buy Prob: {ml_prediction_prob:.2f}"
            )

            # Inside strategies/ai_template_strategy.py -> Scroll to Step 4:

            # ─── 🚀 STEP 4: EXECUTION MATRIX LOGIC GATES (UNBLOCKED) ───
            # ✅ REMOVED 'if not has_position' to let the model trade multiple entries dynamically! [INDEX]
            if current_rsi < self.rsi_oversold and ml_prediction_prob >= self.ml_confidence_threshold:
                logger.info(
                    f"🟢 [{self.strategy_id}] BUY signal identified for {symbol}. "
                    f"RSI: {current_rsi:.2f}, ML Confidence: {ml_prediction_prob:.2f}"
                )
                return self._create_signal(symbol, action="BUY", qty=int(self.parameters.get("default_qty", 50)))

            # SELL TRIGGER CONDITIONS: Reaching overbought technical limits to secure gains [INDEX]
            elif has_position and current_rsi > self.rsi_overbought:
                logger.info(
                    f"🔴 [{self.strategy_id}] SELL profit-taking signal identified for {symbol}. "
                    f"RSI: {current_rsi:.2f} (> {self.rsi_overbought})"
                )
                return self._create_signal(symbol, action="SELL", qty=int(position_details["qty"]))


        return None

    def _create_signal(self, symbol: str, action: str, qty: int) -> Dict[str, Any]:
        """
        Utility construction helper mapping output frames back to core/order_manager.py.
        ✅ COMPLIANCE FIX: Injects real-time asset pricing metrics to prevent risk calculation dropouts!
        """
        # Pull current ticker valuation handles out of your centralized thread-safe cache store
        current_market_price = float(state_manager.market_data.get(symbol, {}).get("last_price", 0.0))

        return {
            "strategy_id": self.strategy_id,
            "symbol": symbol,
            "action": action,
            "order_type": "MARKET",
            "quantity": qty,
            "limit_price": None,
            "price": current_market_price  # ◄── ✅ Injects the baseline price value for risk engines!
        }




import logging
import sqlite3
from typing import Dict, Any, Optional
import pandas as pd

# Explicitly register the pandas extension hooks into the workspace memory
import pandas_ta_classic as ta

# Internal absolute project architecture imports
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
        """Processes real-time streaming pricing updates for emergency stops."""
        if not self.is_enabled:
            return None

            # ─── 🛡️ FIXED: GENTLE NONE-TYPE SAFETY GUARD GATEWALK ───
        if tick_data is None or not isinstance(tick_data, dict):
            return None

        symbol = tick_data.get("symbol")
        last_price = tick_data.get("last_price", 0.0)
        has_position = current_position.get("qty", 0) > 0

        try:
            if has_position:
                if last_price < current_position.get("stop_loss_price", 0.0):
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

        symbol = self.parameters.get("target_symbol", "AAPL")
        position_details = current_positions.get(symbol, {"qty": 0})
        has_position = position_details.get("qty", 0) > 0

        # ─── 💾 CONNECT AND EXTRACT HISTORICAL LEDGER STREAM FROM SQLITE ───
        db_path = "logs/trading_data.db"
        raw_df = pd.DataFrame()

        try:
            with sqlite3.connect(db_path) as conn:
                query = f"""
                    SELECT timestamp, last_price as close, volume 
                    FROM historical_ticks 
                    WHERE symbol = '{symbol}' 
                    ORDER BY timestamp DESC 
                    LIMIT 100
                """
                raw_df = pd.read_sql_query(query, conn)
                if not raw_df.empty:
                    raw_df = raw_df.iloc[::-1].reset_index(drop=True)
        except Exception as e:
            logger.error(f"[{self.strategy_id}] Failed to extract historical rows: {str(e)}")
            return None

        # ─── 🔍 STEP 1: PROGRAMMATIC DATA AUDIT PRINTOUT ───
        logger.info(
            f"📊 [STRATEGY DB AUDIT] Successfully extracted {len(raw_df)} rows from historical_ticks for {symbol}.")
        if not raw_df.empty:
            logger.info(f"   ↳ Earliest row: {raw_df['timestamp'].iloc[0]} | Price: {raw_df['close'].iloc[0]}")
            logger.info(f"   ↳ Latest row:   {raw_df['timestamp'].iloc[-1]} | Price: {raw_df['close'].iloc[-1]}")

        # Safety Gate: Ensure the system database has captured enough records to generate indicators and lags
        if raw_df.empty or len(raw_df) < max(self.rsi_period, 6):
            logger.warning(f"[{self.strategy_id}] Awaiting rows. Count: {len(raw_df)}/{max(self.rsi_period, 6)}")
            return None

        # ─── 🤖 STEP 2: TECHNICAL FEATURE GENERATION ───
        engineered_df = self.generate_features(raw_df)
        latest_row = engineered_df.iloc[-1]
        current_rsi = latest_row["rsi"]

        # Guard against NaN/Null indicator outputs during early database accumulation windows
        if pd.isna(current_rsi) or pd.isna(latest_row["sma_fast"]) or pd.isna(latest_row["sma_slow"]):
            logger.debug(f"[{self.strategy_id}] Indicators contain uncalculated metrics. Skipping pipeline iteration.")
            return None

        # ─── 🛡️ STEP 3: CONSTRUCT THE CHRONOLOGICAL 6-INTERVAL LAG FEATURE VECTOR ───
        ml_features = pd.Series({
            "lag_5": float(engineered_df["close"].iloc[-6]),  # 25 minutes ago
            "lag_4": float(engineered_df["close"].iloc[-5]),  # 20 minutes ago
            "lag_3": float(engineered_df["close"].iloc[-4]),  # 15 minutes ago
            "lag_2": float(engineered_df["close"].iloc[-3]),  # 10 minutes ago
            "lag_1": float(engineered_df["close"].iloc[-2]),  # 5 minutes ago
            "lag_0": float(engineered_df["close"].iloc[-1])   # Price right now (lag_0)
        })

        # Request structural directional probability from your type-safe XGBoost engine layer
        ml_prediction_prob = self.ml_engine.predict_next_move(ml_features)


        logger.info(
            f"🧠 [STRATEGY AI MATRIX] Calculated RSI: {current_rsi:.2f} | XGBoost Buy Prob: {ml_prediction_prob:.2f}")

        # ─── 🚀 STEP 4: EXECUTION MATRIX LOGIC GATES ───

        # BUY TRIGGER CONDITIONS: Oversold asset criteria paired with high ML prediction confidence
        if not has_position:
            if current_rsi < self.rsi_oversold and ml_prediction_prob >= self.ml_confidence_threshold:
                logger.info(
                    f"🟢 [{self.strategy_id}] BUY signal identified for {symbol}. RSI: {current_rsi:.2f} (< {self.rsi_oversold}), ML Confidence: {ml_prediction_prob:.2f} (>= {self.ml_confidence_threshold})")
                return self._create_signal(symbol, action="BUY", qty=self.parameters.get("default_qty", 50))

        # SELL TRIGGER CONDITIONS: Reaching overbought technical limits to secure simulation gains
        elif has_position:
            if current_rsi > self.rsi_overbought:
                logger.info(
                    f"🔴 [{self.strategy_id}] SELL profit-taking signal identified for {symbol}. RSI: {current_rsi:.2f} (> {self.rsi_overbought})")
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

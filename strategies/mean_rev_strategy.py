# ==============================================================================
# 📈 STRATEGY ENGINE 2: STANDALONE MATHEMATICAL MEAN REVERSION (BOL-BANDS)
# ==============================================================================
import logging
import sqlite3
from typing import Dict, Any, Optional
import pandas as pd
import pandas_ta_classic as ta

from strategies.base_strategy import BaseStrategy
from core.state_manager import state_manager

logger = logging.getLogger("TradingEngine.MeanRevStrategy")


class MeanRevStrategy(BaseStrategy):
    """
    Executes a pure mathematical mean-reversion evaluation.
    Triggers BUYs when prices pierce the lower Bollinger Band,
    and takes profits (SELLs) when prices pierce the upper band.
    """

    def __init__(self, strategy_id: str, parameters: Dict[str, Any]):
        super().__init__(strategy_id, parameters)
        self.rsi_period: int = int(parameters.get("rsi_period", 14))
        self.rsi_overbought: float = float(parameters.get("rsi_overbought", 70.0))
        self.rsi_oversold: float = float(parameters.get("rsi_oversold", 30.0))

    # ✅ FIX 1: Implements the required abstract method interface contract!
    def generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates mathematical indicator columns via pandas-ta-classic for base class parity."""
        df_copy = df.copy()
        close_series = df_copy["close"]

        # Calculate technical fields using pandas_ta_classic functional interfaces
        df_copy["rsi"] = ta.rsi(close_series, length=self.rsi_period)

        bb_df = ta.bbands(close_series, length=20, std=2)
        if bb_df is not None:
            df_copy["bbl"] = bb_df.iloc[:, 0]  # Lower Band (BBL)
            df_copy["bbu"] = bb_df.iloc[:, 2]  # Upper Band (BBU)
        else:
            df_copy["bbl"] = close_series
            df_copy["bbu"] = close_series

        df_copy["sma_fast"] = ta.sma(close_series, length=9)
        df_copy["sma_slow"] = ta.sma(close_series, length=21)
        df_copy["macd"] = 0.0

        return df_copy

    def on_market_tick(self, tick_data: Dict[str, Any], current_position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processes real-time streaming pricing updates for emergency stop liquidations."""
        if not self.is_enabled or tick_data is None or not isinstance(tick_data, dict):
            return None

        symbol = tick_data.get("symbol")
        last_price = float(tick_data.get("last_price", 0.0))
        has_position = current_position.get("qty", 0) > 0

        try:
            if has_position:
                if last_price < float(current_position.get("stop_loss_price", 0.0)):
                    logger.warning(f"🚨 [STOP LOSS] MeanRev liquidation triggered for {symbol}.")
                    return self._create_signal(symbol, action="SELL", qty=current_position["qty"])
        except Exception as e:
            logger.error(f"Error executing live tick calculations inside {self.strategy_id}: {str(e)}")

        return None

    def on_interval_check(self, current_positions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Queries local database logs, computes technical metrics, and evaluates entry bands."""
        if not self.is_enabled:
            return None

        active_watchlist = self.parameters.get("active_watchlist", ["AAPL", "NVDA", "SPY"])

        for symbol in active_watchlist:
            symbol = str(symbol).strip().upper()
            position_details = current_positions.get(symbol, {"qty": 0})
            has_position = position_details.get("qty", 0) > 0

            raw_df = pd.DataFrame()

            try:
                with sqlite3.connect("/app_storage/trading_data.db") as conn:
                    query = f"SELECT timestamp, symbol, last_price, volume FROM historical_ticks WHERE symbol = '{symbol}' ORDER BY timestamp DESC LIMIT 100"
                    raw_df = pd.read_sql_query(query, conn)

                    if not raw_df.empty:
                        raw_df["close"] = raw_df["last_price"]
                        raw_df = raw_df.iloc[::-1].reset_index(drop=True)
            except Exception as e:
                logger.error(f"❌ [{self.strategy_id}] Failed database pull for {symbol}: {str(e)}")
                continue

            if raw_df.empty or len(raw_df) < 30:
                continue

            # Process features using the internal class interface natively
            engineered_df = self.generate_features(raw_df)
            latest_row = engineered_df.iloc[-1]

            current_price = float(latest_row["close"])
            current_rsi = float(latest_row["rsi"])
            lower_band = float(latest_row["bbl"])
            upper_band = float(latest_row["bbu"])

            logger.info(
                f"📈 [MATH MATRIX] {symbol} ──► Price: ${current_price:.2f} | BBL: ${lower_band:.2f} | BBU: ${upper_band:.2f} | RSI: {current_rsi:.2f}"
            )

            # ─── 🚀 PURE MEAN REVERSION EXECUTION GATES ───
            if not has_position:
                if current_price <= lower_band and current_rsi < self.rsi_oversold:
                    logger.info(f"🟢 [MEAN_REV SIGNAL] Pierced Lower Bollinger Band! Buying {symbol}.")
                    return self._create_signal(symbol, action="BUY", qty=int(self.parameters.get("default_qty", 50)))

            elif has_position:
                if current_price >= upper_band or current_rsi > self.rsi_overbought:
                    logger.info(f"🔴 [MEAN_REV SIGNAL] Pierced Upper Bollinger Band! Liquidating {symbol}.")
                    return self._create_signal(symbol, action="SELL", qty=int(position_details["qty"]))

        return None

    def _create_signal(self, symbol: str, action: str, qty: int) -> Dict[str, Any]:
        """Utility construction helper mapping output frames back to core/order_manager.py"""
        current_market_price = float(state_manager.market_data.get(symbol, {}).get("last_price", 0.0))
        return {
            "strategy_id": self.strategy_id,
            "symbol": symbol,
            "action": action,
            "order_type": "MARKET",
            "quantity": qty,
            "limit_price": None,
            "price": current_market_price
        }

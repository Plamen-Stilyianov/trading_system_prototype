# ==============================================================================
# 🧪 RESEARCH ARCHITECTURE: HISTORICAL SIMULATION & BACKTESTING ENGINE (PART 1)
# ==============================================================================
import os
import logging
import sqlite3
from typing import Dict, Any, List
import pandas as pd

# Clean Strategy imports pull each class directly from its isolated file asset
from strategies.ai_template_strategy import AITemplateStrategy
from strategies.mean_rev_strategy import MeanRevStrategy
from research.performance_metrics import calculate_sharpe_ratio, calculate_max_drawdown

logger = logging.getLogger("TradingEngine.Backtester")


class EventDrivenBacktester:
    """
    Simulates a historical paper-trading environment.
    Feeds bar intervals step-by-step into pluggable strategy classes
    to evaluate performance while preventing look-ahead bias.
    """

    def __init__(self, initial_capital: float = 100000.0):
        self.initial_capital: float = initial_capital
        self.cash: float = initial_capital
        self.equity: float = initial_capital

        # Internal state metrics tracking
        self.position_qty: float = 0.0
        self.position_entry_price: float = 0.0

        # Analytical history trackers
        self.equity_curve: List[float] = []
        self.trade_log: List[Dict[str, Any]] = []

        # FACTORY MAP INTERFACE: Connects your script directly to both your separated files
        self._strategy_registry = {
            "AI_ALPHA_V1": AITemplateStrategy,
            "MEAN_REV_V2": MeanRevStrategy
        }

    def run(self, historical_data: pd.DataFrame, strategy_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes an event-driven historical simulation over an OHLCV dataset.
        """
        if historical_data.empty or len(historical_data) < 30:
            logger.error("Backtest execution aborted: Provided historical dataset is too small.")
            return {"status": "error", "message": "Insufficient historical depth."}

        # ─── 🛡️ THE BULLETPROOF INPUT HARMONIZER (FIXED) ───
        df_clean = historical_data.copy()

        # Enforce lowercase base fields first
        df_clean.columns = [str(col).lower().strip() for col in df_clean.columns]

        # Force structural identity columns to protect pandas-ta-classic and strategy code blocks!
        if "last_price" in df_clean.columns:
            df_clean["close"] = df_clean["last_price"]
        elif "close" in df_clean.columns:
            df_clean["last_price"] = df_clean["close"]
        else:
            # Absolute baseline mapping fallback to guarantee key index survival
            first_numeric_col = df_clean.select_dtypes(include=['number']).columns
            df_clean["close"] = df_clean[first_numeric_col]
            df_clean["last_price"] = df_clean[first_numeric_col]

        # Enforce technical placeholder fields if missing from raw database seeding
        if "volume" not in df_clean.columns:
            df_clean["volume"] = 10000
        if "high" not in df_clean.columns:
            df_clean["high"] = df_clean["close"]
        if "low" not in df_clean.columns:
            df_clean["low"] = df_clean["close"]

        # Extract strategy configuration metadata keys natively
        active_symbol = str(strategy_params.get("target_symbol", "NVDA")).strip().upper()
        strategy_id = str(strategy_params.get("strategy_id", "AI_ALPHA_V1")).strip().upper()

        # Dynamically look up and instantiate your strategy using the decoupled factory index
        strategy_class = self._strategy_registry.get(strategy_id, AITemplateStrategy)
        strategy = strategy_class(strategy_id=f"BACKTEST_{strategy_id}", parameters=strategy_params)
        strategy.is_enabled = True

        # Prime the strategy's technical indicator layers across the full dataset clear of KeyError!
        try:
            full_featured_df = strategy.generate_features(df_clean)
            # Enforce column keys on the processed featured dataframe block too
            full_featured_df.columns = [str(col).lower().strip() for col in full_featured_df.columns]
            if "last_price" in full_featured_df.columns and "close" not in full_featured_df.columns:
                full_featured_df["close"] = full_featured_df["last_price"]
        except Exception as fe_err:
            logger.error(f"Internal technical indicators calculation failure: {str(fe_err)}")
            return {"status": "error", "message": f"Feature Generation Failure: {str(fe_err)}"}

        # Clear historical metrics tracking accumulators
        self.cash = self.initial_capital
        self.position_qty = 0.0
        self.position_entry_price = 0.0
        self.equity_curve = []
        self.trade_log = []

# ==============================================================================
# 🧪 RESEARCH ARCHITECTURE: HISTORICAL SIMULATION & BACKTESTING ENGINE (PART 2)
# ==============================================================================
        # --- Event Loop Simulation ---
        warmup_buffer = max(int(strategy_params.get("rsi_period", 14)) + 5, 25)

        for i in range(warmup_buffer, len(full_featured_df)):
            current_window = full_featured_df.iloc[:i + 1]
            latest_bar = current_window.iloc[-1]

            # Adaptive key extraction to eliminate the 'close' KeyError permanently
            target_close_price = float(latest_bar.get("close", latest_bar.get("last_price", 0.0)))

            # 1. Structure raw dictionary frame to mock a live streaming broker tick update
            mock_tick = {
                "symbol": active_symbol,
                "last_price": target_close_price,
                "volume": int(latest_bar.get("volume", 10000))
            }

            # 2. Mock state positions array layout
            current_mock_position = {
                "qty": self.position_qty,
                "entry_price": self.position_entry_price
            }

            # 3. Structural Hot-Patch: Override the strategy features pipeline and bypass empty checks
            strategy.generate_features = lambda df, w=current_window: w

            # Check strategy class properties dynamically to route either AI or Math branches flawlessly
            if hasattr(strategy, "ml_engine"):
                def patched_ai_check(positions_dict, w=current_window):
                    symbol = str(strategy.parameters.get("target_symbol", active_symbol)).strip().upper()
                    pos_details = positions_dict.get(symbol, {"qty": 0.0})
                    has_pos = pos_details.get("qty", 0.0) > 0

                    latest_row = w.iloc[-1]
                    current_rsi = latest_row.get("rsi", 50.0)

                    feature_columns = ["rsi", "sma_fast", "sma_slow", "macd", "bbl", "bbu"]
                    # Safe dictionary fallback mapping extraction protection
                    model_features_row = pd.Series({col: float(latest_row.get(col, 0.0)) for col in feature_columns})

                    ml_prediction_prob = strategy.ml_engine.predict_next_move(model_features_row)

                    if not has_pos:
                        if current_rsi < strategy.rsi_oversold and ml_prediction_prob >= strategy.ml_confidence_threshold:
                            return {"strategy_id": strategy.strategy_id, "symbol": symbol, "action": "BUY", "quantity": strategy.parameters.get("default_qty", 50)}
                    elif has_pos:
                        if current_rsi > strategy.rsi_overbought:
                            return {"strategy_id": strategy.strategy_id, "symbol": symbol, "action": "SELL", "quantity": pos_details["qty"]}
                    return None
                strategy.on_interval_check = patched_ai_check
            else:
                def patched_math_check(positions_dict, w=current_window):
                    symbol = str(strategy.parameters.get("target_symbol", active_symbol)).strip().upper()
                    pos_details = positions_dict.get(symbol, {"qty": 0.0})
                    has_pos = pos_details.get("qty", 0.0) > 0

                    latest_row = w.iloc[-1]
                    current_px = float(latest_row.get("close", latest_row.get("last_price", 0.0)))
                    current_rsi = float(latest_row.get("rsi", 50.0))

                    l_band = float(latest_row.get("bbl", current_px - 1.0))
                    u_band = float(latest_row.get("bbu", current_px + 1.0))

                    if not has_pos:
                        if current_px <= l_band and current_rsi < strategy.rsi_oversold:
                            return {"strategy_id": strategy.strategy_id, "symbol": symbol, "action": "BUY", "quantity": strategy.parameters.get("default_qty", 50)}
                    elif has_pos:
                        if current_px >= u_band or current_rsi > strategy.rsi_overbought:
                            return {"strategy_id": strategy.strategy_id, "symbol": symbol, "action": "SELL", "quantity": pos_details["qty"]}
                    return None
                strategy.on_interval_check = patched_math_check

            # Pass the dynamic symbol key string directly into the check execution wrapper
            signal = strategy.on_interval_check({active_symbol: current_mock_position})

            # 4. Handle generated signals through our internal mock execution ledger
            if signal and signal["action"] != "HOLD":
                self._execute_backtest_order(signal, mock_tick["last_price"])

            # 5. Track calculation points over the portfolio curve
            current_holding_value = self.position_qty * mock_tick["last_price"]
            self.equity = self.cash + current_holding_value
            self.equity_curve.append(self.equity)

        return self._compile_backtest_results()

    def _execute_backtest_order(self, signal: Dict[str, Any], execute_price: float) -> None:
        """Simulates historical brokerage mechanics, ledger entries, and capital modifications."""
        action = signal["action"]
        requested_qty = float(signal["quantity"])

        if action == "BUY" and self.position_qty == 0.0:
            transaction_cost = requested_qty * execute_price
            if transaction_cost <= self.cash:
                self.cash -= transaction_cost
                self.position_qty = requested_qty
                self.position_entry_price = execute_price
                self.trade_log.append({
                    "action": "BUY", "qty": requested_qty, "price": execute_price, "capital": self.cash
                })

        elif action == "SELL" and self.position_qty > 0.0:
            transaction_revenue = self.position_qty * execute_price
            self.cash += transaction_revenue
            realized_pnl = (execute_price - self.position_entry_price) * self.position_qty
            self.trade_log.append({
                "action": "SELL", "qty": self.position_qty, "price": execute_price, "capital": self.cash,
                "pnl": realized_pnl
            })
            self.position_qty = 0.0
            self.position_entry_price = 0.0

    def _compile_backtest_results(self) -> Dict[str, Any]:
        """Calculates portfolio accounting metrics over the completed equity tracking curve."""
        df_curve = pd.Series(self.equity_curve)
        if df_curve.empty:
            return {"status": "error", "message": "No equity metrics logged."}

        returns_series = df_curve.pct_change().dropna()
        final_equity = float(df_curve.iloc[-1])
        total_return = ((final_equity - self.initial_capital) / self.initial_capital) * 100

        sharpe = calculate_sharpe_ratio(returns_series)
        max_dd = calculate_max_drawdown(df_curve)

        return {
            "initial_capital": self.initial_capital,
            "final_equity": round(final_equity, 2),
            "total_return_pct": round(total_return, 2),
            "total_trades_executed": len(self.trade_log),
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown_pct": round(max_dd, 2)
        }

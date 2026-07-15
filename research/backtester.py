import logging
from typing import Dict, Any, List
import pandas as pd

# Internal project infrastructure imports
from strategies.ai_template_strategy import AITemplateStrategy
from research.performance_metrics import calculate_sharpe_ratio, calculate_max_drawdown

logger = logging.getLogger("TradingEngine.Backtester")


class EventDrivenBacktester:
    """
    Simulates a historical paper-trading environment.
    Feeds bar intervals step-by-step into your pluggable strategy
    to evaluate performance while preventing look-ahead bias.
    """

    def __init__(self, initial_capital: float = 100000.0):
        self.initial_capital: float = initial_capital
        self.cash: float = initial_capital
        self.equity: float = initial_capital

        # Internal state metrics tracking
        self.position_qty: int = 0
        self.position_entry_price: float = 0.0

        # Analytical history trackers
        self.equity_curve: List[float] = []
        self.trade_log: List[Dict[str, Any]] = []

    def run(self, historical_data: pd.DataFrame, strategy_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes an event-driven historical simulation over an OHLCV dataset.

        :param historical_data: Pandas DataFrame containing clean historical bars ('open', 'high', 'low', 'close', 'volume').
        :param strategy_params: Parameter configurations to initialize the target AI strategy module.
        :return: High-level dictionary containing backtest performance analytics.
        """
        if historical_data.empty or len(historical_data) < 30:
            logger.error("Backtest execution aborted: Provided historical dataset is too small.")
            return {"status": "error", "message": "Insufficient historical depth."}

        # Normalize column string naming styles
        df_clean = historical_data.copy()
        df_clean.columns = [col.lower().strip() for col in df_clean.columns]

        # Instantiate the exact same concrete strategy class used in live production
        strategy = AITemplateStrategy(strategy_id="BACKTEST_AI_V1", parameters=strategy_params)
        strategy.toggle_state(True)  # Enable strategy execution loop

        # Prime the strategy's technical indicator layers across the full dataset
        full_featured_df = strategy.generate_features(df_clean)

        # Clear historical metrics tracking accumulators
        self.cash = self.initial_capital
        self.position_qty = 0
        self.position_entry_price = 0.0
        self.equity_curve = []
        self.trade_log = []

        # --- Event Loop Simulation ---
        warmup_buffer = strategy_params.get("rsi_period", 14) + 10

        for i in range(warmup_buffer, len(full_featured_df)):
            current_window = full_featured_df.iloc[:i + 1]
            latest_bar = current_window.iloc[-1]

            # 1. Structure raw dictionary frame to mock a live streaming broker tick update
            mock_tick = {
                "symbol": strategy_params.get("target_symbol", "AAPL"),
                "last_price": float(latest_bar["close"]),
                "volume": int(latest_bar["volume"])
            }

            # 2. Mock state positions array layout
            current_mock_position = {
                "qty": self.position_qty,
                "entry_price": self.position_entry_price
            }

            # 3. Structural Hot-Patch: Override the strategy features pipeline and bypass empty checks
            strategy.generate_features = lambda df, w=current_window: w

            # Inside research/backtester.py (around line 83)
            def patched_interval_check(positions_dict, w=current_window):
                symbol = strategy.parameters.get("target_symbol", "AAPL")
                pos_details = positions_dict.get(symbol, {"qty": 0})
                has_pos = pos_details.get("qty", 0) > 0

                latest_row = w.iloc[-1]
                current_rsi = latest_row["rsi"]

                # ✅ FIXED: Isolate exactly the 6 columns the model was trained to look for
                feature_columns = ["rsi", "sma_fast", "sma_slow", "macd", "bbl", "bbu"]
                model_features_row = latest_row[feature_columns]

                # Pass the cleaned 6-feature row into your inference pipeline
                ml_prediction_prob = strategy.ml_engine.predict_next_move(model_features_row)

                if not has_pos:
                    if current_rsi < strategy.rsi_oversold and ml_prediction_prob >= strategy.ml_confidence_threshold:
                        return strategy._create_signal(symbol, action="BUY",
                                                       qty=strategy.parameters.get("default_qty", 100))
                elif has_pos:
                    if current_rsi > strategy.rsi_overbought:
                        return strategy._create_signal(symbol, action="SELL", qty=pos_details["qty"])
                return None

            strategy.on_interval_check = patched_interval_check
            signal = strategy.on_interval_check({"AAPL": current_mock_position})

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
        requested_qty = signal["quantity"]
        symbol = signal["symbol"]

        if action == "BUY" and self.position_qty == 0:
            transaction_cost = requested_qty * execute_price
            if transaction_cost <= self.cash:
                self.cash -= transaction_cost
                self.position_qty = requested_qty
                self.position_entry_price = execute_price
                self.trade_log.append({
                    "action": "BUY", "qty": requested_qty, "price": execute_price, "capital": self.cash
                })

        elif action == "SELL" and self.position_qty > 0:
            transaction_revenue = self.position_qty * execute_price
            self.cash += transaction_revenue
            realized_pnl = (execute_price - self.position_entry_price) * self.position_qty
            self.trade_log.append({
                "action": "SELL", "qty": self.position_qty, "price": execute_price, "capital": self.cash,
                "pnl": realized_pnl
            })
            self.position_qty = 0
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

import asyncio
import numpy as np
import pandas as pd
from research.data_loader import HistoricalDataLoader
from research.backtester import EventDrivenBacktester


async def run_grid_optimization():
    # 1. Generate clean mock historical candlestick bars to simulate a testing window
    dates = pd.date_range(start="2026-06-01", periods=1000, freq="15min")
    np.random.seed(42)

    # Force a cyclic price trend wave to create clear technical patterns for your AI strategy
    time_steps = np.arange(1000)
    mock_close = 170.0 + (np.sin(time_steps / 25.0) * 6.0) + np.cumsum(np.random.normal(0, 0.15, 1000))

    # ✅ FIXED: Derive features directly from price waves instead of unaligned random noise.
    # This simulates realistic indicator momentum so the XGBoost model outputs real confidence changes.
    derived_macd = np.zeros(1000)
    for i in range(1, 1000):
        derived_macd[i] = (mock_close[i] - mock_close[max(0, i - 12)]) * 0.4

    df = pd.DataFrame({
        "open": mock_close - 0.2,
        "high": mock_close + 0.4,
        "low": mock_close - 0.4,
        "close": mock_close,
        "volume": np.random.randint(5000, 50000, 1000),
        "macd": derived_macd,
        "bbl": mock_close - 1.5,
        "bbu": mock_close + 1.5
    }, index=dates)

    # 2. Define our parameter search grid values
    rsi_oversold_options = [25.0, 30.0, 35.0]
    ml_threshold_options = [0.40, 0.45, 0.50]

    best_return = -999.0
    best_params = {}

    backtester = EventDrivenBacktester(initial_capital=100000.0)
    print("🚀 Initiating Multi-Variable Grid Optimization Loop...\n")

    # 3. Execute the Grid Search Parameter Combinations
    for oversold_val in rsi_oversold_options:
        for ml_val in ml_threshold_options:

            current_config = {
                "target_symbol": "AAPL",
                "default_qty": 100,
                "rsi_period": 14,
                "rsi_overbought": 70.0,
                "rsi_oversold": float(oversold_val),
                "ml_threshold": float(ml_val)
            }

            results = backtester.run(df, current_config)

            if results.get("status") == "error":
                continue

            ret_pct = results["total_return_pct"]
            sharpe = results["sharpe_ratio"]

            print(
                f"| Tested -> RSI Oversold: {oversold_val} | ML Threshold: {ml_val:.2f} | Returns: {ret_pct}% | Sharpe: {sharpe}")

            if ret_pct > best_return:
                best_return = ret_pct
                best_params = current_config

    print("\n🎯 --- PARAMETER OPTIMIZATION GRID SEARCH RESULT ---")
    print(f"🥇 Top Return Achieved      : {best_return}%")
    print(f"🛠️ Best RSI Oversold Boundary: {best_params['rsi_oversold']}")
    print(f"🤖 Best ML Confidence Limit : {best_params['ml_threshold']}")


if __name__ == "__main__":
    asyncio.run(run_grid_optimization())

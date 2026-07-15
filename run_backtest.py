# run_backtest.py
import asyncio
import pandas as pd
from config.settings import settings
from research.data_loader import HistoricalDataLoader
from research.backtester import EventDrivenBacktester


async def execute_sandbox_run():
    # 1. Initialize data utility components
    loader = HistoricalDataLoader(
        api_key=settings.BROKER_API_KEY,
        secret_key=settings.BROKER_SECRET_KEY,
        use_paper=True
    )

    # Generate temporary mock historical price variables
    # (Or replace with: df = loader.load_from_csv("path/to/historical_data.csv"))
    dates = pd.date_range(start="2026-01-01", periods=100, freq="5min")
    mock_data = {
        "open": [170.0 + i * 0.1 for i in range(100)],
        "high": [171.0 + i * 0.1 for i in range(100)],
        "low": [169.0 + i * 0.1 for i in range(100)],
        "close": [170.5 + i * 0.1 for i in range(100)],
        "volume": [1000 + i for i in range(100)]
    }
    df = pd.DataFrame(mock_data, index=dates)

    # 2. Fire up the Backtest framework
    backtester = EventDrivenBacktester(initial_capital=100000.0)

    strategy_config = {
        "target_symbol": "AAPL",
        "default_qty": 100,
        "rsi_period": 14,
        "rsi_overbought": 70.0,
        "rsi_oversold": 30.0,
        "ml_threshold": 0.60
    }

    results = backtester.run(df, strategy_config)

    # 3. Print out statistical analytics metrics
    print("\n📈 --- ALGORITHMIC BACKTEST HISTORICAL PERFORMANCE REPORT ---")
    print(f"Initial Starting Capital : ${results['initial_capital']:,.2f}")
    print(f"Final Ending Equity      : ${results['final_equity']:,.2f}")
    print(f"Total Strategy Returns   : {results['total_return_pct']}%")
    print(f"Executed Order Fills Count: {results['total_trades_executed']}")
    print(f"Annualized Sharpe Ratio  : {results['sharpe_ratio']}")
    print(f"Maximum Peak Drawdown    : {results['max_drawdown_pct']}%")


if __name__ == "__main__":
    asyncio.run(execute_sandbox_run())

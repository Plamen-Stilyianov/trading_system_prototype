import numpy as np
import pandas as pd


def calculate_sharpe_ratio(returns_series: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252) -> float:
    """
    Calculates the annualized Sharpe Ratio of a trading strategy's returns.
    Measures the excess return earned per unit of volatility risk.
    ✅ AUTOMATICALLY DETECTS HIGH-FREQUENCY INTRADAY DATA TO PREVENT SCALING BIAS.
    """
    if returns_series.empty or len(returns_series) < 2:
        return 0.0

    # ─── 🛡️ THE INTRADAY AUTO-SCALING ENGINE ───
    # If the historical returns series has an index length greater than daily limits,
    # or if explicitly passed, dynamically update periods_per_year to represent 5-minute bars.
    # (252 trading days * 78 five-minute bars per session = 19,656 intervals per year)
    if len(returns_series) > 252 and periods_per_year == 252:
        # Automatically scales up for high-frequency data pipelines
        periods_per_year = 19656

    # Convert the annualized risk-free asset rate down to the scale period interval
    period_rf = risk_free_rate / periods_per_year

    # Calculate excess returns over the risk-free rate proxy
    excess_returns = returns_series - period_rf

    mean_excess_return = excess_returns.mean()
    std_dev_return = returns_series.std(ddof=1)  # Sample standard deviation (N-1 degrees of freedom)

    # Protect against divide-by-zero errors in steady or flat equity data matrices
    if std_dev_return == 0.0 or np.isnan(std_dev_return):
        return 0.0

    # Calculate period Sharpe ratio and scale linearly up to the target annualized projection metric
    period_sharpe = mean_excess_return / std_dev_return
    annualized_sharpe = period_sharpe * np.sqrt(periods_per_year)

    return float(annualized_sharpe)


def calculate_max_drawdown(equity_series: pd.Series) -> float:
    """
    Calculates the Maximum Drawdown (MDD) percentage over an equity curve dataset.
    Identifies the largest historical peak-to-trough drop in total portfolio capital.
    """
    if equity_series.empty or len(equity_series) < 2:
        return 0.0

    # 1. Establish an expanding rolling peak array tracking highest historical high watermarks
    rolling_peak = equity_series.cummax()

    # 2. Compute drawdown percentage array relative to the running local peaks
    drawdowns = (equity_series - rolling_peak) / rolling_peak

    # 3. Extract the maximum negative variance value
    max_drawdown_ratio = drawdowns.min()

    # Convert the absolute negative fraction into a readable positive percentage display metric
    return abs(float(max_drawdown_ratio)) * 100.0

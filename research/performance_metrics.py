import numpy as np
import pandas as pd


def calculate_sharpe_ratio(returns_series: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252) -> float:
    """
    Calculates the annualized Sharpe Ratio of a trading strategy's returns.
    Measures the excess return earned per unit of volatility risk.

    :param returns_series: A Pandas Series of percentage returns (daily or per-bar interval).
    :param risk_free_rate: The baseline annualized risk-free rate (e.g., 0.02 for 2%). Default is 0.0.
    :param periods_per_year: The number of trading bars in an annualized timeframe. Default is 252 (daily).
    :return: Annualized Sharpe Ratio float value. Returns 0.0 if standard deviation is zero or empty.
    """
    if returns_series.empty or len(returns_series) < 2:
        return 0.0

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

    :param equity_series: A Pandas Series tracking the sequential absolute value of account equity.
    :return: Maximum drawdown expressed as a positive percentage float (e.g., 12.5 for -12.5%).
             Returns 0.0 if data is invalid or empty.
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

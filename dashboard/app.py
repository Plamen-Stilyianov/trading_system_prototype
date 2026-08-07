# ==============================================================================
# 🏢 UNIVERSAL TRADING DESK: PRODUCTION FRONTEND COMMAND DASHBOARD (PART 1)
# ==============================================================================
import os
import time
import sqlite3
import requests
import streamlit as st
import pandas as pd

from config.settings import settings

# ==============================================================================
# 📊 LAYER 0: INITIALISATION INITIALISER (MUST BE THE ABSOLUTE FIRST CALL)
# ==============================================================================
st.set_page_config(page_title="Universal Trading Desk", layout="wide", page_icon="🏢")

# ==============================================================================
# 🔌 LAYER 1: MULTI-ENVIRONMENT DEPLOYMENT ROUTING ENGINE
# ==============================================================================
APP_ENV = os.getenv("APP_ENV", "development").lower()
BACKEND_HOST = os.getenv("BACKEND_HOST")
BACKEND_PORT = os.getenv("BACKEND_PORT", "8080")

# Enforce the absolute directory target directly at the configuration level
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "/app_storage/trading_data.db")

if not BACKEND_HOST:
    if APP_ENV == "development":
        BACKEND_HOST = "backend-daemon"
    else:
        st.error("❌ CRITICAL INFRASTRUCTURE FAULT: 'BACKEND_HOST' environment variable is missing on OCI.")
        st.stop()

# Formulate definitive REST routing targets matching your backend active routers
STATE_API_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/state"
TUNING_API_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/v1/engine/tuning"
TOGGLE_API_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/v1/engine/toggle"

# ─── 📡 LAYER 1.5: TELEMETRY HEARTBEAT AUTO-REFRESH (HARDENED) ───
from streamlit_autorefresh import st_autorefresh

if "refresh_counter" not in st.session_state:
    st.session_state.refresh_counter = 0

st.session_state.refresh_counter += 1

# Safely triggers an un-throttled canvas re-evaluation every 5000ms (5 seconds)
refresh_counter = st_autorefresh(interval=5000, limit=None, key="production_isolated_telemetry_pulse")

# ==============================================================================
# 📊 LAYER 2: CANVAS INITIALISATION & TELEMETRY SYNCHRONISATION
# ==============================================================================
st.title("🏢 Production Equity Trading Command Dashboard")
st.caption(f"**Runtime Deployment Mode:** {APP_ENV.upper()}  |  **Target Mesh Gateway:** {BACKEND_HOST}:{BACKEND_PORT}")
st.markdown("---")

@st.cache_data(ttl=2, show_spinner=False)
def fetch_runtime_discovery_payload():
    try:
        res = requests.get(STATE_API_URL, timeout=1.5)
        if res.status_code == 200:
            return res.json()
    except requests.exceptions.RequestException:
        return None
    return None

engine_payload = fetch_runtime_discovery_payload()

if engine_payload is None:
    st.sidebar.error("🚨 Critical Error: Backend Daemon API Offline. Check container network routing.")
    st.sidebar.info(f"Target Destination URI: `{STATE_API_URL}`")

    live_backend_queue = []
    saved_params = {"rsi_period": 14, "rsi_oversold": 30, "rsi_overbought": 70, "ml_confidence_threshold": 0.65, "default_qty": 50}
    saved_registry = {}
    metrics = {"cash_balance": 0.0, "portfolio_value": 0.0, "daily_pnl": 0.0, "roi_percentage": 0.0, "active_positions_count": 0}
    positions = {}
    is_engine_active = False
else:
    is_engine_active = engine_payload.get("is_active", False)
    positions = engine_payload.get("positions", {})
    metrics = engine_payload.get("summary", {})
    live_backend_queue = engine_payload.get("tracked_symbols", [])
    saved_params = engine_payload.get("saved_parameters", {})
    saved_registry = engine_payload.get("saved_registry", {})

    cash_balance = float(metrics.get("cash_balance", 0.0))
    portfolio_value = float(metrics.get("portfolio_value", 0.0))
    daily_pnl = float(metrics.get("daily_pnl", 0.0))
    roi_percentage = float(metrics.get("roi_percentage", 0.0))
    active_positions_count = int(metrics.get("active_positions_count", 0))

# ─── 🎛️ CREATE THE PRIMARY VIEWPORT SPLITTER TABS ───
tab_live, tab_backtest = st.tabs(["📡 Live System Telemetry", "🧪 Quantitative Simulation Sandbox"])

# ==============================================================================
# 🛰️ WORKSPACE TAB 1: LIVE STREAM ACCOUNT AGENT (PART 2)
# ==============================================================================
with tab_live:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Portfolio Value", value=f"${portfolio_value:,.2f}", delta=f"${daily_pnl:+,.2f} Today")
    with col2:
        st.metric(label="Available Cash Balance", value=f"${cash_balance:,.2f}")
    with col3:
        st.metric(label="Active Risk Open Positions", value=f"{active_positions_count} Stocks",
                  delta=f"{roi_percentage:+.2f}% ROI")

    st.subheader("👁️ Live Execution Watchlist Queue Focus")
    st.info(
        f"**The background strategy engine is actively processing calculation iterations for:** {', '.join(live_backend_queue)}")

    if "portfolio_registry" in st.session_state:
        current_ui_enabled = [s for s, m in st.session_state.portfolio_registry.items() if m["enabled"]]
        if set(current_ui_enabled) != set(live_backend_queue):
            st.warning(
                "⚠️ Workspace changes staged. Click 'SAVE & APPLY PORTFOLIO TUNING' to sync configurations and commit adjustments down onto database structures.")

    st.markdown("---")

    # ─── 🎛️ SUB-TABS: SPLITTING EXPOSURE FROM AUDIT RECORDS ───
    sub_tab_exposure, sub_tab_activities = st.tabs(["💼 Current Positions Exposure", "📜 Historical Activities Ledger"])

    # ──────────────────────────────────────────────────────────────────────────
    # 💼 SUB-TAB A: LOCAL POSITION RUNNING AVERAGE EXPOSURE
    # ──────────────────────────────────────────────────────────────────────────
    with sub_tab_exposure:
        st.subheader("💼 Active Inventory Open Positions")
        st.caption(f"⚡ Live Local Ledger Stream | Last UI Sync Pass: #{st.session_state.refresh_counter}")

        try:
            with sqlite3.connect(SQLITE_DB_PATH) as conn:
                # ─── 📊 THE MATHEMATICAL RUNNING AVERAGE LOOP ───
                exposure_query = """
                                 SELECT symbol                                                                      AS [Asset Ticker], \
                                        SUM(CASE \
                                                WHEN action = 'BUY' THEN quantity \
                                                WHEN action = 'SELL' THEN -quantity \
                                                ELSE 0 END)                                                         AS [Net Shares Owned], \
                                        ROUND(SUM(CASE WHEN action = 'BUY' THEN quantity * execution_price ELSE 0 END) / \
                                              NULLIF(SUM(CASE WHEN action = 'BUY' THEN quantity ELSE 0 END), 0), \
                                              4)                                                                    AS [Weighted Avg Price], \
                                        ROUND(SUM(CASE \
                                                      WHEN action = 'BUY' THEN quantity \
                                                      WHEN action = 'SELL' THEN -quantity \
                                                      ELSE 0 END) * \
                                              AVG(execution_price), \
                                              2)                                                                    AS [Current Valuation]
                                 FROM trade_receipts
                                 WHERE status = 'FILLED'
                                 GROUP BY symbol
                                 HAVING [Net Shares Owned] > 0 \
                                 """
                exposure_df = pd.read_sql_query(exposure_query, conn)
        except Exception as e:
            st.error(f"UI Dynamic Weighted Average Calculation Loop failed: {str(e)}")
            exposure_df = pd.DataFrame()

        if not exposure_df.empty:
            st.dataframe(
                exposure_df,
                column_config={
                    "Asset Ticker": "Asset Ticker",
                    "Net Shares Owned": st.column_config.NumberColumn("Net Shares Owned", format="%.2f shares",
                                                                      width="medium"),
                    "Weighted Avg Price": st.column_config.NumberColumn("Weighted Avg Entry Price", format="$%.2f",
                                                                        width="medium"),
                    "Current Valuation": st.column_config.NumberColumn("Total Valuation", format="$%.2f",
                                                                       width="medium")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.caption("No open position risk exposures tracked across the running system core mesh layer.")

    # ──────────────────────────────────────────────────────────────────────────
    # 📜 SUB-TAB B: SCROLLABLE ACTIVITIES LEDGER (READS DIRECTLY FROM LOCAL SQLITE)
    # ──────────────────────────────────────────────────────────────────────────
    with sub_tab_activities:
        st.subheader("📜 Historical Activities Transaction Ledger")
        st.caption("Scrollable database record audit log file parsing your local physical platter tables [INDEX].")

        try:
            with sqlite3.connect(SQLITE_DB_PATH) as conn:
                activities_query = "SELECT id, timestamp, symbol, action, quantity, execution_price, status FROM trade_receipts ORDER BY timestamp DESC"
                activities_df = pd.read_sql_query(activities_query, conn)
        except Exception:
            activities_df = pd.DataFrame()

        if not activities_df.empty:
            st.dataframe(
                activities_df,
                column_config={
                    "id": st.column_config.TextColumn("Transaction Token ID", width="medium"),
                    "timestamp": st.column_config.TextColumn("Execution Date/Time", width="medium"),
                    "symbol": "Asset Ticker",
                    "action": "Type",
                    "quantity": st.column_config.NumberColumn("Shares", format="%.2f"),
                    "execution_price": st.column_config.NumberColumn("Price", format="$%.2f"),
                    "status": "State"
                },
                height=300,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.caption("No physical execution tickets logged inside the local storage file.")


# ==============================================================================
# 🧪 WORKSPACE TAB 2: OFFLINE SYSTEM TIME-MACHINE SIMULATOR (PART 3)
# ==============================================================================
with tab_backtest:
    st.subheader("🔬 Historical Backtest Simulation Engine")
    st.caption("Runs your exact detached strategy class logic components over deep database rows.")

    col_bt1, col_bt2 = st.columns(2)
    with col_bt1:
        bt_strategy = st.selectbox("Select Backtest Strategy Focus:", ["AI_ALPHA_V1", "MEAN_REV_V2"])
    with col_bt2:
        bt_symbol = st.selectbox("Target Simulation Asset Ticker:", ["AAPL", "NVDA", "SPY"])

    if st.button("🚀 EXECUTE OFFLINE HISTORICAL BACKTEST"):
        with st.spinner("Streaming lookback records matrix from persistent drive folder platter..."):
            try:
                from research.backtester import EventDrivenBacktester

                with sqlite3.connect(SQLITE_DB_PATH) as conn:
                    historical_ticks_df = pd.read_sql_query(
                        f"SELECT * FROM historical_ticks WHERE symbol='{bt_symbol}'", conn
                    )

                if historical_ticks_df.empty or len(historical_ticks_df) < 30:
                    st.error(f"❌ Aborted: Local storage lacks pre-seeded candle depth for {bt_symbol}.")
                else:
                    test_params = {
                        "strategy_id": bt_strategy,
                        "target_symbol": bt_symbol,
                        "rsi_period": int(saved_params.get("rsi_period", 14)),
                        "rsi_oversold": float(saved_params.get("rsi_oversold", 30.0)),
                        "rsi_overbought": float(saved_params.get("rsi_overbought", 70.0)),
                        "ml_confidence_threshold": float(saved_params.get("ml_confidence_threshold", 0.55)),
                        "default_qty": int(saved_params.get("default_qty", 50))
                    }

                    backtester = EventDrivenBacktester()
                    results = backtester.run(historical_ticks_df, test_params)

                    st.success(f"🏁 Historical Backtest Simulation Completed for {bt_symbol}!")

                    m_col1, m_col2, m_col3 = st.columns(3)
                    with m_col1:
                        st.metric(
                            label="Final Sandbox Equity",
                            value=f"${results.get('final_equity', 100000.0):,.2f}",
                            delta=f"${results.get('final_equity', 100000.0) - results.get('initial_capital', 100000.0):+,.2f} Net"
                        )
                    with m_col2:
                        st.metric(label="Net Simulation Return", value=f"{results.get('total_return_pct', 0.0):+,.2f}%")
                    with m_col3:
                        st.metric(label="Total Trades Executed", value=f"{results.get('total_trades_executed', 0)} Trades")

                    r_col1, res_col2 = st.columns(2)
                    with r_col1:
                        st.metric(label="Risk-Adjusted Sharpe Ratio", value=f"{results.get('sharpe_ratio', 0.0):.2f}")
                    with res_col2:
                        st.metric(label="Maximum Drawdown Profile", value=f"{results.get('max_drawdown_pct', 0.0):.2f}%",
                                  delta="Worst Case Peak-to-Trough", delta_color="inverse")

                    st.subheader("📜 Simulated Historical Execution Ledger")
                    simulated_trades_list = getattr(backtester, "trade_log", [])

                    if simulated_trades_list:
                        trade_log_df = pd.DataFrame(simulated_trades_list)
                        st.dataframe(
                            trade_log_df,
                            column_config={
                                "action": st.column_config.TextColumn("Action Type"),
                                "qty": st.column_config.NumberColumn("Shares Sizing", format="%.2f shares"),
                                "price": st.column_config.NumberColumn("Execution Price", format="$%.2f"),
                                "capital": st.column_config.NumberColumn("Remaining Cash Pool", format="$%.2f"),
                                "pnl": st.column_config.NumberColumn("Realised Profit/Loss", format="$%.2f")
                            },
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.caption("No historical trade events triggered across this simulation window parameter frame.")
            except Exception as bt_err:
                st.error(f"Backtester Runtime Failure: {str(bt_err)}")

# ==============================================================================
# 💾 LAYER 4: DATABASE DISCOVERY & PERSISTENT CHECKBOX MATRIX (PART 4)
# ==============================================================================
def discover_symbols_from_database(db_path: str) -> list[str]:
    """Queries the true persistence schema to find your true locked asset matrix."""
    if not os.path.exists(db_path):
        return list(settings.DEFAULT_WATCHLIST)
    try:
        with sqlite3.connect(db_path) as conn:
            query = "SELECT DISTINCT symbol FROM historical_ticks"
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                return [str(s).strip().upper() for s in df["symbol"].tolist()]
            return list(settings.DEFAULT_WATCHLIST)
    except Exception:
        return list(settings.DEFAULT_WATCHLIST)

discovered_db_tickers = discover_symbols_from_database(SQLITE_DB_PATH)

if "portfolio_registry" not in st.session_state:
    st.session_state.portfolio_registry = {}

for symbol in discovered_db_tickers:
    if symbol not in st.session_state.portfolio_registry:
        if symbol in saved_registry:
            st.session_state.portfolio_registry[symbol] = {"enabled": saved_registry[symbol]}
        else:
            st.session_state.portfolio_registry[symbol] = {"enabled": symbol in live_backend_queue}

# ==============================================================================
# 🎛️ LAYER 3 & 5: SIDEBAR MASTER SYSTEM CONTROL TOGGLE SWITCHES & FORM
# ==============================================================================
with st.sidebar:
    st.header("🚀 Execution Control")

    if is_engine_active:
        if st.button("🛑 STOP TRADING ENGINE", use_container_width=True, type="primary", key="stop_engine_btn"):
            try:
                res = requests.post(TOGGLE_API_URL, json={"active": False}, timeout=1.5)
                if res.status_code == 200:
                    st.toast("🛑 Trading Engine deactivated and set to IDLE.")
                    st.cache_data.clear()
                    time.sleep(0.3)
                    st.rerun()
            except Exception as e:
                st.error(f"Failed to transmit stop command: {str(e)}")
    else:
        if st.button("🟢 START TRADING ENGINE", use_container_width=True, key="start_engine_btn"):
            try:
                res = requests.post(TOGGLE_API_URL, json={"active": True}, timeout=1.5)
                if res.status_code == 200:
                    st.toast("🚀 Trading Engine activated! Live data feeds engaged.")
                    st.cache_data.clear()
                    time.sleep(0.3)
                    st.rerun()
            except Exception as e:
                st.error(f"Failed to transmit start command: {str(e)}")

    st.markdown(f"**Engine Pulse Status:** {'🟢 RUNNING' if is_engine_active else '🔴 INITIALISED / IDLE'}")
    st.markdown("---")

    with st.form("portfolio_matrix_tuning_form"):
        st.subheader("📋 Persistent Stocks Registry")
        registry_keys = sorted(list(st.session_state.portfolio_registry.keys()))

        for symbol in registry_keys:
            asset_data = st.session_state.portfolio_registry[symbol]
            col_sym, col_toggle = st.columns(2)
            with col_sym:
                st.markdown(f"**🏢 {symbol}**")
            with col_toggle:
                asset_data["enabled"] = st.checkbox(
                    label="Active", value=asset_data["enabled"], key=f"tg_db_{symbol}", label_visibility="collapsed"
                )

        st.markdown("---")
        st.subheader("📊 Strategy Parameter Tuning")

        available_models = ["AI_XGBOOST_V1 (Standard)", "MEAN_REVERSION_V2 (Overbought)"]
        chosen_model_label = st.selectbox(label="Select Active Strategy Engine:", options=available_models)

        strategy_name_token = "AI_XGBOOST" if "AI_XGBOOST" in chosen_model_label else "MEAN_REVERSION"
        strategy_id_token = "AI_ALPHA_V1" if "AI_XGBOOST" in chosen_model_label else "MEAN_REV_V2"

        rsi_period = st.slider("RSI Lookback Window Size", 5, 30, int(saved_params.get("rsi_period", 14)))
        rsi_oversold = st.slider("RSI Oversold Buy Line", 15, 45, int(float(saved_params.get("rsi_oversold", 30.0))))
        rsi_overbought = st.slider("RSI Overbought Sell Line", 55, 85, int(float(saved_params.get("rsi_overbought", 70.0))))
        ml_confidence = st.slider("XGBoost Confidence Floor", 0.50, 0.95, float(saved_params.get("ml_confidence_threshold", 0.65)), 0.05)
        default_qty = st.number_input("Order Size (Whole Shares Only)", min_value=1, value=int(saved_params.get("default_qty", 50)))

        apply_tuning = st.form_submit_button("🔥 SAVE & APPLY PORTFOLIO TUNING", use_container_width=True)

        if apply_tuning:
            staged_registry_map = {symbol: data["enabled"] for symbol, data in st.session_state.portfolio_registry.items()}
            active_count = sum(1 for val in staged_registry_map.values() if val)

            if active_count == 0:
                st.error("❌ Operational safety fault: Engine queue requires at least 1 enabled stock ticker.")
            else:
                payload = {
                    "strategy_name": strategy_name_token,
                    "strategy_id": strategy_id_token,
                    "registry_map": staged_registry_map,
                    "rsi_period": int(rsi_period),
                    "rsi_oversold": float(rsi_oversold),
                    "scheduler_step": 1,
                    "rsi_overbought": float(rsi_overbought),
                    "ml_confidence_threshold": float(ml_confidence),
                    "default_qty": int(default_qty)
                }
                try:
                    res = requests.post(TUNING_API_URL, json=payload, timeout=3.0)
                    if res.status_code == 200:
                        st.sidebar.success("💾 Workspace Session Saved to Database!")
                        st.cache_data.clear()
                        time.sleep(0.3)
                        st.rerun()
                    else:
                        st.sidebar.error(f"Backend rejected execution payload ({res.status_code}): {res.text}")
                except Exception as e:
                    st.sidebar.error(f"Transmission network error: {str(e)}")

    st.markdown(
        """
        <style>
            div[data-testid="stForm"] { margin-bottom: 0px !important; }
        </style>
        """,
        unsafe_allow_html=True
    )

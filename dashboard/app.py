import os
import sys
import time
import sqlite3
import requests
import streamlit as st
import pandas as pd

from config.settings import settings

# ==============================================================================
# 🔌 LAYER 1: MULTI-ENVIRONMENT DEPLOYMENT ROUTING ENGINE
# ==============================================================================
APP_ENV = os.getenv("APP_ENV", "development").lower()
BACKEND_HOST = os.getenv("BACKEND_HOST")
BACKEND_PORT = os.getenv("BACKEND_PORT", "8080")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "logs/trading_data.db")

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

# ==============================================================================
# 📊 LAYER 2: APP CANVAS INITIALISATION & TELEMETRY SYNCHRONISATION
# ==============================================================================
st.set_page_config(page_title="Universal Trading Desk", layout="wide", page_icon="🏢")
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

# ✅ AFTER (Corrected to target your live backend JSON keys):
if engine_payload is None:
    st.sidebar.error("🚨 Critical Error: Backend Daemon API Offline. Check container network routing.")
    st.sidebar.info(f"Target Destination URI: `{STATE_API_URL}`")

    live_backend_queue = []
    saved_params = {"rsi_period": 14, "rsi_oversold": 30, "rsi_overbought": 70, "ml_confidence_threshold": 0.65,
                    "default_qty": 50}
    saved_registry = {}
    metrics = {"cash_balance": 0.0, "portfolio_value": 0.0, "daily_pnl": 0.0, "roi_percentage": 0.0,
               "active_positions_count": 0}
    positions = {}
    is_engine_active = False
else:
    # ─── 🎛️ ALIGNS DIRECTLY WITH YOUR ACTIVE MAIN.PY KEY RESPONSES ───
    is_engine_active = engine_payload.get("is_active", False)  # ◄── TARGETS 'is_active' NATIVELY
    positions = engine_payload.get("positions", {})
    metrics = engine_payload.get("summary", {})
    live_backend_queue = engine_payload.get("tracked_symbols", [])
    saved_params = engine_payload.get("saved_parameters", {})
    saved_registry = engine_payload.get("saved_registry", {})

# ==============================================================================
# 🎛️ LAYER 3: SIDEBAR MASTER SYSTEM CONTROL TOGGLE SWITCHES
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


# ==============================================================================
# 💾 LAYER 4: DATABASE DISCOVERY & PERSISTENT CHECKBOX MATRIX
# ==============================================================================
def discover_symbols_from_database(db_path: str) -> list[str]:
    """Queries the true persistence schema to find your true locked asset matrix."""

    # ✅ FIX: Removed hardcoded array! Now drops back safely to your settings module defaults.
    if not os.path.exists(db_path):
        return list(settings.DEFAULT_WATCHLIST)

    try:
        with sqlite3.connect(db_path) as conn:
            query = "SELECT symbol FROM workspace_asset_registry"
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                return [str(s).strip().upper() for s in df["symbol"].tolist()]

            # Secondary fallback to settings if the workspace table rows are empty
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
# ⚙️ LAYER 5: SIDEBAR STRATEGY TUNING CONTROL HUB (BUFFERED FORM)
# ==============================================================================
with st.sidebar:
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

        # 🧠 DYNAMIC DROPDOWN STRATEGY SWITCH WIDGET
        available_models = ["AI_XGBOOST_V1 (Standard)", "MEAN_REVERSION_V2 (Overbought)"]
        chosen_model_label = st.selectbox(label="Select Active Strategy Engine:", options=available_models)

        # Extract type-safe strategy identifiers matching backend PersistentTuningSchema fields
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
                # ✅ FIXED: Now cleanly routes strategy identifiers down into your Pydantic schema
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

# ==============================================================================
# 🏢 LAYER 6: MAIN SCREEN TELEMETRY VIEWS & SYNCHRONISATION CHECKERS
# ==============================================================================
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Total Portfolio Value", value=f"${metrics.get('portfolio_value', 0.00):,.2f}",
              delta=f"${metrics.get('daily_pnl', 0.00):+,.2f}")
with col2:
    st.metric(label="Available Cash Balance", value=f"${metrics.get('cash_balance', 0.00):,.2f}")
with col3:
    st.metric(label="Active Risk Open Positions", value=f"{metrics.get('active_positions_count', 0)} Stocks",
              delta=f"{metrics.get('roi_percentage', 0.00):.2f}% ROI")

st.markdown("---")
st.subheader("👁️ Live Execution Watchlist Queue Focus")
st.info(
    f"**The background strategy engine is actively processing calculation iterations for:** {', '.join(live_backend_queue)}")

current_ui_enabled = [s for s, m in st.session_state.portfolio_registry.items() if m["enabled"]]
if set(current_ui_enabled) != set(live_backend_queue):
    st.warning(
        "⚠️ Workspace changes staged. Click 'SAVE & APPLY PORTFOLIO TUNING' to sync configurations and commit adjustments down onto database structures.")

st.markdown("---")
st.subheader("💼 Active Inventory Open Positions")
if positions:
    st.dataframe(pd.DataFrame.from_dict(positions, orient='index'), use_container_width=True)
else:
    st.caption("No open position risk exposures tracked across the running system core mesh layer.")

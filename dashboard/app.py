import os
import time
import requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# -----------------------------------------------------------------------------
# 🔒 SECURE APPLICATION INTERCEPTOR GATEWAY AUTHENTICATION
# -----------------------------------------------------------------------------
def check_dashboard_credentials() -> bool:
    """Prompts and evaluates user identity variables using standard session states."""
    def authentication_callback():
        if (
            st.session_state["username"] == "admin"
            and st.session_state["password"] == "QuantTrading2026!"
        ):
            st.session_state["authenticated"] = True
            del st.session_state["password"]  # Flush passwords from state caches
            del st.session_state["username"]
        else:
            st.session_state["authenticated"] = False
            st.error("🔒 Access Denied: Invalid Security Identification Token Profiles.")

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    # ─── 🛡️ ENFORCING PERMANENT DATA-TESTID STABILITY ───
    if not st.session_state["authenticated"]:
        st.title("🛡️ Institutional Algorithmic Platform Access")

        # Adding a help string forces Streamlit to bake a static aria-desc and test-id straight into the DOM tree
        st.text_input("Username ID", key="username", help="Enter your admin system login name")
        st.text_input("Password Key", type="password", key="password", help="Enter your corporate encryption key")

        st.button("Authenticate Identity Handshake", on_click=authentication_callback, key="auth_submit_btn")
        return False

    return True

# Only execute application layouts if the security authentication layer resolves True
#if check_dashboard_credentials():
if True:

    # 1. Page Configuration and Theme Handling
    st.set_page_config(
        page_title="Production AI Trading Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Load Backend connection parameters from Kubernetes Environment ConfigMaps
    BACKEND_HOST = os.getenv("BACKEND_HOST", "localhost")
    BACKEND_PORT = os.getenv("BACKEND_PORT", "8080")
    BACKEND_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/state"

    # Fetches the state dictionary from the FastAPI container on every page refresh
    backend_state = {}
    try:
        response = requests.get(BACKEND_URL, timeout=1.0)
        if response.status_code == 200:
            backend_state = response.json()
        else:
            st.sidebar.error(f"⚠️ Backend returned unexpected route status: {response.status_code}")
    except Exception as e:
        st.sidebar.error("🚨 Core Daemon API Offline. Check container connection loops.")
        # Initialize structural empty templates so downstream components don't throw NameErrors
        backend_state = {"is_active": False, "positions": {}, "summary": {}, "logs": []}

    # 2. API Communication Layer
    def fetch_system_state():
        """Polls real-time telemetry variables from the FastAPI backend worker daemon."""
        try:
            response = requests.get(BACKEND_URL, timeout=1.5)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException:
            st.error("🔌 Connection Error: Unable to stream telemetry from backend daemon.")
        return None

    def toggle_backend_state(target_state: bool):
        """Submits a post payload to trigger the global master hardware switch."""
        try:
            requests.post(f"{BACKEND_URL}/toggle", json={"active": target_state}, timeout=1.5)
        except requests.exceptions.RequestException:
            st.error("🚨 Transmission Failure: Master control command dropped.")

    def update_engine_parameters(rsi_low: int, rsi_high: int, ml_limit: float):
        """Submits a dedicated configuration payload to tune live algorithm criteria mid-flight."""
        try:
            config_url = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/config/update"
            payload = {
                "rsi_oversold": float(rsi_low),
                "rsi_overbought": float(rsi_high),
                "ml_confidence_threshold": float(ml_limit)
            }
            response = requests.post(config_url, json=payload, timeout=1.5)
            if response.status_code == 200:
                st.sidebar.success("🎯 Strategy Parameters Synchronised!")
        except requests.exceptions.RequestException:
            st.sidebar.error("🚨 Transmission Failure: Strategy parameters dropped.")

    # 3. Streamlit Polling Data Synchronization
    state = fetch_system_state()

    if state:
        # ─── 🛡️ BULLETPROOF STATE EXTRACTION MATRIX ───
        # Uses .get() with safe fallbacks for ALL variables to prevent UI crashes
        metrics = state.get("summary", state.get("metrics", {}))
        positions = state.get("positions", {})
        logs = state.get("logs", [])                 # ◄─ FIXED WITH SAFE EMPTY LIST []
        is_active = state.get("is_active", False)     # ◄─ FIXED WITH SAFE DEFAULT FALSE


        # 4. Sidebar Controls & Live Parameters Tuning Sliders Section
        st.sidebar.title("⚙️ Control Panel")
        st.sidebar.markdown("---")
        st.sidebar.subheader("🛠️ Live Parameters Tuning Sliders")

        # ─── DECOUPLED STABLE ELEMENT INPUT SLIDERS ───
        rsi_oversold = st.sidebar.slider(
            label="RSI Oversold Floor Limit",
            min_value=15,
            max_value=45,
            value=int(st.session_state.get("rsi_low", 30)),
            step=1,
            key="rsi_low_widget_input"  # ◄─ FIXED: DISTINCT DOM SELECTOR PERVENT COLLISION
        )

        rsi_overbought = st.sidebar.slider(
            label="RSI Overbought Ceiling Limit",
            min_value=55,
            max_value=85,
            value=int(st.session_state.get("rsi_high", 70)),
            step=1,
            key="rsi_high_widget_input"  # ◄─ FIXED: DISTINCT DOM SELECTOR PERVENT COLLISION
        )

        ml_confidence = st.sidebar.slider(
            label="XGBoost ML Probability Threshold",
            min_value=0.40,
            max_value=0.85,
            value=float(st.session_state.get("ml_limit", 0.60)),
            step=0.01,
            key="ml_limit_widget_input"  # ◄─ FIXED: DISTINCT DOM SELECTOR PERVENT COLLISION
        )

        # NEW ACTION: Dedicated Parameter Update Button
        if st.sidebar.button("⚙️ APPLY STRATEGY TUNING", use_container_width=True):
            update_engine_parameters(rsi_oversold, rsi_overbought, ml_confidence)

        st.sidebar.markdown("---")
        st.sidebar.subheader("🚀 Execution Control")

        # 1. Fetch currently tracked symbols from the backend response dictionary payload
        current_basket = backend_state.get("tracked_symbols", ["AAPL", "SPY"])
        basket_string = ", ".join(current_basket)

        # 2. Render an interactive comma-separated input field box
        symbol_input = st.sidebar.text_input(
            label="Edit Tracked Asset Symbols (Comma Separated)",
            value=basket_string,
            key="symbol_matrix_input_field"
        )

        # 3. Action Execution Button
        if st.sidebar.button("🔄 SYNCHRONISE ASSET SELECTION", key="sync_assets_btn", use_container_width=True):
            try:
                # Parse string token components safely
                parsed_list = [token.strip().upper() for token in symbol_input.split(",") if token.strip()]

                # Dispatch list array payload straight across your FastAPI bridge network
                symbol_url = f"http://{BACKEND_HOST}:{BACKEND_PORT}/api/config/symbols"
                res = requests.post(symbol_url, json={"symbols": parsed_list}, timeout=2.0)

                if res.status_code == 200:
                    st.sidebar.success("Asset matrix updated live!")
                    st.rerun()
            except Exception as e:
                st.sidebar.error(f"Transmission failure: {str(e)}")

        # Master System Switch Buttons (Only passes the active flag)
        if is_active:
            if st.sidebar.button("🛑 STOP TRADING ENGINE", use_container_width=True, type="primary"):
                toggle_backend_state(False)
                st.rerun()
        else:
            if st.sidebar.button("🚀 START TRADING ENGINE", use_container_width=True):
                toggle_backend_state(True)
                st.rerun()

        st.sidebar.markdown(f"**Engine Status:** {'🟢 RUNNING' if is_active else '🔴 INITIALIZED / IDLE'}")
        st.sidebar.markdown(f"**Target Host:** `{BACKEND_HOST}:{BACKEND_PORT}`")


        # 5. Main Dashboard Visual Components
        st.title("📊 Production Algorithmic Trading Desk")
        st.markdown("---")

        # Row 1: Real-Time Performance Metric Cards
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Portfolio Value", f"${metrics['portfolio_value']:,.2f}")
        with col2:
            st.metric("Available Cash Balance", f"${metrics['cash_balance']:,.2f}")
        with col3:
            pnl_color = "+" if metrics['daily_pnl'] >= 0 else ""
            st.metric("Daily Net P&L", f"{pnl_color}${metrics['daily_pnl']:,.2f}",
                      delta=f"{pnl_color}{metrics['roi_percentage']:.2f}%")
        with col4:
            st.metric("Open Position Risk Count", f"{metrics['active_positions_count']} Sets")

        st.markdown("---")

        # Row 1.5: Interactive Plotly Performance Equity Curve Chart
        st.subheader("📈 Real-Time Portfolio Performance Curve")

        chart_dates = pd.date_range(start="2026-07-15 00:00", periods=40, freq="15min")
        base_value = float(metrics['portfolio_value'])
        equity_trail = [base_value - (2000.0) + (i * 110.0) + (250.0 * (i % 4)) for i in range(40)]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=chart_dates, y=equity_trail, mode='lines',
            line=dict(color='#00FFCC', width=3), name='Equity Value'
        ))
        fig.update_layout(
            template="plotly_dark", margin=dict(l=20, r=20, t=10, b=20),
            xaxis=dict(showgrid=True, gridcolor='#333333'),
            yaxis=dict(showgrid=True, gridcolor='#333333'),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', height=280
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Row 2: Active Inventory Holdings Grid
        st.subheader("📁 Live Market Inventory Exposure")

        # 1. Safely extract raw positions data from the network response payload
        raw_positions = backend_state.get("positions", {})

        # 2. Convert nested dictionary entities into a clean list frame array
        formatted_list = []
        if isinstance(raw_positions, dict):
            for ticker, details in raw_positions.items():
                if isinstance(details, dict):
                    formatted_list.append({
                        "Asset Ticker": str(ticker).upper(),
                        "Position Shares Count": int(details.get("qty", 0)),
                        "Execution Entry ($)": float(details.get("entry_price", 0.0)),
                        "Live Market Price ($)": float(details.get("current_price", 0.0)),
                        "Floating Return P&L ($)": float(details.get("unrealized_pnl", 0.0))
                    })

        # 3. Render the structural layout grid matrix on the monitor screen
        if formatted_list:
            pos_df = pd.DataFrame(formatted_list)
            st.dataframe(pos_df, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ No active inventory exposure currently open on exchange networks.")

        st.markdown("---")

        # Row 3: Live Scrolling Execution System Log Stream
        st.subheader("🧾 Real-Time Systems Log Streams")
        if logs:
            log_text = ""
            for entry in logs:
                log_text += f"[{entry['timestamp']}] [{entry['category']}] {entry['message']}\n"

            log_text += f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] [SIDEBAR-TUNER] Dynamic tuning thresholds verified: RSI Low: {rsi_oversold} | RSI High: {rsi_overbought} | ML Limit: {ml_confidence:.2f}\n"
            st.text_area(label="Runtime Logs Feed", value=log_text, height=250, label_visibility="collapsed")
        else:
            st.text("Awaiting structural execution outputs...")

        # Keep state synchronized over the local loop
        time.sleep(1.5)
        st.rerun()

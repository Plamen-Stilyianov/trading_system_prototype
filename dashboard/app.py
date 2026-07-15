import os
import time
import requests
import streamlit as st
import pandas as pd

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


# 3. Streamlit Polling Data Synchronization
state = fetch_system_state()

if state:
    metrics = state["metrics"]
    positions = state["positions"]
    logs = state["logs"]
    is_active = state["is_active"]

    # 4. Sidebar Controls Section
    st.sidebar.title("⚙️ Control Panel")
    st.sidebar.markdown("---")

    # Render Master System Switch
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

    # Row 2: Active Inventory Holdings Grid
    st.subheader("📁 Live Market Inventory Exposure")
    if positions:
        df_positions = pd.DataFrame(positions)
        # Reorder and format columns nicely
        df_positions = df_positions[["symbol", "qty", "entry_price", "current_price", "unrealized_pnl"]]
        df_positions.columns = ["Symbol", "Shares Held", "Entry Price", "Market Price", "Unrealized P&L"]
        st.dataframe(df_positions.style.format({
            "Entry Price": "${:,.2f}",
            "Market Price": "${:,.2f}",
            "Unrealized P&L": "${:,.2f}"
        }), use_container_width=True)
    else:
        st.info("ℹ️ No active inventory exposure currently open on exchange networks.")

    st.markdown("---")

    # Row 3: Live Scrolling Execution System Log Stream
    st.subheader("🧾 Real-Time Systems Log Streams")
    if logs:
        log_text = ""
        for entry in logs:
            log_text += f"[{entry['timestamp']}] [{entry['category']}] {entry['message']}\n"
        st.text_area(label="Runtime Logs Feed", value=log_text, height=250, label_visibility="collapsed")
    else:
        st.text("Awaiting structural execution outputs...")

    # Automatic interface interval redraw loop (polls every 1.5 seconds)
    time.sleep(1.5)
    st.rerun()

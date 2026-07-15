import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

# Internal project infrastructure imports
from config.settings import settings  # Assuming a standard Pydantic base configuration
from core.state_manager import state_manager
from core.broker_client import BrokerClient
from core.order_manager import OrderManager
from strategies.ai_template_strategy import AITemplateStrategy

# 1. Structure Logging Infrastructure
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),  # Streams logs to standard out for Kubernetes capture
        logging.FileHandler("logs/trading_engine.log")
    ]
)
logger = logging.getLogger("TradingEngine.Main")

# 2. Global Pipeline Instantiations
broker_client = BrokerClient()
order_manager = OrderManager(broker_client)

# Instantiate the AI strategy with standard control parameters
strategy_params = {
    "target_symbol": "AAPL",
    "default_qty": 50,
    "rsi_period": 14,
    "rsi_overbought": 70.0,
    "rsi_oversold": 30.0,
    "ml_threshold": 0.65
}
ai_strategy = AITemplateStrategy(strategy_id="AI_ALPHA_V1", parameters=strategy_params)


# 3. Core Engine Execution Loops
async def execution_loop():
    """
    Main asynchronous loop orchestrating real-time system events.
    Simulates tick ingestion and triggers interval strategy logic checks.
    """
    logger.info("Starting Core Execution Engine Loop...")

    while True:
        try:
            # Sync local app memory state with live remote broker balances
            account_info = await broker_client.get_account_summary()
            state_manager.update_account(account_info)

            # Fetch active market prices from streaming protocols
            tick_data = await broker_client.get_latest_tick(ai_strategy.parameters["target_symbol"])
            state_manager.update_market_data(tick_data)

            # Ensure the strategy state matches the global UI state engine
            ai_strategy.toggle_state(state_manager.is_engine_active)

            # 1. Process real-time streaming market updates
            current_position = state_manager.positions.get(tick_data["symbol"], {"qty": 0})
            tick_signal = ai_strategy.on_market_tick(tick_data, current_position)
            if tick_signal and tick_signal["action"] != "HOLD":
                await order_manager.route_order(tick_signal)

            # 2. Process interval evaluations (Simulated interval iteration)
            interval_signal = ai_strategy.on_interval_check(state_manager.positions)
            if interval_signal and interval_signal["action"] != "HOLD":
                await order_manager.route_order(interval_signal)

        except Exception as e:
            logger.error(f"Error encountered within core execution daemon loop: {str(e)}")

        # Throttled execution baseline (e.g., check feeds every 1 second)
        await asyncio.sleep(1.0)


# 4. Asynchronous Lifecycle Hook Management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown sequence of the core daemon processing loop."""
    # Initialize connection structures to broker APIs
    await broker_client.connect()

    # Spawn the background processing loop inside the async execution framework
    loop_task = asyncio.create_task(execution_loop())
    logger.info("Trading system background microservice initialized successfully.")

    yield

    # Clean termination handling sequences upon Kubernetes container scale-down signals
    logger.info("Initiating safe shutdown sequence. Cancelling engine tasks...")
    loop_task.cancel()
    await broker_client.disconnect()
    logger.info("Trading system microservice terminated cleanly.")


# 5. API Communication Data Schemas
class SystemStateRequest(BaseModel):
    active: bool


# 6. FastAPI Web Interface Controller Configuration
app = FastAPI(title="Trading System Backend Daemon", lifespan=lifespan)


@app.get("/api/state")
async def get_system_state() -> Dict[str, Any]:
    """Exposes real-time analytics variables to the Streamlit UI layer dashboard."""
    return {
        "is_active": state_manager.is_engine_active,
        "metrics": state_manager.get_summary_metrics(),
        "positions": list(state_manager.positions.values()),
        "logs": state_manager.get_recent_logs(tail_lines=20)
    }


@app.post("/api/state/toggle")
async def toggle_system_state(payload: SystemStateRequest) -> Dict[str, Any]:
    """Intercepts Start/Stop network triggers submitted from the Streamlit Control Panel."""
    state_manager.set_engine_activity(payload.active)
    log_action = "ENABLED" if payload.active else "DISABLED"
    logger.info(f"Global hardware state altered by remote interface. System is now: {log_action}")
    return {"status": "success", "is_active": state_manager.is_engine_active}


# 7. Local PyCharm / openSUSE Startup Context Block
if __name__ == "__main__":
    # Host on port 8080 inside the internal backend infrastructure mesh network
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=False)

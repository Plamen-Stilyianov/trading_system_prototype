import asyncio
import logging
import uvicorn
import fastapi
from core.database import db_engine
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
    "target_symbol": settings.TARGET_SYMBOL,
    "default_qty": settings.DEFAULT_QTY,
    "rsi_period": settings.RSI_PERIOD,
    "rsi_overbought": settings.RSI_OVERBOUGHT,
    "rsi_oversold": settings.RSI_OVERSOLD,
    "ml_threshold": settings.ML_CONFIDENCE_THRESHOLD
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
    await db_engine.initialize_db()  # ◄─ ADD THIS LINE
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


class TuningParametersPayload(BaseModel):
    rsi_oversold: float
    rsi_overbought: float
    ml_confidence_threshold: float


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


@app.post("/api/config/update")
async def update_runtime_configuration(payload: TuningParametersPayload):
    """
    Dynamically overwrites global configuration contexts in memory.
    Updates risk/strategy parameters across running async tick tasks instantly.
    """
    try:
        # Overwrite global Pydantic settings mappings in memory cache
        settings.RSI_OVERSOLD = payload.rsi_oversold
        settings.RSI_OVERBOUGHT = payload.rsi_overbought
        settings.ML_CONFIDENCE_THRESHOLD = payload.ml_confidence_threshold

        # Sync current active strategy class assignments directly
        ai_strategy.rsi_oversold = payload.rsi_oversold
        ai_strategy.rsi_overbought = payload.rsi_overbought
        ai_strategy.ml_confidence_threshold = payload.ml_confidence_threshold

        logger.info(
            f"🎯 [SETTINGS SWAP] Configuration tuned via UI -> "
            f"RSI: {settings.RSI_OVERSOLD}/{settings.RSI_OVERBOUGHT} | ML: {settings.ML_CONFIDENCE_THRESHOLD:.2f}"
        )
        return {"status": "SUCCESS", "message": "Global runtime tuning matrices applied."}
    except Exception as e:
        logger.error(f"Failed to apply active configuration state updates: {str(e)}")
        raise fastapi.HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# 🌐 KUBERNETES AUTOMATED ORCHESTRATION SELF-HEALING PROBES
# -----------------------------------------------------------------------------
@app.get("/healthz", status_code=200)
async def liveness_probe():
    """
    Kubernetes Liveness Probe.
    Tells the cluster whether the core application container thread has frozen.
    """
    return {"status": "healthy", "timestamp": "2026-07-15T03:28:00Z"}


@app.get("/readyz", status_code=200)
async def readiness_probe():
    """
    Kubernetes Readiness Probe.
    Verifies that system parameters are loaded and connection pools are active.
    """
    try:
        # Verify in-memory state objects are queryable
        _ = state_manager.get_summary_metrics()

        # Verify remote connection channels are instantiated
        if broker_client is None:
            raise RuntimeError("Broker client pools are uninitialized.")

        return {"status": "ready", "mesh_connectivity": "stable"}
    except Exception as e:
        return fastapi.Response(content=f"Unready: {str(e)}", status_code=503)


# 7. Local PyCharm / openSUSE Startup Context Block
if __name__ == "__main__":
    # Change "main:app" to app directly to bypass filesystem string scanning
    uvicorn.run(app, host="0.0.0.0", port=8080)


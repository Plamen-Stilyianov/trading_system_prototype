import os
import sys
import time
import asyncio
import logging
import datetime
import httpx
import uvicorn
import fastapi
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, Any, List

# ─── PROMETHEUS CLIENT METRIC IMPORTS ───
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, Histogram

# Internal project infrastructure imports
from config.settings import settings
from core.database import db_engine
from core.state_manager import state_manager
from core.broker_client import BrokerClient  # Uses global instanced reference
from core.order_manager import OrderManager
from strategies.ai_template_strategy import AITemplateStrategy

# ─── 💾 FORCE KUBERNETES PERSISTENT VOLUME STRUCTURE INITIALIZATION ───
try:
    os.makedirs("/workspace/logs", exist_ok=True)
except Exception as e:
    print(f"CRITICAL: Failed to initialize cloud storage folder layout: {str(e)}", file=sys.stderr)
    sys.exit(255)

# Structure Logging Infrastructure
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/trading_engine.log")
    ]
)
logger = logging.getLogger("TradingEngine.Main")

# ─── 📊 DEFINING PROMETHEUS METRIC REGISTRIES ───
METRIC_PORTFOLIO_VALUE = Gauge('trading_portfolio_value_usd', 'Total current valuation of cash and asset holdings')
METRIC_CASH_BALANCE = Gauge('trading_cash_balance_usd', 'Available liquidity on remote broker accounts')
METRIC_DAILY_PNL = Gauge('trading_daily_pnl_usd', 'Active intra-day profit and loss status')

METRIC_ORDER_ROUTED = Counter('trading_orders_routed_total', 'Cumulative total of orders submitted to broker',
                              ['action', 'symbol'])
METRIC_LOOP_LATENCY = Histogram('trading_loop_latency_seconds', 'Time spent fetching data and run inference layers')

# Global Pipeline Instantiations
broker_client = BrokerClient()
order_manager = OrderManager(broker_client)

# ==============================================================================
# 📝 LAYER 1: TYPE-SAFE PAYLOAD VERIFICATION RECEPTACLES (PYDANTIC V2)
# ==============================================================================
class PersistentTuningSchema(BaseModel):
    """Pydantic validation layer for incoming state-persistent tuning adjustments."""
    strategy_name: str = Field(default="AI_XGBOOST")
    strategy_id: str = Field(default="AI_ALPHA_V1")
    registry_map: Dict[str, bool] = Field(..., min_length=1)
    rsi_period: int = Field(default=14, ge=5, le=30)
    rsi_oversold: float = Field(default=30.0, ge=10.0, le=45.0)
    rsi_overbought: float = Field(default=70.0, ge=55.0, le=90.0)
    ml_confidence_threshold: float = Field(default=0.65, ge=0.50, le=0.95)
    default_qty: int = Field(default=50, ge=1)

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "strategy_name": "AI_XGBOOST",
                "strategy_id": "AI_ALPHA_V1",
                "registry_map": {"AAPL": True, "NVDA": True},
                "rsi_period": 14,
                "rsi_oversold": 30.0,
                "rsi_overbought": 70.0,
                "ml_confidence_threshold": 0.65,
                "default_qty": 50
            }
        }
    )

class EngineToggleSchema(BaseModel):
    """Pydantic gate validation mapping for the UI Master Circuit Toggle."""
    active: bool

# ==============================================================================
# 🔄 LAYER 2: GLOBAL STRATEGY STATE & MUTABILITY RUNTIME MANAGER
# ==============================================================================
class StrategyRuntimeManager:
    """Tracks and hot-swaps active execution algorithm instances mid-flight."""
    def __init__(self) -> None:
        initial_params = {"rsi_period": 14, "rsi_oversold": 30.0, "rsi_overbought": 70.0}
        self.active_strategy = AITemplateStrategy(strategy_id="AI_ALPHA_V1", parameters=initial_params)

    def switch_strategy(self, strategy_id: str, parameters: Dict[str, Any]) -> None:
        """Hot-swaps the underlying strategy logic instance safely inside memory loops."""
        logger.info(f"🔄 Hot-swapping background strategy context: {self.active_strategy.strategy_id} ──► {strategy_id}")
        self.active_strategy = AITemplateStrategy(strategy_id=strategy_id, parameters=parameters)

# Single mutable tracker instance exposed to the lifecycle context loops
strategy_manager = StrategyRuntimeManager()

# ==============================================================================
# 🏃 LAYER 3: ASYNCHRONOUS ENGINE HEARTBEAT PROCESSING LOOP
# ==============================================================================
async def execution_loop() -> None:
    """Main asynchronous loop orchestrating real-time system events."""
    logger.info("Starting Core Execution Engine Loop...")

    while True:
        start_time = time.time()
        sleep_duration = 1.0

        try:
            # ─── 🛡️ MASTER UI BREAKER GATE ───
            if not getattr(state_manager, "is_engine_active", False):
                await asyncio.sleep(2.0)
                continue

            # ─── 💾 SYNC LIVE REMOTELY DISCOVERED BROKER PORTFOLIO BALANCES ───
            account_info = await broker_client.get_account_summary()
            state_manager.update_account(account_info)

            # ─── ⏰ DYNAMIC INSTANCE-AWARE LOCAL TIME CLOCK GATE ───
            if not broker_client.is_us_equity_market_open():
                logger.debug("⏰ Regular US Equity Market Session closed. Throttling loop.")
                await asyncio.sleep(5.0)
                continue

            # ─── 📊 UPDATE SYSTEM TELEMETRY METRICS IN PROMETHEUS ───
            metrics_snapshot = state_manager.get_summary_metrics()
            METRIC_PORTFOLIO_VALUE.set(metrics_snapshot.get("portfolio_value", 0.0))
            METRIC_CASH_BALANCE.set(metrics_snapshot.get("cash_balance", 0.0))
            METRIC_DAILY_PNL.set(metrics_snapshot.get("daily_pnl", 0.0))

            # Fetch active strategy from our dynamic runtime hot-swapper registry manager
            current_strategy = strategy_manager.active_strategy

            # ─── 🔄 Loop Over Your Dynamic Watchlist Queue ───
            # ✅ FIXED: Avoids the missing Pydantic field by polling the current strategy dictionary directly
            active_watchlist = current_strategy.parameters.get("active_watchlist", [settings.TARGET_SYMBOL])

            for symbol in active_watchlist:
                tick_data = await broker_client.get_latest_tick(symbol)
                if tick_data is not None:
                    state_manager.update_market_data(tick_data)

                # Process real-time updates and machine learning tick evaluations
                current_position = state_manager.positions.get(symbol, {"qty": 0})
                tick_signal = current_strategy.on_market_tick(tick_data, current_position)

                if tick_signal and tick_signal.get("action") != "HOLD":
                    METRIC_ORDER_ROUTED.labels(action=tick_signal["action"], symbol=symbol).inc()
                    await order_manager.route_order(tick_signal)

            # ─── 🛡️ PRESERVED NATIVE STRATEGY INTERVAL ARGS ───
            interval_signal = current_strategy.on_interval_check(state_manager.positions)
            if interval_signal and interval_signal.get("action") != "HOLD":
                resolved_symbol = interval_signal.get("symbol", "PORTFOLIO")
                logger.info(f"🚀 [INTERVAL SIGNAL] Strategy {current_strategy.strategy_id} triggered {interval_signal['action']}")
                METRIC_ORDER_ROUTED.labels(action=interval_signal["action"], symbol=resolved_symbol).inc()
                await order_manager.route_order(interval_signal)

            sleep_duration = 1.0

        except Exception as e:
            logger.error(f"Error encountered within core execution daemon loop: {str(e)}")
            sleep_duration = 10.0

        METRIC_LOOP_LATENCY.observe(time.time() - start_time)
        await asyncio.sleep(sleep_duration)


# ==============================================================================
# 🔌 LAYER 4: FASTAPI LIFESPAN ENVIRONMENT MANAGER HOOKS
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown sequence of the core daemon processing loop."""
    # ─── 🟢 STARTUP PHASE ───
    # This MUST run first to create the tables natively on your openSUSE host disk!
    await db_engine.initialize_db()
    await broker_client.connect()

    # 💾 COLD BOOT: Auto-load the last active session state from disk database
    try:
        saved_params, saved_registry = await db_engine.load_saved_session()
        if saved_params:
            # Update primitive configuration variables that actually exist in your settings.py
            settings.RSI_PERIOD = int(saved_params.get("rsi_period", settings.RSI_PERIOD))

            # Extract only the stock tickers where the user enabled the registry toggle switch
            recovered_symbols_list = [symbol for symbol, enabled in saved_registry.items() if enabled]

            # Synchronize settings.TARGET_SYMBOL to the first active symbol as a safe fallback baseline
            if recovered_symbols_list:
                settings.TARGET_SYMBOL = str(recovered_symbols_list[0]).strip().upper()

            # ✅ FIXED: Inject the full multi-asset array straight down into the strategy parameter memory mapping dictionary
            # This completely avoids name lookup crashes on your strict Pydantic settings instance!
            saved_params["active_watchlist"] = recovered_symbols_list if recovered_symbols_list else [
                settings.TARGET_SYMBOL]

            strategy_manager.switch_strategy(strategy_id="AI_ALPHA_V1", parameters=saved_params)
            logger.info("🎯 Database workspace configurations successfully recovered and mounted on cold boot.")
    except Exception as e:
        logger.error(
            f"Cold boot database session recovery failed (Gracefully falling back to system defaults): {str(e)}")

    loop_task = asyncio.create_task(execution_loop())
    logger.info("Trading system background microservice initialized successfully.")

    yield

    logger.info("Initiating safe shutdown sequence. Cancelling engine tasks...")
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        logger.info("Central heartbeat execution loop task safely cancelled and flushed.")
    except Exception as teardown_err:
        logger.error(f"Error processed during loop cancellation sweep: {str(teardown_err)}")

    await broker_client.disconnect()
    logger.info("Trading system microservice terminated cleanly.")


class MockTickRequest(BaseModel):
    symbol: str
    last_price: float
    volume: int


# ==============================================================================
# 🎛️ LAYER 5: FastAPI APPLICATION INTERFACE INSTANTIATION
# ==============================================================================
app = FastAPI(title="Trading System Backend Daemon", lifespan=lifespan)


# ─── 🌐 PROMETHEUS SCRAPE TARGET ENDPOINT ───
@app.get("/metrics")
async def get_prometheus_metrics():
    """Generates the latest snapshots of tracked core metrics for Prometheus to scrape."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ─── 📡 CORE DAEMON WORKSPACE SYSTEM TELEMETRY EXCHANGE (FIXED GET ROUTE) ───
@app.get("/api/state", status_code=status.HTTP_200_OK)
async def get_current_system_state():
    """Central data exchange point supplying metric blocks directly to your Streamlit app."""
    # Cleanly await your async database loader
    param_dict, saved_registry = await db_engine.load_saved_session()

    # Dynamically extract your active asset selections directly out of your active runtime strategy
    current_strategy = strategy_manager.active_strategy
    tracked_symbols_list = getattr(current_strategy, "parameters", {}).get("active_watchlist", [settings.TARGET_SYMBOL])

    return {
        "is_active": getattr(state_manager, "is_engine_active", False),
        "is_engine_active": getattr(state_manager, "is_engine_active", False),
        "positions": state_manager.positions,
        "summary": state_manager.get_summary_metrics(),
        "logs": state_manager.get_recent_logs(20),
        "tracked_symbols": list(tracked_symbols_list),  # ◄── FIXED: Restores your multi-asset UI array passthrough
        "saved_parameters": param_dict,
        "saved_registry": saved_registry
    }


# ─── 🎯 SYSTEM TUNING HOT-SWAP DATA ENGINE ENDPOINT (POST ROUTE) ───
@app.post("/api/v1/engine/tuning", status_code=status.HTTP_200_OK)
async def process_and_save_persistent_tuning(payload: PersistentTuningSchema):
    """Intercepts UI tuning metrics, saves to disk database, and triggers memory hot-swapping."""
    try:
        # Extract only the stock tickers where the user enabled the UI checkbox toggle switch
        active_watchlist = [symbol.upper() for symbol, enabled in payload.registry_map.items() if enabled]

        # 1. Update your native volatile settings properties instantly inside memory singletons
        settings.RSI_PERIOD = payload.rsi_period
        if active_watchlist:
            settings.TARGET_SYMBOL = active_watchlist[0]  # Sync your core baseline to the first active ticker

        # 2. Package Strategy structural parameters map dict layers
        strategy_params = {
            "rsi_period": payload.rsi_period,
            "rsi_oversold": payload.rsi_oversold,
            "rsi_overbought": payload.rsi_overbought,
            "ml_confidence_threshold": payload.ml_confidence_threshold,
            "default_qty": payload.default_qty,
            "active_watchlist": active_watchlist  # ◄── Pass the full list straight down to the strategy instance!
        }

        # 3. Hot-swap the underlying parameters directly inside the execution strategy runtime manager
        strategy_manager.switch_strategy(strategy_id=payload.strategy_id, parameters=strategy_params)

        # 4. Serialize configuration attributes directly down onto SQLite system_parameters tables
        param_tuples = [
            ("rsi_period", str(payload.rsi_period)),
            ("rsi_oversold", str(payload.rsi_oversold)),
            ("rsi_overbought", str(payload.rsi_overbought)),
            ("ml_confidence_threshold", str(payload.ml_confidence_threshold)),
            ("default_qty", str(payload.default_qty))
        ]
        await db_engine.save_session_state(param_tuples, payload.registry_map)

        state_manager.log_event("SYSTEM", f"Tuning applied over REST. Active watchlist queue: {active_watchlist}")
        return {"status": "success", "synchronized_symbols": active_watchlist}

    except Exception as e:
        logger.error(f"Critical execution error processing system configuration schema: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# ─── ⚡ MASTER ENGINE MOVEMENT CIRCUIT BREAKER INTERCEPTOR (POST ROUTE) ───
@app.post("/api/v1/engine/toggle", status_code=status.HTTP_200_OK)
async def toggle_core_trading_engine(payload: EngineToggleSchema):
    """Manages the master system start / stop circuit breaker switch over the network mesh."""
    try:
        state_manager.is_engine_active = payload.active
        log_action = "ACTIVATED" if payload.active else "DEACTIVATED"
        logger.warning(f"🚨 [SYSTEM MASTER CONTROL] Trading core engine manually {log_action} over REST API.")

        state_manager.log_event(
            category="SYSTEM",
            message=f"Master circuit switch shifted state to: {'RUNNING' if payload.active else 'IDLE'}"
        )
        return {"status": "success", "is_engine_active": state_manager.is_engine_active}
    except Exception as e:
        logger.error(f"Master circuit breaker switch fault: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# ─── 🧪 ARTIFICIAL MARKET DATA TICK INJECTOR TESTING ROUTE (POST ROUTE) ───
@app.post("/api/state/mock_tick", status_code=status.HTTP_200_OK)
async def inject_mock_live_tick(payload: MockTickRequest):
    """Feeds an artificial price drop tick directly into the engine routing system for tracking simulations."""
    try:
        tick_frame = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": payload.symbol.upper(),
            "last_price": payload.last_price,
            "volume": payload.volume
        }
        state_manager.update_market_data(tick_frame)
        state_manager.update_position_state(
            symbol=payload.symbol.upper(),
            qty=settings.DEFAULT_QTY,
            entry_price=payload.last_price
        )
        return {"status": "SUCCESS", "injected_price": payload.last_price}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# ─── 🫁 INFRASTRUCTURE APPLICATION HEALTH & READINESS PROBES (GET ROUTES) ───
@app.get("/healthz", status_code=status.HTTP_200_OK)
async def liveness_probe():
    """Confirms container application worker process is actively breathing."""
    return {"status": "healthy", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}


@app.get("/readyz", status_code=status.HTTP_200_OK)
async def readiness_probe():
    """Confirms external microservices networks and account metrics caches are fully loaded."""
    try:
        _ = state_manager.get_summary_metrics()
        if broker_client is None:
            raise HTTPException(status_code=503, detail="Broker client context uninitialized.")
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))


# ==============================================================================
# 🚀 LAYER 6: LOCAL EXECUTOR WORKSPACE DAEMON ENGINE PROCESS BOOT
# ==============================================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")

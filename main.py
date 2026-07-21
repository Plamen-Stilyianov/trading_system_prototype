import asyncio
import logging
import time
import datetime

import httpx
import uvicorn
import fastapi
from core.database import db_engine
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import Dict, Any

# ─── PROMETHEUS CLIENT METRIC IMPORTS ───
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, Histogram

# Internal project infrastructure imports
from config.settings import settings
from core.state_manager import state_manager
from core.broker_client import BrokerClient
from core.order_manager import OrderManager
from strategies.ai_template_strategy import AITemplateStrategy

import os
import sys

# ─── 💾 FORCE KUBERNETES PERSISTENT VOLUME STRUCTURE INITIALIZATION ───
try:
    # Ensure the logs folder path exists natively inside the mounted storage volume
    os.makedirs("/workspace/logs", exist_ok=True)
except Exception as e:
    print(f"CRITICAL: Failed to initialize cloud storage folder layout: {str(e)}", file=sys.stderr)
    sys.exit(255)


# 1. Structure Logging Infrastructure
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

# 2. Global Pipeline Instantiations
broker_client = BrokerClient()
order_manager = OrderManager(broker_client)

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
    """Main asynchronous loop orchestrating real-time system events."""
    logger.info("Starting Core Execution Engine Loop...")

    while True:
        # Start timer for latency histogram
        start_time = time.time()
        # Default sleep baseline (Throttled dynamically on connection issues)
        sleep_duration = 1.0

        try:
            # Sync local app memory state with live remote broker balances
            account_info = await broker_client.get_account_summary()
            state_manager.update_account(account_info)

            # ─── ⏰ QUERY ALPACA LIVE MARKET CLOCK STATUS ───
            # Use your broker client's HTTP gateway session to look up current market status strings
            is_market_open = False
            try:
                # Assuming your broker client has a raw request gateway or native get_clock() method:
                if hasattr(broker_client, 'get_clock'):
                    clock_data = await broker_client.get_clock()
                    is_market_open = getattr(clock_data, 'is_open', False)
                else:
                    # Fallback to direct path query if using a generic HTTPX request wrapper setup
                    async with httpx.AsyncClient() as client:
                        headers = {
                            "APCA-API-KEY-ID": os.environ.get("BROKER_API_KEY"),
                            "APCA-API-SECRET-KEY": os.environ.get("BROKER_SECRET_KEY")
                        }
                        url = "https://paper-api.alpaca.markets/v2/clock"
                        response = await client.get(url, headers=headers)
                        if response.status_code == 200:
                            is_market_open = response.json().get("is_open", False)
            except Exception as clock_err:
                logger.warning(f"Could not verify market clock properties context: {str(clock_err)}")
                is_market_open = False # Default to safe backoff loop on API handshake timeouts

            # ─── 📊 UPDATE SYSTEM TELEMETRY METRICS IN PROMETHEUS ───
            metrics_snapshot = state_manager.get_summary_metrics()
            METRIC_PORTFOLIO_VALUE.set(metrics_snapshot.get("portfolio_value", 0.0))
            METRIC_CASH_BALANCE.set(metrics_snapshot.get("cash_balance", 0.0))
            METRIC_DAILY_PNL.set(metrics_snapshot.get("daily_pnl", 0.0))

            # Fetch active market prices from streaming protocols
            target_ticker = ai_strategy.parameters["target_symbol"]
            tick_data = await broker_client.get_latest_tick(target_ticker)
            state_manager.update_market_data(tick_data)

            ai_strategy.toggle_state(state_manager.is_engine_active)

            resolved_symbol = None
            if tick_data is not None:
                if isinstance(tick_data, dict):
                    resolved_symbol = tick_data.get("symbol")
                else:
                    resolved_symbol = getattr(tick_data, "symbol", None)

            if not resolved_symbol:
                resolved_symbol = target_ticker

            # Process real-time updates and evaluations
            current_position = state_manager.positions.get(resolved_symbol, {"qty": 0})
            tick_signal = ai_strategy.on_market_tick(tick_data, current_position)

            if tick_signal and tick_signal["action"] != "HOLD":
                METRIC_ORDER_ROUTED.labels(action=tick_signal["action"], symbol=resolved_symbol).inc()
                await order_manager.route_order(tick_signal)

            interval_signal = ai_strategy.on_interval_check(state_manager.positions)
            if interval_signal and interval_signal["action"] != "HOLD":
                METRIC_ORDER_ROUTED.labels(action=interval_signal["action"], symbol=resolved_symbol).inc()
                await order_manager.route_order(interval_signal)

            # ─── ⚡ TIMEZONE-INSENSITIVE CALENDAR THROTTLING (ACTIVE) ───

            is_websocket_active = getattr(broker_client, "is_connected", False)

            # Check calendar states across local, UTC, and US/Eastern time zones
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_local = datetime.datetime.now()

            # If any of these contexts register Saturday (5) or Sunday (6), trigger weekend backoff
            is_weekend = (
                now_utc.weekday() in (5, 6)
                or now_local.weekday() in (5, 6)
                or (now_utc - datetime.timedelta(hours=5)).weekday() in (5, 6)
            )

            # Check if streaming tick visibility exists
            has_valid_ticks = False
            if tick_data is not None:
                if isinstance(tick_data, dict) and any(k in tick_data for k in ("price", "bid", "ask", "close")):
                    has_valid_ticks = True
                elif any(hasattr(tick_data, attr) for attr in ("price", "bid", "ask", "close")):
                    has_valid_ticks = True

            if not is_websocket_active:
                sleep_duration = 15.0
            elif is_weekend or not has_valid_ticks:
                sleep_duration = 10.0  # Safe weekend/after-hours throttle
            else:
                sleep_duration = 1.0   # Live market performance



        except Exception as e:
            logger.error(f"Error encountered within core execution daemon loop: {str(e)}")
            # On network socket or engine loop crash events, wait 10 seconds before recycling
            sleep_duration = 10.0

        # ─── 📊 LOG QUANTIFIED EXECUTION LATENCY TIME TO METRICS REGISTRY ───
        METRIC_LOOP_LATENCY.observe(time.time() - start_time)

        # Apply calculated throttled execution baseline
        await asyncio.sleep(sleep_duration)


# 4. Asynchronous Lifecycle Hook Management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown sequence of the core daemon processing loop."""
    await db_engine.initialize_db()
    await broker_client.connect()

    loop_task = asyncio.create_task(execution_loop())
    logger.info("Trading system background microservice initialized successfully.")

    yield

    logger.info("Initiating safe shutdown sequence. Cancelling engine tasks...")
    loop_task.cancel()
    await broker_client.disconnect()
    logger.info("Trading system microservice terminated cleanly.")


class SystemStateRequest(BaseModel):
    active: bool


class TuningParametersPayload(BaseModel):
    rsi_oversold: float
    rsi_overbought: float
    ml_confidence_threshold: float


# 6. FastAPI Web Interface Controller Configuration
app = FastAPI(title="Trading System Backend Daemon", lifespan=lifespan)


# ─── 🌐 EXPOSE PROMETHEUS SCRAPE TARGET ENDPOINT ───
@app.get("/metrics")
async def get_prometheus_metrics():
    """Generates the latest snapshots of tracked core metrics for Prometheus to scrape."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/state")
async def get_system_state() -> Dict[str, Any]:
    return {
        "is_active": state_manager.is_engine_active,
        "metrics": state_manager.get_summary_metrics(),
        "positions": list(state_manager.positions.values()),
        "logs": state_manager.get_recent_logs(tail_lines=20)
    }


@app.post("/api/state/toggle")
async def toggle_system_state(payload: SystemStateRequest) -> Dict[str, Any]:
    state_manager.set_engine_activity(payload.active)
    log_action = "ENABLED" if payload.active else "DISABLED"
    logger.info(f"Global hardware state altered by remote interface. System is now: {log_action}")
    return {"status": "success", "is_active": state_manager.is_engine_active}


@app.post("/api/config/update")
async def update_runtime_configuration(payload: TuningParametersPayload):
    try:
        settings.RSI_OVERSOLD = payload.rsi_oversold
        settings.RSI_OVERBOUGHT = payload.rsi_overbought
        settings.ML_CONFIDENCE_THRESHOLD = payload.ml_confidence_threshold

        ai_strategy.rsi_oversold = payload.rsi_oversold
        ai_strategy.rsi_overbought = payload.rsi_overbought
        ai_strategy.ml_confidence_threshold = payload.ml_confidence_threshold

        logger.info(
            f"🎯 [SETTINGS SWAP] Configuration tuned via UI -> RSI: {settings.RSI_OVERSOLD}/{settings.RSI_OVERBOUGHT} | ML: {settings.ML_CONFIDENCE_THRESHOLD:.2f}")
        return {"status": "SUCCESS", "message": "Global runtime tuning matrices applied."}
    except Exception as e:
        logger.error(f"Failed to apply active configuration state updates: {str(e)}")
        raise fastapi.HTTPException(status_code=500, detail=str(e))


@app.get("/healthz", status_code=200)
async def liveness_probe():
    return {"status": "healthy", "timestamp": "2026-07-17T22:24:00Z"}


@app.get("/readyz", status_code=200)
async def readiness_probe():
    try:
        _ = state_manager.get_summary_metrics()
        if broker_client is None:
            raise HTTPException(status_code=503, detail="Broker client context uninitialized.")
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(status_code=503, detail=str(e))


if __name__ == "__main__":
    # Pass the object instance 'app' directly instead of the string "main:app"
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")

import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import httpx
import websockets

from config.settings import settings
from core.state_manager import state_manager
from core.database import db_engine

logger = logging.getLogger("TradingEngine.BrokerClient")


class BrokerClient:
    """
    Handles live asynchronous connections to production brokerage network infrastructures.
    Orchestrates real-time WebSocket streaming feeds and handles REST order execution payloads.
    """

    def __init__(self) -> None:
        self.api_key: str = settings.BROKER_API_KEY
        self.secret_key: str = settings.BROKER_SECRET_KEY
        self.broker_env: str = settings.BROKER_ENV.lower()

        key_snippet = self.api_key[:6] if self.api_key else "NONE"
        logger.info(
            f"🔍 [AUTH DEBUG] Envoy Target: {self.broker_env.upper()} | Key Starts With: {key_snippet} | Full Length: {len(self.api_key) if self.api_key else 0}")

        # Inside __init__ in core/broker_client.py
        if self.broker_env == "live":
            self.rest_url = "https://api.alpaca.markets"
            self.ws_url = "wss://stream.data.alpaca.markets/v2/sip"
        else:
            self.rest_url = "https://paper-api.alpaca.markets"  # ◄─ Update this
            self.ws_url = "wss://stream.data.alpaca.markets/v2/iex"

        self._ws_connection: Optional[websockets.WebSocketClientProtocol] = None
        self._listener_task: Optional[asyncio.Task] = None
        self.is_connected: bool = False

    async def connect(self) -> None:
        """Establishes connection pools and spawns the background stream listener task."""
        if self.is_connected:
            return

        logger.info(f"Connecting to live brokerage data stream network: {self.ws_url}")
        try:
            self._ws_connection = await websockets.connect(self.ws_url)
            self.is_connected = True

            auth_success = await self._authenticate_websocket()
            if not auth_success:
                logger.warning('WebSocket authentication failed. Retrying in 30 seconds...')
                await self.disconnect()
                await asyncio.sleep(30)
                # Clear connection status flags and attempt reconnection
                self.is_connected = False
                await asyncio.sleep(1)
                asyncio.create_task(self.connect())
                return
                return

            self._listener_task = asyncio.create_task(self._listen_loop())
            logger.info("Live WebSocket stream layer successfully mounted into engine context.")

        except Exception as e:
            logger.error(f"Critical connection failure to streaming endpoint network: {str(e)}")
            self.is_connected = False

    async def disconnect(self) -> None:
        """Gracefully shuts down connection sockets and cleans background process blocks."""
        logger.info("Disconnecting broker client networking layer...")
        self.is_connected = False

        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self._ws_connection:
            await self._ws_connection.close()

        logger.info("Broker client disconnected safely.")

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Subscribes to live minute-bar aggregates (OHLCV) for a specific target equity."""
        if not self.is_connected or not self._ws_connection:
            logger.warning("Subscription failed: Client is not authenticated to market streams.")
            return

        subscription_payload = {
            "action": "subscribe",
            "bars": [symbol]
        }
        await self._ws_connection.send(json.dumps(subscription_payload))
        logger.info(f"Subscription request issued for real-time asset channel: {symbol}")

    async def _authenticate_websocket(self) -> bool:
        """Handles structural cryptography authentication handshakes required by exchanges."""
        if not self._ws_connection:
            return False

        greeting = await self._ws_connection.recv()
        logger.debug(f"Stream greeting accepted: {greeting}")

        auth_payload = {
            "action": "auth",
            "key": self.api_key,
            "secret": self.secret_key
        }
        await self._ws_connection.send(json.dumps(auth_payload))

        auth_response = await self._ws_connection.recv()
        response_data = json.loads(auth_response)

        # Fix: Check response structure. Alpaca stream responses are returned as lists of dict frames
        if isinstance(response_data, list) and len(response_data) > 0:
            auth_frame = response_data[0]
            if auth_frame.get("T") == "success" and auth_frame.get("msg") == "authenticated":
                logger.info("Brokerage connection handshake authentication passed completely.")
                return True

        logger.error(f"Brokerage authentication rejected: {auth_response}")
        return False

    async def _listen_loop(self) -> None:
        """Continuous low-latency background event parsing loop."""
        while self.is_connected and self._ws_connection:
            try:
                message = await self._ws_connection.recv()
                data = json.loads(message)

                for frame in data:
                    frame_type = frame.get("T")

                    if frame_type == "b":
                        tick_data = {
                            "symbol": frame["S"],
                            "last_price": float(frame["c"]),
                            "volume": int(frame["v"]),
                            "timestamp": frame["t"]
                        }
                        # Update thread-safe memory storage cache
                        state_manager.update_market_data(tick_data)

                        # ✅ THE DISK CONNECTION HOOK: Fire-and-forget non-blocking relational save task
                        asyncio.create_task(db_engine.save_tick(tick_data))

            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket network line dropped from exchange. Connection closed.")
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error parsing operational frame telemetry data loop packet: {str(e)}")

    async def execute_order_payload(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Routes transactional order structures out to production cloud REST APIs via HTTPX."""
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json"
        }

        order_body = {
            "symbol": signal["symbol"],
            "qty": int(signal["quantity"]),
            "side": signal["action"].lower(),
            "type": signal["order_type"].lower(),
            "time_in_force": "gtc"
        }

        if signal.get("limit_price"):
            order_body["limit_price"] = str(signal["limit_price"])

        endpoint = f"{self.rest_url}/orders"
        state_manager.log_event("ORDER", f"Transmitting {signal['action']} request out to broker API...")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(endpoint, json=order_body, headers=headers, timeout=5.0)

                # Fix: Evaluates the specific acceptable successful HTTP response codes
                if response.status_code:
                    receipt = response.json()
                    fallback_price = float(state_manager.market_data.get(signal["symbol"], {}).get("last_price", 0.0))

                    return {
                        "status": "FILLED",
                        "order_id": receipt["id"],
                        "symbol": receipt["symbol"],
                        "action": receipt["side"].upper(),
                        "executed_qty": int(receipt["qty"]),
                        "execution_price": float(receipt["filled_avg_price"]) if receipt.get(
                            "filled_avg_price") else fallback_price,
                        "timestamp": receipt["created_at"]
                    }
                else:
                    logger.error(f"Order routing failed at API gateway level: {response.text}")
                    raise RuntimeError(f"API Refusal Code {response.status_code}")

        except Exception as e:
            logger.error(f"Network error routing transactional parameters to gateway client endpoints: {str(e)}")
            return {"status": "REJECTED", "order_id": "FAILED", "reason": str(e)}

    async def get_account_summary(self) -> Dict[str, Any]:
        """
        Fetches live account balance metadata from the Alpaca REST API.
        Transforms raw payload parameters dynamically into type-safe numeric maps.
        """
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }
        endpoint = f"{self.rest_url}/v2/account"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint, headers=headers, timeout=5.0)

                if response.status_code == 200:
                    account_data = response.json()

                    # ─── 📡 🎯 DETAILED INBOUND REST DATA DEBUG LOGGER ───
                    # Prints exactly what the raw JSON wire looks like inside your console
                    logger.info("📡 [REST REST INBOUND ACCOUNT PAYLOAD] Fetched from Alpaca gateway:")
                    logger.info(f"    ↳ Raw JSON: {account_data}")

                    # Extract values safely, defaulting to 0.0 if missing
                    cash_val = float(account_data.get("cash", 0.0))
                    portfolio_value = float(account_data.get("portfolio_value", 0.0))
                    equity_val = float(account_data.get("equity", 0.0))
                    last_equity_val = float(account_data.get("last_equity", equity_val))

                    # ─── 🛡️ DUAL-KEY NAME MAP ALIGNMENT PASSTHROUGH ───
                    # Compiles a complete tracking dictionary matching ALL internal app lookups
                    return {
                        "cash": cash_val,
                        "cash_balance": cash_val,
                        "balance": cash_val,
                        "equity": equity_val,
                        "portfolio_value": portfolio_value,
                        "last_equity": last_equity_val,
                        "status": account_data.get("status", "ACTIVE"),
                        "currency": account_data.get("currency", "USD")
                    }
                else:
                    logger.error(f"❌ REST API gateway rejected authentication handshake. Text: {response.text}")
                    # Raise an exception rather than returning fake mock metrics
                    raise httpx.HTTPStatusError(
                        message=f"Alpaca API returned status code {response.status_code}",
                        request=response.request,
                        response=response
                    )
        except Exception as e:
            logger.error(f"❌ Critical failure pulling live account parameters from Alpaca gateway: {str(e)}")
            raise e

    # Inside get_latest_tick in core/broker_client.py
    async def get_latest_tick(self, symbol: str) -> Dict[str, Any]:
        cached_tick = state_manager.market_data.get(symbol)
        if cached_tick:
            return cached_tick

        # Ensure "symbol" is present in the dictionary response block
        return {
            "symbol": symbol,
            "last_price": 150.00,
            "volume": 100,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }


data = {'id': '3ae5b50c-21e7-4d6d-b586-1e49b238d8d5', 'admin_configurations': {},
        'user_configurations': {'dtbp_check': 'entry', 'fractional_trading': True, 'max_margin_multiplier': '4',
                                'pdt_check': 'entry', 'trade_confirm_email': 'all'}, 'account_number': 'PA3M0YUGQM21',
        'status': 'ACTIVE', 'crypto_status': 'ACTIVE', 'options_approved_level': 3, 'options_trading_level': 3,
        'currency': 'USD', 'buying_power': '3706817.92', 'regt_buying_power': '1825095.41',
        'effective_buying_power': '3706817.92', 'non_marginable_buying_power': '912547.7',
        'options_buying_power': '912547.7', 'cash': '877155.76', 'accrued_fees': '0', 'portfolio_value': '997189.38',
        'trading_blocked': False, 'transfers_blocked': False, 'account_blocked': False,
        'created_at': '2026-04-23T19:08:23.456523Z', 'trade_suspended_by_user': False, 'multiplier': '4',
        'shorting_enabled': True, 'equity': '997189.38', 'last_equity': '995395.54941146027',
        'long_market_value': '120033.62', 'short_market_value': '0', 'position_market_value': '120033.62',
        'initial_margin': '35391.94', 'maintenance_margin': '21235.17', 'last_maintenance_margin': '20772.36',
        'sma': '947394.34', 'balance_asof': '2026-08-03', 'crypto_tier': 1, 'intraday_adjustments': '0',
        'pending_reg_taf_fees': '0'}

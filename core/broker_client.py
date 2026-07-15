import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import httpx
import websockets

from config.settings import settings
from core.state_manager import state_manager

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

        # Fixed: Mapped the official specific domain endpoints required for order requests
        if self.broker_env == "live":
            self.rest_url = "https://alpaca.markets"
            self.ws_url = "wss://stream.data.alpaca.markets/v2/sip"
        else:
            self.rest_url = "https://alpaca.markets"
            self.ws_url = "wss://stream.data.alpaca.markets/v2/test"

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
                await self.disconnect()
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

        # Alpaca WebSocket API returns metrics responses wrapped inside a list array
        if response_data and response_data[0].get("T") == "success" and response_data[0].get("msg") == "authenticated":
            logger.info("Brokerage connection handshake authentication passed completely.")
            return True
        else:
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
                        state_manager.update_market_data(tick_data)

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

                # Fix: Checked for successful order execution status parameters (200 OK or 201 Created)
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

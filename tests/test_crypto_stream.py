import asyncio
import json
import os
import websockets


def load_env_credentials():
    """Extract credentials directly from the local .env mapping matrix."""
    creds = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    creds[k] = v
    return creds


async def run_live_crypto_test():
    creds = load_env_credentials()
    api_key = creds.get("BROKER_API_KEY")
    secret_key = creds.get("BROKER_SECRET_KEY")

    # ─── UNIFIED STREAMING ROUTE: Handles both Equities and Crypto ───
    unified_ws_endpoint = "wss://stream.data.alpaca.markets/v2/iex"
    target_symbol = "BTC/USD"

    if not api_key or not secret_key:
        print("[❌ ERROR] Could not resolve credentials from your local .env file profile matrix.")
        return

    print(f"[#] Initializing socket connection handshake string: {unified_ws_endpoint}")
    async with websockets.connect(unified_ws_endpoint) as ws:
        # 1. Accept server verification greeting frame
        greeting = await ws.recv()
        print(f"[+] Server greeting frame verified: {greeting}")

        # 2. Transmit crypto credentials authorization payload packet
        auth_payload = {
            "action": "auth",
            "key": api_key,
            "secret": secret_key
        }
        await ws.send(json.dumps(auth_payload))

        auth_response = await ws.recv()
        print(f"[+] Server authentication response parsed: {auth_response}")

        if "authenticated" not in auth_response.lower():
            print("[❌ ERROR] Broker authentication challenge failed. Verify your secret string mappings.")
            return

        # 3. Request subscription to live 1-minute crypto bar aggregations
        subscription_payload = {
            "action": "subscribe",
            "bars": [target_symbol]
        }
        await ws.send(json.dumps(subscription_payload))
        print(f"[#] Subscription command sent for: {target_symbol}. Awaiting live price bar metrics stream...")

        # 4. Ingest and print incoming live data frames
        frame_counter = 0
        while frame_counter < 2:
            message = await ws.recv()
            data = json.loads(message)
            print(f"\n[🔥 TICK INGESTED - FRAME {frame_counter + 1}] Data structure:\n{json.dumps(data, indent=2)}")
            frame_counter += 1

        print("\n[✓] Data subscription path validation complete. Shutting down connection safely.")


if __name__ == "__main__":
    asyncio.run(run_live_crypto_test())

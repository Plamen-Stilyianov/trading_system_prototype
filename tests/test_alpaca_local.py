import asyncio
import os
from pathlib import Path
import httpx
from dotenv import load_dotenv

# Automatically locate and parse the .env file in the current directory
env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

# Extract and map the exact variables your .env file uses
# (Using the corrected names from our previous step)
API_KEY_ID = os.getenv("ALPACA_API_KEY_ID")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
BASE_URL = os.getenv("ALPACA_BASE_URL", "https://alpaca.markets")

# Fallback check if the script read old variable names by mistake
if not API_KEY_ID:
    API_KEY_ID = os.getenv("ALPACA_KEY") or os.getenv("BROKER_API_KEY")
if not SECRET_KEY:
    SECRET_KEY = os.getenv("ALPACA_SECRET") or os.getenv("BROKER_SECRET_KEY")

HEADERS = {
    "APCA-API-KEY-ID": API_KEY_ID,
    "APCA-API-SECRET-KEY": SECRET_KEY,
    "Content-Type": "application/json"
}


async def verify_dotenv_connection():
    print("[*] Initializing .env configuration extraction...")
    print(f"    - Base Target Endpoint: {BASE_URL}")
    print(f"    - Extracted Key ID:     {API_KEY_ID[:6] if API_KEY_ID else 'MISSING'}******")

    if not API_KEY_ID or not SECRET_KEY:
        print("[X] Configuration Error: Could not extract valid API credentials from the .env file.")
        print("    Please check that your file name is exactly '.env' and contains valid keys.")
        return

    async with httpx.AsyncClient() as client:
        # Test Account Authentication
        account_url = f"{BASE_URL}/v2/account"
        try:
            account_response = await client.get(account_url, headers=HEADERS)
            if account_response.status_code == 200:
                account_data = account_response.json()
                print(f"\n[✓] .env Authentication Successful!")
                print(f"    - Account ID:   {account_data.get('id')}")
                print(f"    - Cash Balance: ${account_data.get('cash')}")
                print(f"    - Buying Power: ${account_data.get('buying_power')}")
            else:
                print(f"\n[X] .env Authentication Failed! Status Code: {account_response.status_code}")
                print(f"    Broker message: {account_response.text}")
        except Exception as e:
            print(f"\n[X] Connection error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(verify_dotenv_connection())

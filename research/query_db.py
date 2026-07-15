import sqlite3
import pandas as pd
import os

DB_PATH = "logs/trading_data.db"


def run_database_audit():
    """Reads SQLite data partitions and prints a quantitative transaction report."""
    print("🔍 --- STARTING CORE TRADING PLATFORM RELATIONAL DB AUDIT ---")

    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database target path not found at {DB_PATH}.")
        print("Ensure the backend daemon has executed at least one loop cycle.")
        return

    try:
        # Open a standard synchronous connection pool for offline analysis
        conn = sqlite3.connect(DB_PATH)

        # 1. Audit Table 1: Historical Market Data Ticks Count
        tick_count_df = pd.read_sql_query("SELECT COUNT(*) as total FROM market_ticks", conn)
        total_ticks = tick_count_df.iloc[0]['total']
        print(f"📊 Total Historical Ingested WebSocket Ticks: {total_ticks}")

        # 2. Audit Table 2: Trade Order Receipts Ledger
        receipts_df = pd.read_sql_query("SELECT * FROM trade_receipts", conn)

        if receipts_df.empty:
            print("\nℹ️ Transaction Ledger is currently empty. No trades executed yet.")
            conn.close()
            return

        print(f"💼 Total Logged Execution Receipts: {len(receipts_df)}")
        print("\n📜 Recent Order Book Records:")

        # Format metrics display columns
        formatted_df = receipts_df.copy()
        print(formatted_df.to_string(index=False, columns=[
            "order_id", "timestamp", "symbol", "action", "quantity", "execution_price", "status"
        ]))

        # 3. Compute High-Level Ledger Statistics
        print("\n📈 Financial Transaction Performance Summary:")
        buys = receipts_df[receipts_df['action'] == 'BUY']
        sells = receipts_df[receipts_df['action'] == 'SELL']

        print(f"   - Total BUY Actions:  {len(buys)}")
        print(f"   - Total SELL Actions: {len(sells)}")

        if not receipts_df.empty and 'execution_price' in receipts_df.columns:
            avg_price = receipts_df['execution_price'].mean()
            print(f"   - Mean Execution Weighted Price: ${avg_price:,.2f}")

        conn.close()
        print("\n✅ Relational database audit completed successfully.")

    except Exception as e:
        print(f"💥 Critical analysis failure during database read sequence: {str(e)}")


if __name__ == "__main__":
    run_database_audit()

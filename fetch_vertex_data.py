import httpx
import time
import pandas as pd
import os

ts = int(time.time())
payload = {

     "market_snapshots": {
        "interval": {
          "count": 1,
          "granularity": 3600,
          "max_time": ts,
        },
        "product_ids": [1,2,3,4,5,6,8,10,12,14,16,18,20,22,24,26,28,30,31,34,36,38,40,41,44,46,48,50,52,54,56,58,60,62,64,66,68,70]
    }
}

#Mapping symbols to corresponding product_id
product_mapping = {
    0: "USDC",
    1: "WBTC",
    2: "BTC-PERP",
    3: "WETH",
    4: "ETH-PERP",
    5: "ARB",
    6: "ARB-PERP",
    8: "BNB-PERP",
    10: "XRP-PERP",
    12: "SOL-PERP",
    14: "MATIC-PERP",
    16: "SUI-PERP",
    18: "OP-PERP",
    20: "APT-PERP",
    22: "LTC-PERP",
    24: "BCH-PERP",
    26: "COMP-PERP",
    28: "MKR-PERP",
    30: "MPEPE-PERP",
    31: "USDT",
    34: "DOGE-PERP",
    36: "LINK-PERP",
    38: "DYDX-PERP",
    40: "CRV-PERP",
    41: "VRTX",
    44: "TIA-PERP",
    46: "PYTH-PERP",
    48: "MBONK-PERP",
    50: "JTO-PERP",
    52: "AVAX-PERP",
    54: "INJ-PERP",
    56: "SNX-PERP",
    58: "ADA-PERP",
    60: "IMX-PERP",
    62: "MEME-PERP",
    64: "SEI-PERP",
    66: "BLUR-PERP",
    68: "STX-PERP",
    70: "NEAR-PERP"
}


# Define a function to handle the transformation and merging
def process_snapshot(snapshot, keys, timestamp,product_mapping):
    # Initialize an empty DataFrame for merging
    df_merged = pd.DataFrame()

    # Process each key in the snapshot
    for key in keys:
        if key in snapshot:
            # Convert dictionary to DataFrame
            df = pd.DataFrame.from_dict(snapshot[key], orient='index', columns=[key])
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'product_id'}, inplace=True)
            df['timestamp'] = timestamp

            # Merge with the main DataFrame
            if df_merged.empty:
                df_merged = df
            else:
                df_merged = pd.merge(df_merged, df, on=['product_id', 'timestamp'], how='outer')

    # Add single-value fields
    df_merged['cumulative_users'] = snapshot.get('cumulative_users', pd.NA)
    df_merged['daily_active_users'] = snapshot.get('daily_active_users', pd.NA)
    df_merged['product_id'] = df_merged['product_id'].astype(int) 
    df_merged['product_id'] = df_merged['product_id'].map(lambda x: product_mapping.get(x, 'Unknown'))
    return df_merged


url = "https://archive.prod.vertexprotocol.com/v1"
req = httpx.post(url, json=payload).json()

# List of keys to process in each snapshot
keys = ['cumulative_volumes', 'cumulative_trade_sizes', 'cumulative_trades',
        'cumulative_taker_fees', 'cumulative_maker_fees', 'cumulative_liquidation_amounts',
        'open_interests', 'total_deposits', 'total_borrows', 'funding_rates',
        'deposit_rates', 'borrow_rates', 'cumulative_inflows', 'cumulative_outflows']

directory = "vertex_data/snapshots"
if not os.path.exists(directory):
    os.makedirs(directory)

for snapshot in req['snapshots']:
    timestamp = snapshot['timestamp']
    df_snapshot = process_snapshot(snapshot, keys, timestamp,product_mapping)

    # Export to CSV
    filename = f"{directory}/snapshot_{timestamp}.csv"
    df_snapshot.to_csv(filename, index=False)
    print(f"Data written to {filename}")

import os
import requests
import pandas as pd
from supabase import create_client

# Load Config
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
feed_url = os.environ.get("AWIN_FEED_URL")
supabase = create_client(url, key)

def run_sync():
    # 1. Download and Decompress
    print("Fetching data from Awin...")
    response = requests.get(feed_url)
    
    with open("data.csv.gz", "wb") as f:
        f.write(response.content)

    # 2. Process with Pandas
    print("Processing CSV...")
    df = pd.read_csv("data.csv.gz", compression='gzip')
    
    # Clean up the data for JSON
    df = df.replace([float('inf'), float('-inf')], float('nan'))
    df = df.astype(object).where(pd.notnull(df), None)
    
    # Convert to dictionary for Supabase
    data = df.to_dict(orient='records')

    # 3. Wipe the old data
    print("Clearing old deals from the database...")
    # Using '0' as a dummy value. Since Awin IDs are never 0, this deletes all rows.
    # Note: If your aw_product_id column is strictly TEXT, change 0 to "0"
    supabase.table("LastMinute").delete().neq("aw_product_id", 0).execute()

    # 4. Upsert the fresh data
    print(f"Syncing {len(data)} fresh rows...")
    
    # Batching the upload (Awin feeds can be large, this prevents timeout crashes)
    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        supabase.table("LastMinute").upsert(batch, on_conflict="aw_product_id").execute()
        print(f"Synced {min(i + batch_size, len(data))} out of {len(data)}...")
        
    print("Sync complete!")

if __name__ == "__main__":
    run_sync()

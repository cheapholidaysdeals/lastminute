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
    df = pd.read_csv("data.csv.gz", compression='gzip')
    
    # Rename columns to match your Supabase schema exactly
    df.columns = [c.replace(':', '_').lower() for c in df.columns]
    
    # FIX: Convert missing/empty values (NaN) to None so JSON doesn't crash
    df = df.where(pd.notnull(df), None)
    
    # Convert to dictionary for Supabase
    data = df.to_dict(orient='records')

    # 3. Upsert to Supabase
    print(f"Syncing {len(data)} rows...")
    supabase.table("LastMinute").upsert(data, on_conflict="aw_product_id").execute()
    print("Sync complete!")

if __name__ == "__main__":
    run_sync()

import json
import psycopg2
from dotenv import load_dotenv
import os
import requests

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

BASE_URL = "https://api.sportmonks.com/v3/core/"
ENDPOINT = "countries"
TABLE_NAME = "sportmonks_src_countries"

# Connect to Postgres
conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
)
cur = conn.cursor()

# Create schema + table if not exists
cur.execute(f"""
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE TABLE IF NOT EXISTS ingestion.{TABLE_NAME} (
    id BIGINT PRIMARY KEY,
    data JSONB
);
""")
conn.commit()

# Fetch all pages loop
print(f"🔄 Fetching {ENDPOINT} from Sportmonks...")

data = []
page = 1
per_page = 50  # fetch 50 countries per page to reduce API calls

while True:
    url = f"{BASE_URL}{ENDPOINT}?api_token={API_TOKEN}&page={page}&per_page={per_page}"
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()

    if "data" not in json_data or not json_data["data"]:
        print("✅ No more data, exiting loop.")
        break

    data.extend(json_data["data"])
    print(f"📄 Fetched page {page}, {len(json_data['data'])} records so far...")
    page += 1

print(f"📦 Total records retrieved: {len(data)}")

# Insert / update into Postgres in batches
batch_size = 100
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    values = [(item["id"], json.dumps(item, ensure_ascii=False)) for item in batch]
    cur.executemany(f"""
        INSERT INTO ingestion.{TABLE_NAME} (id, data)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE
        SET data = EXCLUDED.data
    """, values)
    conn.commit()
    print(f"💾 Saved batch {i // batch_size + 1} ({len(batch)} records) to Postgres...")

cur.close()
conn.close()
print(f"✅ {ENDPOINT.capitalize()} successfully saved in Postgres.")

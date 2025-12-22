import json
import psycopg2
import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
BASE_URL = "https://api.sportmonks.com/v3/football/"
ENDPOINT = "fixtures"
TABLE_NAME = "sportmonks_src_fixtures"
CONTEXT = "fixtures"  # bijvoorbeeld "eredivisie_2024" of "premier_league_2024"

INCLUDES = ['lineups', 'events', 'participants', 'coaches', 'tvStations', 'referees', 'formations', 'scores']  # voeg hier toe wat je wil includen
MAX_CALLS_PER_RUN = 2500
PER_PAGE = 50

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)
cur = conn.cursor()

# Setup tables
cur.execute("""
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE TABLE IF NOT EXISTS ingestion.api_progress (
    endpoint TEXT NOT NULL,
    context TEXT,
    last_page INT,
    PRIMARY KEY (endpoint, context)
);
CREATE TABLE IF NOT EXISTS ingestion.sportmonks_src_fixtures (
    id BIGINT PRIMARY KEY,
    data JSONB
);
""")
conn.commit()

# Resume progress
cur.execute("""
SELECT last_page FROM ingestion.api_progress
WHERE endpoint = %s AND context = %s;
""", (ENDPOINT, CONTEXT))
row = cur.fetchone()
page = row[0] + 1 if row else 1

print(f"🔄 Starting from page {page} for {ENDPOINT} ({CONTEXT})")

total_inserted = 0
call_count = 0
include_param = f"&include={';'.join(INCLUDES)}" if INCLUDES else ""

while call_count < MAX_CALLS_PER_RUN:
    url = f"{BASE_URL}{ENDPOINT}?api_token={API_TOKEN}&page={page}&per_page={PER_PAGE}{include_param}"
    try:
        response = requests.get(url)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after else 120
            print(f"🚦 Rate limit reached. Waiting {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error: {e}. Waiting 30s and retrying...")
        time.sleep(30)
        continue

    json_data = response.json()
    records = json_data.get("data", [])
    if not records:
        print("✅ No more data, exiting loop.")
        break

    # Insert batch
    values = [(item["id"], json.dumps(item, ensure_ascii=False)) for item in records]
    cur.executemany(f"""
        INSERT INTO ingestion.{TABLE_NAME} (id, data)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
    """, values)
    conn.commit()

    total_inserted += len(records)
    call_count += 1
    print(f"💾 Page {page}: {len(records)} records saved. Total: {total_inserted}. Calls used: {call_count}/{MAX_CALLS_PER_RUN}")

    # Save progress
    cur.execute("""
        INSERT INTO ingestion.api_progress (endpoint, context, last_page)
        VALUES (%s, %s, %s)
        ON CONFLICT (endpoint, context)
        DO UPDATE SET last_page = EXCLUDED.last_page;
    """, (ENDPOINT, CONTEXT, page))
    conn.commit()

    page += 1

print(f"🏁 Run complete for {ENDPOINT} ({CONTEXT}): {total_inserted} records saved, {call_count} API calls used.")
print("⏭️ Next run will continue from the next page.")

cur.close()
conn.close()

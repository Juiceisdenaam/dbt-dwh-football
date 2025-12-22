import json
import psycopg2
import os
import requests
import time
import logging
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
API_TOKEN = os.getenv("API_TOKEN")
BASE_URL = "https://api.sportmonks.com/v3/football/odds/"
ENDPOINT = "pre-match"
TABLE_NAME = "sportmonks_src_pre_match_odds"
CONTEXT = "pre_match"

INCLUDES = []  # bijv. ["fixture", "markets"]
MAX_CALLS_PER_RUN = 2995
PER_PAGE = 50

# Filters (1 market, 1 bookmaker)
MARKET_ID = 1
BOOKMAKER_ID = 2
filters_param = f"&filters=markets:{MARKET_ID};bookmakers:{BOOKMAKER_ID}"

# === LOGGING ===
logging.basicConfig(
    filename="sportmonks_pre_match.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
print("🪵 Logging naar sportmonks_pre_match.log")

# === DB CONNECTIE ===
conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)
cur = conn.cursor()

# === DB SETUP ===
cur.execute("""
CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.api_progress (
    endpoint TEXT NOT NULL,
    context TEXT,
    last_page INT,
    PRIMARY KEY (endpoint, context)
);

CREATE TABLE IF NOT EXISTS ingestion.sportmonks_src_pre_match_odds (
    id BIGINT PRIMARY KEY,
    data JSONB
);
""")
conn.commit()

# === RESUME PROGRESS ===
cur.execute("""
SELECT last_page FROM ingestion.api_progress
WHERE endpoint = %s AND context = %s;
""", (ENDPOINT, CONTEXT))
row = cur.fetchone()
page = row[0] + 1 if row else 1

print(f"🔄 Starting from page {page} for {ENDPOINT} ({CONTEXT})")
logging.info(f"Starting from page {page} for {ENDPOINT} ({CONTEXT})")

total_inserted = 0
call_count = 0
empty_pages = 0
include_param = f"&include={','.join(INCLUDES)}" if INCLUDES else ""

# === MAIN LOOP ===
while call_count < MAX_CALLS_PER_RUN:
    url = f"{BASE_URL}{ENDPOINT}?api_token={API_TOKEN}&page={page}&per_page={PER_PAGE}{filters_param}{include_param}"
    print(f"➡️ Fetching page {page} ...")
    logging.info(f"Fetching page {page} ...")

    # Retry logic per pagina
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=60)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after else 120
                msg = f"🚦 Rate limit reached. Waiting {wait_seconds}s (Ctrl+C to stop)..."
                print(msg)
                logging.warning(msg)
                for _ in range(wait_seconds):
                    time.sleep(1)
                continue

            response.raise_for_status()
            break  # ✅ success — verlaat retry-loop

        except KeyboardInterrupt:
            print("🛑 Script manually stopped by user.")
            logging.warning("Script manually stopped by user.")
            cur.close()
            conn.close()
            raise SystemExit

        except requests.exceptions.RequestException as e:
            msg = f"⚠️ Attempt {attempt+1}/{MAX_RETRIES} failed on page {page}: {e}"
            print(msg)
            logging.warning(msg)
            if attempt < MAX_RETRIES - 1:
                print("⏳ Retrying in 15s...")
                for _ in range(15):
                    time.sleep(1)
            else:
                print("🚫 Skipping this page after 3 failed attempts.")
                logging.error(f"Skipped page {page} after 3 failed attempts.")
                response = None
                break

    # Sla over als 3x gefaald
    if not response:
        page += 1
        continue

    json_data = response.json()
    records = json_data.get("data", [])
    meta = json_data.get("meta", {}).get("pagination", {})
    has_more = meta.get("has_more", False)

    # Check voor lege resultaten
    if not records:
        empty_pages += 1
        print(f"⚠️ Empty page ({empty_pages} in a row).")
        logging.info(f"Empty page {page} ({empty_pages} in a row).")
        if empty_pages > 3:
            print("🚫 Too many empty pages, stopping loop.")
            logging.warning("Stopped due to too many empty pages.")
            break
        if not has_more:
            print("✅ No more data, exiting loop.")
            logging.info("No more data, exiting loop.")
            break
        page += 1
        continue
    else:
        empty_pages = 0

    # === INSERT BATCH ===
    values = [(item["id"], json.dumps(item, ensure_ascii=False)) for item in records]
    cur.executemany(f"""
        INSERT INTO ingestion.{TABLE_NAME} (id, data)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
    """, values)
    conn.commit()

    total_inserted += len(records)
    call_count += 1
    remaining = response.headers.get("X-RateLimit-Remaining")

    msg = f"💾 Page {page}: {len(records)} records saved. Total: {total_inserted}. Calls used: {call_count}/{MAX_CALLS_PER_RUN}"
    print(msg)
    logging.info(msg)
    if remaining is not None:
        print(f"🔢 Remaining API calls: {remaining}")
        logging.info(f"Remaining API calls: {remaining}")

    # === SAVE PROGRESS ===
    cur.execute("""
        INSERT INTO ingestion.api_progress (endpoint, context, last_page)
        VALUES (%s, %s, %s)
        ON CONFLICT (endpoint, context)
        DO UPDATE SET last_page = EXCLUDED.last_page;
    """, (ENDPOINT, CONTEXT, page))
    conn.commit()

    page += 1

print(f"🏁 Run complete for {ENDPOINT} ({CONTEXT}): {total_inserted} records saved, {call_count} API calls used.")
logging.info(f"Run complete for {ENDPOINT} ({CONTEXT}): {total_inserted} records saved, {call_count} API calls used.")
print("⏭️ Next run will continue from the next page.")

cur.close()
conn.close()

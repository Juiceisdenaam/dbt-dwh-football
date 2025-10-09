import json
import psycopg2
import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
BASE_URL = "https://api.sportmonks.com/v3/football/"
ENDPOINT_TEMPLATE = "squads/seasons/{season_id}/teams/{team_id}"
TABLE_NAME = "sportmonks_src_squads"
CONTEXT = "squads"

MAX_CALLS_PER_RUN = 2800

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

CREATE TABLE IF NOT EXISTS ingestion.api_progress_1 (
    endpoint TEXT NOT NULL,
    context TEXT,
    last_index INT,
    PRIMARY KEY (endpoint, context)
);

CREATE TABLE IF NOT EXISTS ingestion.sportmonks_src_squads (
    id TEXT PRIMARY KEY,
    season_id INT NOT NULL,
    team_id INT NOT NULL,
    data JSONB
);
""")
conn.commit()

# Haal lijst van alle combinaties uit hulp_table
cur.execute("""
SELECT season_id, team_id
FROM ingestion.hulp_table_2
ORDER BY season_id, team_id;
""")
all_rows = cur.fetchall()
total_rows = len(all_rows)

# Resume progress
cur.execute("""
SELECT last_index FROM ingestion.api_progress_1
WHERE endpoint = %s AND context = %s;
""", (ENDPOINT_TEMPLATE, CONTEXT))
row = cur.fetchone()
start_index = row[0] + 1 if row else 0

print(f"🔄 Starting from index {start_index} / {total_rows} for {ENDPOINT_TEMPLATE} ({CONTEXT})")

total_inserted = 0
call_count = 0

for i in range(start_index, total_rows):
    if call_count >= MAX_CALLS_PER_RUN:
        print("⚠️ Max API calls reached for this run, stopping...")
        break

    season_id, team_id = all_rows[i]
    url = f"{BASE_URL}squads/seasons/{season_id}/teams/{team_id}?api_token={API_TOKEN}"

    try:
        response = requests.get(url)
        if response.status_code == 429:
            print(f"🚦 Rate limit reached at season {season_id}, team {team_id}. Stopping run.")
            break  # stop direct bij rate limit
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error: {e}. Skipping this call.")
        continue

    json_data = response.json()
    records = json_data.get("data", [])
    if not records:
        print(f"✅ No data for season {season_id}, team {team_id}. Saving empty JSON.")
        records = []

    unique_id = f"{season_id}_{team_id}"  # unieke id per team-season call
    cur.execute(f"""
        INSERT INTO ingestion.{TABLE_NAME} (id, season_id, team_id, data)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
    """, (
        unique_id,
        season_id,
        team_id,
        json.dumps(records, ensure_ascii=False)
    ))
    conn.commit()

    total_inserted += 1
    call_count += 1
    print(f"💾 {i+1}/{total_rows} - season {season_id}, team {team_id}: 1 record saved. Total records: {total_inserted}. Calls: {call_count}/{MAX_CALLS_PER_RUN}")

    # Save progress
    cur.execute("""
        INSERT INTO ingestion.api_progress_1 (endpoint, context, last_index)
        VALUES (%s, %s, %s)
        ON CONFLICT (endpoint, context)
        DO UPDATE SET last_index = EXCLUDED.last_index;
    """, (ENDPOINT_TEMPLATE, CONTEXT, i))
    conn.commit()

print(f"🏁 Run complete for {ENDPOINT_TEMPLATE} ({CONTEXT}): {total_inserted} records saved, {call_count} API calls used.")
print("⏭️ Next run will continue from the next index.")

cur.close()
conn.close()

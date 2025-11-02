# ⚽ Football Data Warehouse (dbt + PostgreSQL)

A modular **Football Data Warehouse** built with **dbt** and **PostgreSQL**, designed to transform raw football datasets into clean, analysis-ready tables.  
This project demonstrates best practices in **data modeling, SQL performance optimization, and dimensional design** using modern analytics engineering principles.

---

## 🧱 Project Overview

This project follows a layered dbt structure:
- **staging/** – cleans and standardizes raw data  
- **intermediate/** – joins and enriches datasets  
- **marts/** – dimensional models (`dim_`) and fact tables (`fct_`)  
- **analysis/** – final analytical queries and KPIs  

The goal is to create a reproducible foundation for advanced football analytics — from match results to transfers, player statistics, and broadcast data.

---

## 🗂️ Folder Structure

dbt_football_warehouse/
│
├── models/
│ ├── staging/
│ │ ├── stg_players.sql
│ │ ├── stg_teams.sql
│ │ ├── stg_fixtures.sql
│ │ └── ...
│ │
│ ├── intermediate/
│ │ ├── int_fixtures.sql
│ │ ├── int_events.sql
│ │ └── ...
│ │
│ ├── marts/
│ │ ├── dim/
│ │ │ ├── dim_players.sql
│ │ │ ├── dim_teams.sql
│ │ │ └── ...
│ │ ├── fcts/
│ │ │ ├── fct_fixtures.sql
│ │ │ ├── fct_transfers.sql
│ │ │ └── ...
│ │
│ └── analysis/
│ └── avg_height_per_team.sql
│
└── dbt_project.yml

yaml
Code kopiëren

---

## 🏗️ Data Model Layers

### 1️⃣ Staging Layer
Cleans and standardizes raw football data (players, fixtures, transfers, etc.), ensuring consistent column naming, typing, and key generation.

Example:
```sql
-- stg_players.sql
select
    player_id,
    trim(player_name) as player_name,
    cast(height as integer) as height_cm,
    cast(weight as integer) as weight_kg,
    nationality,
    team_id
from {{ source('raw', 'players') }}
2️⃣ Intermediate Layer
Combines and enriches staging data.
Example: linking fixtures with player lineups and match metadata.

sql
Code kopiëren
-- int_fixtures_lineups.sql
select
    f.fixture_id,
    l.player_id,
    l.position,
    l.starting,
    f.date as match_date
from {{ ref('stg_fixtures') }} f
join {{ ref('stg_fixtures_lineups') }} l using (fixture_id)
3️⃣ Marts Layer
Implements the dimensional model with:

dim_ — static reference entities (players, teams, leagues)

fct_ — event-based data (fixtures, transfers, standings)

sql
Code kopiëren
-- fct_fixtures.sql
select
    fixture_id,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    date
from {{ ref('int_fixtures') }}
📊 Analysis Layer
The analysis/ folder contains domain-specific insights and KPIs.
Example:

sql
Code kopiëren
-- avg_height_per_team.sql
select
    t.team_name,
    round(avg(p.height_cm), 2) as avg_height
from {{ ref('dim_players') }} p
join {{ ref('dim_teams') }} t on p.team_id = t.team_id
group by t.team_name
order by avg_height desc
Example output:

team_name	avg_height
FC Bayern	184.3 cm
PSG	182.7 cm
Arsenal	181.9 cm

🧠 Planned Analyses
Planned analytical models include:

🧍‍♂️ Average age per team

🔁 Player transfer network

🟥 Top referees (cards/game)

📈 Match momentum by minute

📺 TV coverage vs. attendance

🚀 Setup Instructions
Prerequisites
dbt Core

PostgreSQL (or compatible warehouse)

Run the Project
bash
Code kopiëren
# Install dependencies
dbt deps

# Test connections
dbt debug

# Run all models
dbt run

# Run specific model
dbt run --select fct_fixtures

# Test and generate docs
dbt test
dbt docs generate
dbt docs serve
🧩 Tech Stack
Tool	Purpose
dbt Core	Data modeling & transformation
PostgreSQL	Data warehouse
GitHub Actions	CI/CD & model testing
VS Code	Development environment

🧾 Learnings
Modular dbt project design improves maintainability

Consistent naming (stg_, int_, dim_, fct_) improves readability

PostgreSQL-specific optimizations (indexed joins, date handling) improve performance

Documentation and testing ensure data reliability

📰 Related Article
For a deeper explanation of the design process and analytics, read the full Substack post:
👉 Building a Football Data Warehouse with dbt: From Raw Data to Analytics

🧑‍💻 Author
[Your Name]
Data Engineer / Analytics Engineer
📧 [your.email@example.com]
🔗 LinkedIn • Substack • Portfolio

📜 License
This project is released under the MIT License.
# **DBT Football Warehouse Project (In Progress)**

## **Overview**
This project builds a **football analytics data warehouse** using **dbt**, **PostgreSQL**, and data ingested from the **Sportmonks Football API** via Python.  
The goal is to create a scalable architecture for football data analysis, enabling both tactical insights and fun queries.

### **Data Source**
- **Sportmonks Football API**: Top 5 European leagues, last 10 seasons  
Includes entities such as:
- Fixtures, lineups, formations
- Players, coaches, referees
- Teams, squads, standings
- Venues, TV stations, transfers

---

## **Objectives**
1. **Ingest raw JSON data** from Sportmonks API into PostgreSQL using Python scripts.
2. **Model data using dbt** across layers:
   - Sources → Staging → Intermediate → Marts (dim & fct)
3. **Perform analysis** on tactical and fun metrics (e.g., impactful goals after 85th minute).
4. **Document models** and generate dbt docs for transparency.

---

## **Current Status**
✅ **API Ingestion**: Python scripts for all major entities  
✅ **dbt Project Setup**: Sources, staging, intermediate, marts layers created  
✅ **Initial Analysis Queries**: Fun insights added in `analysis/`  
⏳ **Next Steps**:
- Expand analysis layer with more tactical metrics
- Improve documentation and add ERD diagrams
- Host dbt docs on GitHub Pages

---

## **Tech Stack**
- **Database**: PostgreSQL  
- **Transformation**: dbt  
- **ETL**: Python (Sportmonks API, psycopg2)  
- **Version Control**: Git  

---

## **Repository Structure**
```
DBT-DWH-FOOTBALL/
│
├── dbt_football_warehouse/
│   ├── models/
│   │   ├── sources/        # Raw source definitions
│   │   ├── staging/        # JSONB parsing & column renaming
│   │   ├── intermediate/   # Business logic joins
│   │   ├── marts/          # Dimensional & fact tables
│   │   └── analysis/       # Fun & tactical analysis queries
│   │
│   ├── macros/             # Custom dbt macros
│   ├── tests/              # Data quality tests
│   └── seeds/              # Static reference data
│
├── ingestion/
│   └── football/           # Python scripts for API ingestion
│       ├── sportmonks_src_coaches.py
│       ├── sportmonks_src_fixtures.py
│       ├── sportmonks_src_players.py
│       └── ... (other entities)
│
└── README.md
```

---

## **Analysis Examples**
- **Average height per team**
- **Impactful goals after the 85th minute**
- **Most common first name in a lineup**
- **Last five players wearing jersey number 10**

---

## **Plan of Approach**
### Step-by-Step Roadmap
1. **API Ingestion**
   - Retrieve data from Sportmonks API using Python
   - Load raw JSON into PostgreSQL
2. **dbt Modeling**
   - Create staging models for each entity
   - Build intermediate joins for fixtures, lineups, and events
   - Design marts layer with dim and fact tables
3. **Analysis**
   - Add tactical and fun queries in `analysis/`
4. **Documentation**
   - Generate dbt docs and ERD diagrams
   - Publish on GitHub Pages

---

## **Why This Project?**
This project demonstrates:
- **Data Engineering Skills**: ETL, dbt modeling, SQL transformations
- **Football Analytics**: Tactical insights and creative queries
- **Documentation & Portfolio**: Clear structure for showcasing technical expertise

---

## **How to Run**
1. Clone the repo:
   ```bash
   git clone <repo-url>
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your `.env` file with DB credentials and API keys.
4. Run dbt:
   ```bash
   dbt run
   dbt test
   dbt docs generate
   ```

---

# Podcast Analytics - Analytics Engineer Tech Test

This project implements a complete analytics solution for podcast streaming data using **dbt** and **DuckDB**.

## 🏗️ Architecture Overview

The solution follows a **medallion architecture** with layered data transformations:

- **Sources**: Raw CSV files (`event_logs.csv`, `users.csv`, `episodes.csv`)
- **Staging**: Data cleaning and validation (`stg_*` models)
- **Intermediate**: Business logic and complex calculations (`int_*` models)
- **Marts**: Analytics-ready dimensional models (`dim_*` and `fact_*` tables)

## 📊 Data Model

### Dimensional Model (Star Schema)

**Dimension Tables:**
- `dim_users`: User demographics and cohort information
- `dim_episodes`: Episode metadata and categorization

**Fact Tables:**
- `fact_listening_sessions`: Granular session-level metrics
- `fact_daily_user_activity`: Daily user engagement aggregations

### Key Metrics Calculated
- **Listen-through rate**: `total_listen_duration / episode_duration`
- **Session metrics**: Duration, event counts, completion status
- **User engagement scores**: Weighted activity scoring
- **Power user identification**: Users listening to 3+ episodes per day

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- dbt-core
- dbt-duckdb

### Installation

1. **Install dbt and DuckDB adapter:**
```bash
pip install dbt-core dbt-duckdb
```

2. **Install dbt packages:**
```bash
dbt deps
```

3. **Set up DuckDB profile:**
The project includes a `profiles.yml` file configured for DuckDB. The database will be created as `podcast_analytics.duckdb` in the project root.

### Running the Project

1. **Load CSV data into DuckDB:**
```bash
# The sources are configured to read directly from CSV files
# DuckDB will automatically read from the sources/ directory
```

2. **Run dbt models:**
```bash
# Run all models
dbt run

# Run with tests
dbt run && dbt test
```

3. **Generate documentation:**
```bash
dbt docs generate
dbt docs serve
```

### Project Structure
```
├── analyses/              # Analysis queries
├── models/
│   ├── staging/          # Data cleaning (views)
│   ├── intermediate/     # Business logic (views)
│   ├── marts/           # Analytics tables (tables)
│   └── sources.yml      # Source definitions
├── sources/             # Raw CSV files
├── tests/              # Custom data quality tests
├── dbt_project.yml     # Project configuration
├── packages.yml        # Dependencies
└── profiles.yml        # DuckDB connection
```

## 📈 Analysis Queries

The project includes several analysis queries in the `analyses/` folder:

### 1. Top Episodes (Past 7 Days)
```sql
dbt compile --select analyses/top_episodes_past_7_days
```
**Output**: Top 10 most completed episodes with completion counts and listen-through rates.

### 2. Listen-Through Rate by Country
```sql
dbt compile --select analyses/listen_through_rate_by_country
```
**Output**: Average listen-through rates aggregated by user country.

### 3. Power Users Analysis
```sql
dbt compile --select analyses/power_users_analysis
```
**Output**: Users who listened to 3+ different episodes in one day.

### 4. Additional Insights
```sql
dbt compile --select analyses/additional_insights
```
**Output**: Podcast performance, user cohort analysis, and engagement by episode length.

## 🧪 Data Quality

### Tests Implemented
- **Source validation**: Null checks, unique constraints
- **Referential integrity**: Foreign key relationships
- **Business logic validation**: Listen-through rates, timestamp ranges
- **Custom tests**: Future event detection, reasonable rate validation

### Running Tests
```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --select stg_events
```

## ⚙️ Orchestration Considerations

For production deployment, consider:

### Airflow DAG Structure
```python
# Example DAG outline
extract_data_task >> dbt_run_task >> dbt_test_task >> notify_task
```

### Incremental Processing
- Modify staging models to be incremental based on `event_timestamp`
- Implement backfill logic for historical data reprocessing
- Add partition pruning for large datasets

### Monitoring
- Set up dbt test notifications
- Monitor data freshness
- Track model run times and failures

## 🎯 Key Design Decisions

### 1. **CSV Sources vs Seeds**
- **Decision**: Treat CSV files as **sources** rather than seeds
- **Rationale**: Sources represent external data that changes independently, while seeds are for static reference data

### 2. **Session Definition**
- **Decision**: Define sessions with 30-minute inactivity gaps
- **Rationale**: Balances granularity with realistic user behavior patterns

### 3. **Listen-Through Rate Calculation**
- **Decision**: `total_listen_duration / episode_duration`
- **Rationale**: Accounts for replays and partial listens, can exceed 100%

### 4. **Power User Definition**
- **Decision**: 3+ different episodes per day
- **Rationale**: Indicates high engagement while being achievable for active users

### 5. **Materialization Strategy**
- **Staging/Intermediate**: Views (for flexibility and cost)
- **Marts**: Tables (for performance in analytics queries)

## 📝 Assumptions Made

1. **Duplicate Events**: Removed duplicates based on user_id, episode_id, timestamp, and event_type
2. **Missing Durations**: Only play/complete events should have duration values
3. **Timezone**: All timestamps assumed to be in UTC
4. **Data Quality**: Events without user_id or episode_id are filtered out
5. **Analysis Period**: Using configurable `analysis_date` variable for testing consistency

## 🔍 Sample Queries

### Most Engaged Users by Country
```sql
SELECT 
    country,
    COUNT(DISTINCT user_id) as active_users,
    AVG(daily_engagement_score) as avg_engagement
FROM fact_daily_user_activity
GROUP BY country
ORDER BY avg_engagement DESC;
```

### Episode Performance Dashboard
```sql
SELECT 
    e.title,
    COUNT(DISTINCT s.user_id) as unique_listeners,
    AVG(s.listen_through_rate) as avg_completion_rate,
    SUM(s.completed_episode) as total_completions
FROM fact_listening_sessions s
JOIN dim_episodes e ON s.episode_id = e.episode_id
GROUP BY e.title
ORDER BY unique_listeners DESC;
```

## 🚨 Known Limitations

1. **Memory Usage**: Large datasets may require partitioning strategies
2. **Real-time Processing**: Current design is batch-oriented
3. **Advanced Analytics**: Could benefit from window functions for cohort analysis
4. **Data Validation**: More sophisticated anomaly detection could be added
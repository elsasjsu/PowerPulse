# PowerPulse
Real-Time Electricity Demand &amp; Generation Analytics

PowerPulse
Real-Time Electricity Demand & Generation Analytics

PowerPulse is an end-to-end data engineering and analytics solution designed to monitor, analyze, and visualize real-time and historical electricity demand and generation across U.S. balancing authorities. The project integrates automated data ingestion, scalable data transformation, cloud warehousing, and interactive dashboarding to support operational intelligence and energy-sector decision-making.

🚀 Project Overview

PowerPulse combines data from the U.S. Energy Information Administration (EIA) and the California ISO (CAISO) to deliver insights into electricity consumption, generation, forecasting accuracy, regional performance, and hourly demand patterns.
The system uses a modern data stack—Apache Airflow, Snowflake, dbt, and Power BI—to automate pipelines, manage transformations, and deliver business-ready analytics.

📊 Key Features

Automated ETL/ELT workflows using Airflow

Historical + Real-time data integration

Cloud-native data warehousing with Snowflake

Modular SQL transformations using dbt

Star-schema analytics model for fast querying

Interactive Power BI dashboards showing trends, regional behavior, KPIs, and heatmaps

Scalable and reproducible architecture suitable for production deployment

🗄️ Data Sources
1. EIA Historical Data

Source: U.S. Energy Information Administration (EIA-930)

Granularity: Hourly demand, forecast, generation, interchange

Format: JSON API

Geography: State & regional balancing authorities

Coverage: Multi-year (historical trends)

2. CAISO Real-Time Data

Source: CAISO “Today’s Outlook” API

Update Frequency: Every 5–10 minutes

Metrics: Load, renewable generation, imports/exports

Format: XML/JSON feeds

Coverage: California grid operations

Combined, these sources produce 8M+ hourly records across 50+ data series.

🔧 Architecture
Orchestration – Apache Airflow

Schedules ETL & ELT pipelines

Extracts data from APIs

Triggers dbt transformations

Handles retries, logging, and monitoring

Data Warehouse – Snowflake

RAW Layer: Unprocessed API ingestions

STAGING Layer: Cleaned and standardized tables

ANALYTICS Layer: Curated fact and dimension models (star schema)

Transformation – dbt

Staging & analytics SQL models

Automated testing (quality checks)

Incremental processing

Snapshot tracking for historical attributes

Documentation & lineage generation

Visualization – Power BI

Interactive dashboards visualize:

Daily/Hourly demand patterns

Regional comparisons

Forecast accuracy

Net generation trends

Weekday vs. weekend behavior

Heat maps & summary matrices

🧱 Database Schema (Star Model)

Dimensions:

DimDate

DimRegion

DimSeries

Facts:

FactRegionHourly

FactRegionComp

FactMovingAvg

FactWeekdayWeekend

This structure supports flexible slicing by date, region, and metric type.

📈 Dashboard Insights

Power BI dashboards include:

Peak, minimum, average, and moving-average KPIs

Time-series line charts across series

Regional performance bar charts

Series distribution donut chart

Weekday/weekend summary matrix

Hourly heatmap for load patterns

📂 Repository Structure
PowerPulse/
│── airflow/               # DAG scripts for ETL/ELT
│── dbt/                   # dbt models, tests, snapshots
│── sql/                   # Additional SQL scripts
│── notebooks/             # Exploration & validation notebooks
│── dashboards/            # Power BI files
│── docs/                  # Documentation, diagrams, reports
│── README.md              # Project overview

🛠️ Tech Stack

Apache Airflow

Snowflake Cloud Data Warehouse

dbt Core

Power BI / Tableau

Python

REST APIs (EIA, CAISO)

📌 Use Cases

Grid demand forecasting

Renewable integration monitoring

Regional electricity market analysis

Operational planning & reliability studies

Anomaly detection in demand patterns

Real-time situational awareness

📬 Contributors

Group Project 9

Bavishna Ashok Kumar

Elsa Rose

Kruthika Virupakshappa

Shruti Naik

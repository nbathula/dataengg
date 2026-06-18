# Data Engineering

Production data engineering projects — ingestion pipelines, lakehouse architecture, and streaming infrastructure.

---

## Projects

### [marketo-data-streams/](marketo-data-streams/)

Real-time and batch ingestion pipeline for Marketo marketing data on Apache Iceberg. Shared curated core in Unity Catalog with engine-native marts for BI (Snowflake) and ML (Databricks). Gold layer doubles as the central grounding layer for AI agents.

**Stack:** AWS API Gateway · Lambda · Kafka · Databricks Auto Loader · Spark Streaming · Apache Iceberg · Unity Catalog · dbt · Snowflake

---

### [flink-ecommerce/](flink-ecommerce/)

Real-time ecommerce analytics pipeline built on Apache Flink and Apache Kafka. A Python producer continuously generates simulated ecommerce events (orders, cart interactions, page views) into Kafka topics. Apache Flink processes the stream using SQL with tumbling, sliding, and cumulative windows to compute business metrics — revenue per category, top trending products, cart abandonment signals, cart-to-order conversion, and lifetime customer spend. Includes full technical documentation and requirements.

**Stack:** Apache Flink 1.18 · Apache Kafka · Flink SQL · Kafka SQL Connector · Python · Docker Compose

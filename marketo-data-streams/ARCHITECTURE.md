# Marketo Data Streams — Architecture

## Overview

The Marketo Data Streams (MDS) pipeline ingests marketing activity data from Marketo via two paths — historical bulk exports and near-realtime webhooks — and lands them in a shared Apache Iceberg core managed by Unity Catalog. Downstream consumers (BI, ML, AI) read from engine-native materializations of that shared core, never from raw sources directly.

---

## Layer Breakdown

### Layer 1 — Sources

| Source | Type | Description |
|--------|------|-------------|
| Historical + Batch | Files | Marketo bulk export API dumps (leads, activities, campaigns, programs). Scheduled daily or on-demand. |
| Webhooks | Near-realtime | Marketo webhook callbacks on lead state changes, email events, form fills, etc. Sub-minute latency. |

**Key decision:** both paths share the same Bronze schema. The ingestion layer normalises field names so downstream layers never see source-format differences.

---

### Layer 2 — Ingestion

```
Marketo Webhook → API Gateway → Lambda → SQS DLQ (on failure)
                                       ↓
                               S3 Landing  or  Kafka Topic
```

| Component | Purpose |
|-----------|---------|
| **API Gateway** | TLS termination, auth (HMAC signature validation for Marketo webhooks) |
| **Lambda** | Lightweight fan-out: validates payload, writes to S3 (batch) or publishes to Kafka (streaming). Retries via SQS DLQ on failure. |
| **S3 Landing** | Immutable raw drop zone for bulk files and Lambda-routed payloads. Partitioned by `source/date/hour`. |
| **Kafka** | Near-realtime event bus. One topic per Marketo event type (lead_change, email_click, form_fill, etc.). Retention: 7 days. |
| **SQS DLQ** | Captures Lambda failures for manual replay. Alarm on queue depth > 0. |

---

### Layer 3 — Processing

| Component | Reads from | Writes to | Trigger |
|-----------|-----------|-----------|---------|
| **Auto Loader** (Databricks) | S3 Landing | Unity Catalog RAW (Bronze) | File arrival (cloudFiles trigger) |
| **Spark Structured Streaming** | Kafka topics | Unity Catalog RAW (Bronze) | Continuous, micro-batch 30s |

Both processors apply:
- Schema enforcement against the Bronze contract
- Deduplication by `marketo_id + event_timestamp`
- Metadata columns: `_ingested_at`, `_source_path`, `_pipeline_version`

---

### Layer 4 — Unity Catalog (Governance Plane)

Unity Catalog sits as the governance layer across the entire pipeline:

| Capability | Implementation |
|-----------|----------------|
| **Data lineage** | Column-level lineage tracked automatically for all Spark and SQL operations |
| **Access control** | Row-level and column-level security per team (BI, ML, AI) |
| **Data contracts** | Table constraints + expectations enforced at write time |
| **REST API** | Token-based access for external consumers and AI agents |
| **Catalog structure** | `marketo.raw.*`, `marketo.curated.*`, `marketo.marts.*` |

---

### Layer 5 — Shared Core (Apache Iceberg)

The single source of truth for all Marketo data. Stored as Iceberg tables in Unity Catalog.

#### RAW / Bronze
- **Append-only** — no updates, no deletes
- Preserves source field names and types
- Full event history retained indefinitely
- Tables: `leads_raw`, `activities_raw`, `campaigns_raw`, `programs_raw`, `email_events_raw`

#### CURATED / Gold
- **Entity-resolved** — lead identity stitched across channels using deterministic + probabilistic matching
- **Business-canonical** — field names, enums, and hierarchies follow company data dictionary
- **SCD Type 2** for slowly-changing dimensions (lead attributes, company firmographics)
- **Agent grounding layer** — AI agents query Gold exclusively; never raw
- Tables: `leads`, `activities`, `campaigns`, `programs`, `email_engagement`, `lead_journey`
- Transformation: dbt models running on Databricks, scheduled via Databricks Workflows

**Bronze → Gold transformations include:**
- Lead deduplication and merge
- Campaign hierarchy resolution (program → campaign → variant)
- Email engagement attribution
- Activity sequence reconstruction (lead journey)

---

### Layer 6 — Native Marts (Per Engine)

Rather than a single serving layer, each engine materialises from Gold in its own native format for optimal query performance.

#### Snowflake — BI Mart
- Materialised as Snowflake native tables via Snowflake's Iceberg reader or Databricks → Snowflake sync
- Optimised for OLAP: pre-aggregated metrics, flattened dimensions
- Refresh: high-frequency delta (every 15 minutes for engagement metrics, hourly for lead attributes)
- Consumers: Metabase, Tableau, Domo

#### Databricks — ML Mart
- Materialised as Delta tables in Databricks via direct Iceberg reads
- Optimised for feature engineering: wide tables, pre-joined, with rolling window features
- Refresh: near-realtime for scoring features, daily for training datasets
- Consumers: MLflow experiments, batch scoring jobs, Feature Store

---

### Layer 7 — Consumers

| Consumer | Reads from | Use case |
|----------|-----------|---------|
| **BI Tools** (Metabase, Tableau, Domo) | Snowflake BI Mart | Campaign performance, lead funnel, email analytics dashboards |
| **ML Tooling** (notebooks, models) | Databricks ML Mart | Lead scoring, churn prediction, propensity models, A/B test analysis |
| **AI Agents** | Unity Catalog Gold (REST API) | Lead enrichment, campaign Q&A, sales rep assist, anomaly investigation |

---

## Failure Modes and Mitigations

| Failure | Mitigation |
|---------|-----------|
| Webhook delivery failure | Marketo retries 3x; SQS DLQ captures Lambda failures |
| S3 write failure | Lambda retries with exponential back-off |
| Kafka consumer lag | Alert on consumer group lag > 10k events; auto-scale Spark executor |
| Schema drift from Marketo | Auto Loader schema evolution + alerting on new/removed fields |
| Duplicate events | Deduplication at Bronze write time using `marketo_id + event_timestamp` |
| dbt model failure | Databricks Workflow retries + Slack alert; Gold tables remain at last-good state |
| Snowflake sync failure | BI Mart shows last-refreshed timestamp; stale data alert if > 30 min |

---

## Tech Stack Summary

| Layer | Technology |
|-------|-----------|
| Ingestion gateway | AWS API Gateway + Lambda |
| Dead letter queue | AWS SQS |
| Batch storage | AWS S3 |
| Event streaming | Apache Kafka (MSK or Confluent) |
| Batch processing | Databricks Auto Loader |
| Stream processing | Databricks Spark Structured Streaming |
| Table format | Apache Iceberg |
| Governance | Unity Catalog |
| Transformation | dbt on Databricks |
| BI serving | Snowflake (native tables) |
| ML serving | Databricks Delta / Feature Store |
| Orchestration | Databricks Workflows |
| BI tools | Metabase / Tableau / Domo |
| AI grounding | Unity Catalog REST API |

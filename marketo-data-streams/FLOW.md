# Marketo Data Streams — Data Flow Sequences

## 1. Batch / Historical Ingestion Path

```mermaid
sequenceDiagram
    participant M  as Marketo Bulk API
    participant LM as Lambda
    participant S3 as S3 Landing
    participant AL as Auto Loader
    participant BR as Bronze (Iceberg)
    participant DT as dbt / Databricks
    participant GD as Gold (Iceberg)

    M->>LM: Bulk export file (leads / activities / campaigns)
    LM->>LM: Validate HMAC signature
    LM->>S3: Write raw file (partitioned by source/date/hour)
    S3-->>AL: File arrival event (cloudFiles trigger)
    AL->>AL: Apply schema enforcement + dedup
    AL->>BR: Append to Bronze table
    Note over BR: append-only, source-fidelity
    DT->>BR: Read incremental batch
    DT->>DT: Resolve entities, apply business rules
    DT->>GD: Upsert / SCD Type 2 merge
    Note over GD: entity-resolved, business-canonical
```

---

## 2. Near-Realtime Webhook Path

```mermaid
sequenceDiagram
    participant MK as Marketo
    participant AG as API Gateway
    participant LM as Lambda
    participant DL as SQS DLQ
    participant KF as Kafka
    participant SS as Spark Streaming
    participant BR as Bronze (Iceberg)
    participant DT as dbt / Databricks
    participant GD as Gold (Iceberg)

    MK->>AG: Webhook POST (lead_change / email_click / form_fill)
    AG->>LM: Forward validated request
    alt Success
        LM->>KF: Produce event to topic (e.g. marketo.email_events)
        KF->>SS: Consume (micro-batch, 30s window)
        SS->>SS: Dedup + schema enforce
        SS->>BR: Append to Bronze table
        DT->>BR: Read incremental stream
        DT->>GD: Merge into Gold
    else Lambda failure
        LM->>DL: Send to SQS DLQ
        Note over DL: Alarm triggers on queue depth > 0
    end
```

---

## 3. Mart Refresh and Serving Path

```mermaid
sequenceDiagram
    participant GD as Gold (Iceberg / UC)
    participant WF as Databricks Workflow
    participant SF as Snowflake BI Mart
    participant DB as Databricks ML Mart
    participant BI as BI Tools
    participant ML as ML Models
    participant AI as AI Agents

    loop Every 15 min (engagement) / Hourly (attributes)
        WF->>GD: Read incremental delta from Gold
        WF->>SF: Sync to Snowflake native tables
        SF-->>BI: Dashboards refreshed
    end

    loop Near-realtime (scoring) / Daily (training)
        WF->>GD: Read feature set from Gold
        WF->>DB: Write to Delta / Feature Store
        DB-->>ML: Train or score models
    end

    AI->>GD: Query via Unity Catalog REST API
    GD-->>AI: Return grounded context (entities, history, metrics)
```

---

## 4. Lead Journey Reconstruction

```mermaid
flowchart TD
    A["Lead enters Marketo\n(form fill / import)"]
    B["lead_created event\n→ Bronze activities_raw"]
    C["Email sent\n→ email_events_raw"]
    D["Email opened / clicked\n→ email_events_raw"]
    E["Lead score change\n→ activities_raw"]
    F["MQL threshold crossed\n→ activities_raw"]
    G["CRM sync\n→ lead state change"]

    A --> B --> C --> D --> E --> F --> G

    H["dbt: lead_journey model\nStitches all events by lead_id\nBuilds ordered activity timeline"]
    B & C & D & E & F & G --> H

    H --> I["Gold: lead_journey table\nSCD Type 2 — full history"]
    I --> J["AI Agent: answers\n'What is this lead's engagement history?'"]
    I --> K["ML Model: propensity\nfeature — days_since_last_engagement"]
    I --> L["BI Dashboard:\nLead funnel stage distribution"]
```

---

## 5. Schema Evolution Handling

```mermaid
flowchart LR
    A["Marketo adds new field\n(e.g. lead.industry_code)"]
    B["Auto Loader detects\nnew column in file"]
    C{"Schema evolution\nmode?"}
    D["addNewColumns mode:\nautomatically add column\nto Bronze table schema"]
    E["Alert: new field detected\nnotify data engineering"]
    F["Review: add to\nGold dbt model?"]
    G["Deploy dbt change\npropagates to Gold + Marts"]

    A --> B --> C
    C -->|Auto Loader config| D
    D --> E --> F --> G
```

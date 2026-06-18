# Flink Ecommerce — Local Dev Setup & Technical Guide

## Overview

This guide documents the setup, architecture, and operation of a local Apache Flink streaming pipeline that simulates a real-time ecommerce data platform. The stack includes Apache Kafka as the event broker, a Python event producer generating fake ecommerce events, and Apache Flink for stream processing via SQL.

---

## Architecture

```
┌─────────────────┐     events      ┌───────────────┐     SQL queries     ┌─────────────────┐
│  Python Producer│ ──────────────► │  Apache Kafka │ ──────────────────► │  Apache Flink   │
│  (fake events)  │                 │  (3 topics)   │                     │  JobManager     │
└─────────────────┘                 └───────────────┘                     │  TaskManager    │
                                           │                              └─────────────────┘
                                           ▼
                                    ┌───────────────┐
                                    │   Kafka UI    │
                                    │  (dashboard)  │
                                    └───────────────┘
```

### Components

| Component | Image | Port | Purpose |
|---|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| Kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Event broker |
| Kafka UI | provectuslabs/kafka-ui:latest | 8080 | Visual topic browser |
| Flink JobManager | apache/flink:1.18 (custom) | 8081 | Job coordination |
| Flink TaskManager | apache/flink:1.18 (custom) | — | Stream processing |
| Producer | Python 3.11 (custom) | — | Fake event generator |

---

## Project Structure

```
flink-ecommerce/
├── docker-compose.yml          # Service definitions
├── flink/
│   └── Dockerfile              # Flink + Kafka SQL connector JAR
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             # Event generator
└── sql/
    ├── create_tables.sql       # Flink SQL table definitions
    └── queries.sql             # Example streaming queries
```

---

## Kafka Topics & Event Schemas

The producer emits events to 3 Kafka topics continuously.

### `ecommerce.orders`
```json
{
  "order_id":     "uuid",
  "user_id":      "user_0042",
  "product_id":   "p001",
  "product_name": "Laptop Pro 15",
  "category":     "Electronics",
  "quantity":     2,
  "amount":       2599.98,
  "status":       "placed",
  "event_time":   "2026-06-18T22:12:00"
}
```

### `ecommerce.cart`
```json
{
  "event_id":     "uuid",
  "user_id":      "user_0117",
  "product_id":   "p002",
  "product_name": "Wireless Headphones",
  "action":       "add",
  "quantity":     1,
  "event_time":   "2026-06-18T22:12:01"
}
```

### `ecommerce.pageviews`
```json
{
  "view_id":    "uuid",
  "user_id":    "user_0099",
  "product_id": "p003",
  "category":   "Electronics",
  "page_type":  "product",
  "event_time": "2026-06-18T22:12:02"
}
```

**Event mix (approximate):**
- 20% orders
- 35% cart events
- 45% page views

**Data volume:** ~5–15 events/second across 200 simulated users and 10 products across 4 categories (Electronics, Sports, Home, Books).

---

## Setup & Running

### Prerequisites
- Docker Desktop installed and running

### Start all services
```bash
cd ~/Learning/flink-ecommerce
docker compose up --build
```

The `--build` flag is only needed the first time (or after code changes). Subsequent starts:
```bash
docker compose up
```

### Stop all services
```bash
docker compose down
```

### Verify services are running
```bash
docker compose ps
```

Expected output — all services should show `Up`:
```
NAME                                  STATUS
flink-ecommerce-zookeeper-1           Up
flink-ecommerce-kafka-1               Up (healthy)
flink-ecommerce-kafka-ui-1            Up
flink-ecommerce-flink-jobmanager-1    Up
flink-ecommerce-flink-taskmanager-1   Up
flink-ecommerce-producer-1            Up
```

---

## Monitoring Events

### Kafka UI
Open http://localhost:8080 in a browser.

- **Topics** tab → select any topic → **Messages** tab to see live JSON events
- Watch message counts grow in real time as the producer runs

### Producer logs
```bash
docker logs -f flink-ecommerce-producer-1
```

Sample output:
```
[ORDER]    user_0042 → Laptop Pro 15 x2  $2599.98
[CART]     user_0117 add    Wireless Headphones
[CART]     user_0051 add    Running Shoes
>> totals — orders:2483  cart:4459  views:5658
```

### Flink Dashboard
Open http://localhost:8081 to see:
- Running jobs
- TaskManager slots and parallelism
- Job execution graph

---

## Running Flink SQL Queries

### Important: Session Catalog

Flink uses an **in-memory catalog** by default. Table definitions registered in one SQL session are lost when the session ends. The solution is to always include `CREATE TABLE` statements in the same SQL file as the query.

### How to run a query

**Step 1** — Write a SQL file containing both the table definition and query into the container:
```bash
cat <<'EOF' | docker exec -i flink-ecommerce-flink-jobmanager-1 bash -c "cat > /tmp/myquery.sql"
SET sql-client.execution.result-mode=TABLEAU;

CREATE TABLE orders ( ... ) WITH ( 'connector' = 'kafka', ... );

SELECT ... FROM orders ...;
EOF
```

**Step 2** — Execute it:
```bash
docker exec flink-ecommerce-flink-jobmanager-1 bash -c "timeout 90 ./bin/sql-client.sh -f /tmp/myquery.sql"
```

The `timeout 90` runs the streaming query for 90 seconds then exits. For windowed queries (e.g. 1-minute tumbling window), results only appear when each window closes, so allow at least 60–90 seconds.

### Interactive SQL CLI (optional)
To get a live `Flink SQL>` prompt:
```bash
docker exec -it flink-ecommerce-flink-jobmanager-1 ./bin/sql-client.sh
```
Then paste statements from `sql/create_tables.sql` first, followed by any query.

---

## Flink SQL Table Definitions

All three Kafka topics are registered as Flink SQL tables using the Kafka connector. The `WATERMARK` definition tells Flink how to handle late-arriving events — allowing up to 5 seconds of latency before closing a window.

```sql
CREATE TABLE orders (
    order_id      STRING,
    user_id       STRING,
    product_id    STRING,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    amount        DOUBLE,
    status        STRING,
    event_time    TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'ecommerce.orders',
    'properties.bootstrap.servers'   = 'kafka:29092',
    'properties.group.id'            = 'flink-orders',
    'scan.startup.mode'              = 'earliest-offset',
    'format'                         = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
```

Similar definitions exist for `pageviews` and `cart_events` — see `sql/create_tables.sql`.

---

## Streaming Queries & Use Cases

### 1. Revenue per category per minute — Tumbling Window
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    category,
    COUNT(*)                                       AS num_orders,
    ROUND(SUM(amount), 2)                          AS total_revenue
FROM orders
WHERE status = 'placed'
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), category;
```
**Window type:** Tumbling — fixed 1-minute non-overlapping buckets. Emits one row per category per minute when the window closes.

**Sample output:**
```
| 2026-06-18 22:12:00 | Electronics |  9 | 12589.83 |
| 2026-06-18 22:12:00 | Sports      |  7 |   569.90 |
| 2026-06-18 22:12:00 | Home        |  5 |  1079.91 |
| 2026-06-18 22:12:00 | Books       |  4 |   334.92 |
```

---

### 2. Top 5 products last 5 minutes — Sliding Window
```sql
SELECT
    product_name,
    category,
    COUNT(*) AS order_count
FROM orders
WHERE status = 'placed'
GROUP BY
    HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE),
    product_name, category
ORDER BY order_count DESC
LIMIT 5;
```
**Window type:** Sliding (HOP) — 5-minute window that slides every 1 minute. More granular than tumbling; windows overlap.

---

### 3. Cart add vs remove ratio — Tumbling Window
```sql
SELECT
    product_name,
    SUM(CASE WHEN action = 'add'    THEN quantity ELSE 0 END) AS added,
    SUM(CASE WHEN action = 'remove' THEN quantity ELSE 0 END) AS removed
FROM cart_events
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), product_name;
```

---

### 4. Page views per category per minute
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    category,
    COUNT(*) AS view_count
FROM pageviews
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), category;
```

---

### 5. Cart add → order conversion — Stream-Stream Join
```sql
SELECT c.user_id, c.product_id, o.amount
FROM cart_events c
JOIN orders o
  ON c.user_id    = o.user_id
 AND c.product_id = o.product_id
 AND o.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '10' MINUTE
WHERE c.action = 'add';
```
**Concept:** Joins two live Kafka streams within a time boundary. Identifies users who added to cart and then placed an order within 10 minutes.

---

### 6. Cart abandonment — Anti Join
```sql
SELECT DISTINCT pv.user_id, pv.product_id, pv.category
FROM pageviews pv
WHERE pv.page_type = 'product'
  AND NOT EXISTS (
      SELECT 1 FROM orders o
      WHERE o.user_id    = pv.user_id
        AND o.product_id = pv.product_id
        AND o.event_time BETWEEN pv.event_time AND pv.event_time + INTERVAL '10' MINUTE
  );
```
**Concept:** Detects users who viewed a product page but did not purchase within 10 minutes — a cart abandonment signal.

---

### 7. Lifetime order count per user — Stateful (no window)
```sql
SELECT
    user_id,
    COUNT(*)              AS lifetime_orders,
    ROUND(SUM(amount), 2) AS lifetime_spend
FROM orders
WHERE status = 'placed'
GROUP BY user_id;
```
**Concept:** No window means Flink maintains a continuously updated running total per user in its **state store**. The count grows as long as the job runs.

---

### 8. Unique buyers per hour
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start,
    COUNT(DISTINCT user_id)                      AS unique_buyers,
    ROUND(SUM(amount), 2)                        AS total_revenue
FROM orders
WHERE status = 'placed'
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR);
```

---

### 9. Running revenue within hour updated every minute — Cumulative Window
```sql
SELECT
    CUMULATE_START(event_time, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) AS window_start,
    CUMULATE_END(event_time, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)   AS window_end,
    category,
    ROUND(SUM(amount), 2) AS cumulative_revenue
FROM orders
WHERE status = 'placed'
GROUP BY
    CUMULATE(event_time, INTERVAL '1' MINUTE, INTERVAL '1' HOUR),
    category;
```
**Window type:** Cumulative — grows from the start of the hour, emits every minute, resets at the top of the next hour.

---

## Key Flink Concepts

### Windowing
| Type | Description | Use case |
|---|---|---|
| Tumbling | Fixed, non-overlapping buckets | Revenue per minute |
| Sliding (HOP) | Overlapping buckets | Rolling top-N |
| Session | Closes on inactivity gap | User session grouping |
| Cumulative | Grows until max, then resets | Intra-hour running total |

### State
Flink maintains per-key state (keyed by `GROUP BY` column) in memory or RocksDB. State enables running totals across unbounded streams without windowing. In production, state is checkpointed to S3/GCS for fault tolerance.

### Watermarks
Watermarks tell Flink how far event time has progressed. `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` means Flink waits 5 seconds for late events before closing a window.

### Changelog (`op` column)
In TABLEAU mode, Flink prefixes each output row with an operation type:
- `+I` — Insert (new row)
- `-U` — Before-update (retract old value)
- `+U` — After-update (emit new value)
- `-D` — Delete

For append-only windowed queries, you will only see `+I`.

### Catalog
Flink's default catalog is in-memory. Table definitions are lost when the SQL session ends. For persistent definitions across sessions, use a persistent catalog (Hive Metastore or JDBC catalog).

---

## Production Considerations

| Concern | Local Setup | Production |
|---|---|---|
| Cluster | Docker Compose | Kubernetes + Flink Operator |
| State backend | In-memory | RocksDB + S3 checkpoints |
| Kafka | Single broker | MSK / Confluent Cloud (multi-broker) |
| Flink job packaging | SQL file | Fat JAR (Maven/Gradle) |
| Catalog | In-memory | Hive Metastore / JDBC catalog |
| Monitoring | Flink UI | Prometheus + Grafana |
| Fault tolerance | None | Checkpoints every 30s to S3 |
| Sink | Print to screen | Kafka / PostgreSQL / BigQuery |

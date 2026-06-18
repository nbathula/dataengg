# Flink Ecommerce — Use Case & Requirements

## Business Context

An ecommerce platform generates a continuous stream of user activity events — page views, cart interactions, and orders — at high velocity. Business and operations teams need real-time visibility into what is happening on the platform: which products are trending, how much revenue is being generated right now, which users are abandoning carts, and whether conversion rates are healthy.

Traditional batch pipelines (e.g. nightly ETL into a data warehouse) introduce hours of latency — by the time an insight is available, the opportunity to act on it has passed. A streaming architecture built on Apache Flink processes events as they arrive, making insights available within seconds of the event occurring.

This project implements a local development version of that streaming platform to demonstrate real-time ecommerce analytics using Apache Flink and Apache Kafka.

---

## Use Case Summary

> **Real-time ecommerce analytics pipeline** — process a continuous stream of ecommerce events (orders, cart interactions, page views) and compute business metrics in real time using Apache Flink SQL.

---

## Actors

| Actor | Description |
|---|---|
| Ecommerce Platform | Source system generating user activity events |
| Apache Kafka | Event broker — decouples producers from consumers |
| Apache Flink | Stream processing engine — computes metrics from the event stream |
| Business Analyst | Consumes query results to make operational decisions |
| Data Engineer | Develops and maintains Flink SQL jobs |

---

## Functional Requirements

### FR-01 — Ingest real-time ecommerce events
The system must consume three types of events from Kafka topics in real time:
- **Orders** — a user completing a purchase
- **Cart events** — a user adding or removing a product from their cart
- **Page views** — a user viewing a product, category, or search page

### FR-02 — Revenue tracking per category per minute
The system must compute total revenue and order count grouped by product category for every 1-minute tumbling window.

**Business value:** Operations teams can monitor revenue in real time and detect drops or spikes immediately.

**Query type:** Tumbling window aggregation (1 minute)

**Output fields:** `window_start`, `category`, `num_orders`, `total_revenue`

---

### FR-03 — Top products by order volume (rolling)
The system must identify the top 5 products ranked by order count over a rolling 5-minute window, updated every minute.

**Business value:** Merchandising teams can identify trending products in real time and adjust promotions or inventory accordingly.

**Query type:** Sliding (HOP) window — 5-minute window, 1-minute slide

**Output fields:** `product_name`, `category`, `order_count`

---

### FR-04 — Cart sentiment — add vs remove ratio
The system must compute the quantity of items added and removed per product over 5-minute windows.

**Business value:** A high remove rate on a product signals pricing issues, stock concerns, or a poor product page experience.

**Query type:** Tumbling window aggregation (5 minutes) with conditional aggregation

**Output fields:** `product_name`, `added`, `removed`

---

### FR-05 — Page view volume per category
The system must count page views grouped by product category per minute.

**Business value:** Identifies which categories are attracting traffic, enabling comparison against conversion (FR-02) to spot high-traffic low-conversion categories.

**Query type:** Tumbling window aggregation (1 minute)

**Output fields:** `window_start`, `category`, `view_count`

---

### FR-06 — Cart-to-order conversion detection
The system must detect users who added a product to their cart and then placed an order for the same product within 10 minutes.

**Business value:** Measures the cart-to-purchase conversion rate at the user and product level in real time.

**Query type:** Stream-to-stream join with time boundary (10-minute interval join)

**Output fields:** `user_id`, `product_id`, `amount`

---

### FR-07 — Cart abandonment detection
The system must identify users who viewed a product page but did not place an order for that product within 10 minutes of the view.

**Business value:** Enables real-time cart abandonment alerts — downstream systems (email, push notification) can act on this signal immediately to recover the sale.

**Query type:** Anti join (NOT EXISTS) between page views and orders

**Output fields:** `user_id`, `product_id`, `category`

---

### FR-08 — Lifetime order metrics per user
The system must maintain a continuously updated running total of order count and total spend per user across the full history of the stream.

**Business value:** Enables real-time customer segmentation — identify high-value customers as they cross spend thresholds without waiting for a nightly batch run.

**Query type:** Stateful aggregation (no window — unbounded running total, keyed by `user_id`)

**Output fields:** `user_id`, `lifetime_orders`, `lifetime_spend`

---

### FR-09 — Unique buyers per hour
The system must count distinct users who placed at least one order within each 1-hour tumbling window.

**Business value:** Tracks the size of the active buying audience hour by hour.

**Query type:** Tumbling window aggregation (1 hour) with COUNT DISTINCT

**Output fields:** `window_start`, `unique_buyers`, `total_revenue`

---

### FR-10 — Intra-hour cumulative revenue by category
The system must compute the running revenue total per category within the current hour, updated every minute.

**Business value:** Allows teams to track progress toward hourly revenue targets in real time, with a reset at the top of each hour.

**Query type:** Cumulative window — grows every minute, resets every hour

**Output fields:** `window_start`, `window_end`, `category`, `cumulative_revenue`

---

## Non-Functional Requirements

### NFR-01 — Latency
Event processing latency must be under 10 seconds from event production to query result emission under normal load.

### NFR-02 — Fault tolerance
The system must be able to recover from a Flink TaskManager failure without losing aggregated state. In production this is achieved via checkpoints to S3/GCS every 30 seconds.

### NFR-03 — Scalability
The system must scale horizontally by adding Flink TaskManagers and Kafka partitions. Flink's `keyBy` partitioning ensures state is evenly distributed across TaskManager slots.

### NFR-04 — Late event handling
Events arriving up to 5 seconds late (relative to event time) must still be included in the correct window. Events arriving later than 5 seconds may be dropped.

### NFR-05 — Replayability
In the event of a job restart, Flink must be able to replay events from Kafka from the last committed offset so no events are lost.

---

## Data Requirements

### Event volume (simulated)
| Topic | Approximate rate |
|---|---|
| ecommerce.orders | ~3 events/second |
| ecommerce.cart | ~6 events/second |
| ecommerce.pageviews | ~8 events/second |

### Reference data
| Entity | Values |
|---|---|
| Users | 200 simulated users (`user_0001` to `user_0200`) |
| Products | 10 products across 4 categories |
| Categories | Electronics, Sports, Home, Books |
| Order statuses | `placed` (75%), `cancelled` (25%) |
| Cart actions | `add` (80%), `remove` (20%) |

### Time semantics
All events carry an `event_time` field in ISO-8601 format (UTC). Flink uses **event time** (not processing time) for all window computations, with a 5-second watermark delay to handle late arrivals.

---

## Use Case to Query Mapping

| # | Business Question | Flink Feature | Topic |
|---|---|---|---|
| FR-02 | Revenue per category per minute | Tumbling window + SUM | orders |
| FR-03 | Top 5 trending products | Sliding window + COUNT | orders |
| FR-04 | Cart add vs remove ratio | Tumbling window + CASE | cart |
| FR-05 | Page views per category | Tumbling window + COUNT | pageviews |
| FR-06 | Cart-to-order conversion | Stream-stream join | cart + orders |
| FR-07 | Cart abandonment | Anti join (NOT EXISTS) | pageviews + orders |
| FR-08 | Lifetime spend per user | Stateful aggregation | orders |
| FR-09 | Unique buyers per hour | Tumbling window + COUNT DISTINCT | orders |
| FR-10 | Running revenue within hour | Cumulative window + SUM | orders |

---

## Out of Scope (for this local setup)

- Persistent Flink catalog across sessions (requires Hive Metastore or JDBC catalog)
- Writing results to a persistent sink (PostgreSQL, BigQuery, S3)
- Authentication and authorization on Kafka or Flink
- Production-grade deployment on Kubernetes
- Schema Registry for event schema enforcement
- Monitoring and alerting (Prometheus / Grafana)

-- Run these in the Flink SQL CLI to register Kafka topics as tables.
-- Start the CLI: docker exec -it flink-ecommerce-flink-jobmanager-1 ./bin/sql-client.sh

-- ─── Orders ──────────────────────────────────────────────────────────────────
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
    'connector'                     = 'kafka',
    'topic'                         = 'ecommerce.orders',
    'properties.bootstrap.servers'  = 'kafka:29092',
    'properties.group.id'           = 'flink-orders',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ─── Page Views ───────────────────────────────────────────────────────────────
CREATE TABLE pageviews (
    view_id    STRING,
    user_id    STRING,
    product_id STRING,
    category   STRING,
    page_type  STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'ecommerce.pageviews',
    'properties.bootstrap.servers'  = 'kafka:29092',
    'properties.group.id'           = 'flink-pageviews',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ─── Cart Events ──────────────────────────────────────────────────────────────
CREATE TABLE cart_events (
    event_id     STRING,
    user_id      STRING,
    product_id   STRING,
    product_name STRING,
    action       STRING,
    quantity     INT,
    event_time   TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'ecommerce.cart',
    'properties.bootstrap.servers'  = 'kafka:29092',
    'properties.group.id'           = 'flink-cart',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

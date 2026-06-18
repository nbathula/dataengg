-- ─── Example Flink SQL queries ────────────────────────────────────────────────
-- Run these one at a time in the SQL CLI after creating tables (create_tables.sql)

-- 1. Live order stream
SELECT order_id, user_id, product_name, amount, status, event_time
FROM orders;

-- 2. Revenue per category per minute (tumbling window)
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE)  AS window_start,
    category,
    COUNT(*)                                        AS num_orders,
    ROUND(SUM(amount), 2)                           AS total_revenue
FROM orders
WHERE status = 'placed'
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), category;

-- 3. Top products by number of orders (last 5 minutes, sliding window)
SELECT
    product_name,
    category,
    COUNT(*) AS order_count
FROM orders
WHERE status = 'placed'
GROUP BY
    HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE),
    product_name,
    category
ORDER BY order_count DESC
LIMIT 5;

-- 4. Cart add vs remove ratio by product (last 5 minutes)
SELECT
    product_name,
    SUM(CASE WHEN action = 'add'    THEN quantity ELSE 0 END) AS added,
    SUM(CASE WHEN action = 'remove' THEN quantity ELSE 0 END) AS removed
FROM cart_events
GROUP BY
    TUMBLE(event_time, INTERVAL '5' MINUTE),
    product_name;

-- 5. Page views per category per minute
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    category,
    COUNT(*) AS view_count
FROM pageviews
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), category;

-- 6. Users who viewed a product but haven't ordered in last 10 min (intent gap)
SELECT DISTINCT pv.user_id, pv.product_id, pv.category
FROM pageviews pv
WHERE pv.page_type = 'product'
  AND NOT EXISTS (
      SELECT 1 FROM orders o
      WHERE o.user_id = pv.user_id
        AND o.product_id = pv.product_id
        AND o.event_time BETWEEN pv.event_time AND pv.event_time + INTERVAL '10' MINUTE
  );

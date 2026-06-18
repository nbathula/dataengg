import json
import os
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

PRODUCTS = [
    {"id": "p001", "name": "Laptop Pro 15",       "category": "Electronics",  "price": 1299.99},
    {"id": "p002", "name": "Wireless Headphones",  "category": "Electronics",  "price": 79.99},
    {"id": "p003", "name": "Mechanical Keyboard",  "category": "Electronics",  "price": 129.99},
    {"id": "p004", "name": "Running Shoes",         "category": "Sports",       "price": 89.99},
    {"id": "p005", "name": "Yoga Mat",              "category": "Sports",       "price": 34.99},
    {"id": "p006", "name": "Coffee Maker",          "category": "Home",         "price": 59.99},
    {"id": "p007", "name": "Air Purifier",          "category": "Home",         "price": 149.99},
    {"id": "p008", "name": "Python Cookbook",       "category": "Books",        "price": 39.99},
    {"id": "p009", "name": "Data Engineering Book", "category": "Books",        "price": 44.99},
    {"id": "p010", "name": "Smartwatch Series X",   "category": "Electronics",  "price": 249.99},
]

USERS = [f"user_{i:04d}" for i in range(1, 201)]


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def get_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return p
        except NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(3)


def generate_order():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 3)
    return {
        "order_id": fake.uuid4(),
        "user_id": random.choice(USERS),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": qty,
        "amount": round(product["price"] * qty, 2),
        "status": random.choices(["placed", "placed", "placed", "cancelled"], weights=[3, 3, 3, 1])[0],
        "event_time": now_iso(),
    }


def generate_pageview():
    product = random.choice(PRODUCTS)
    return {
        "view_id": fake.uuid4(),
        "user_id": random.choice(USERS),
        "product_id": product["id"],
        "category": product["category"],
        "page_type": random.choice(["product", "search", "home", "category"]),
        "event_time": now_iso(),
    }


def generate_cart_event():
    product = random.choice(PRODUCTS)
    return {
        "event_id": fake.uuid4(),
        "user_id": random.choice(USERS),
        "product_id": product["id"],
        "product_name": product["name"],
        "action": random.choices(["add", "remove"], weights=[4, 1])[0],
        "quantity": random.randint(1, 2),
        "event_time": now_iso(),
    }


producer = get_producer()
print("Generating ecommerce events... (Ctrl+C to stop)")

counters = {"orders": 0, "pageviews": 0, "cart": 0}

while True:
    r = random.random()

    if r < 0.20:
        event = generate_order()
        producer.send("ecommerce.orders", value=event)
        counters["orders"] += 1
        print(f"[ORDER]    {event['user_id']} → {event['product_name']} x{event['quantity']}  ${event['amount']}")

    elif r < 0.55:
        event = generate_cart_event()
        producer.send("ecommerce.cart", value=event)
        counters["cart"] += 1
        print(f"[CART]     {event['user_id']} {event['action']:6s} {event['product_name']}")

    else:
        event = generate_pageview()
        producer.send("ecommerce.pageviews", value=event)
        counters["pageviews"] += 1

    total = sum(counters.values())
    if total % 50 == 0:
        print(f"  >> totals — orders:{counters['orders']}  cart:{counters['cart']}  views:{counters['pageviews']}")

    time.sleep(random.uniform(0.05, 0.4))

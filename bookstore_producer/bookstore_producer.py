import os
import sys
import mysql.connector
import json
import random
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Kafka configuration
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

producer = Producer({
    "bootstrap.servers": bootstrap_servers
})

# Fail fast if Kafka is unreachable
max_retries = 30
retry_delay = 2

for attempt in range(max_retries):
    try:
        producer.list_topics(timeout=5)
        print("Connected to Kafka successfully.")
        break
    except Exception as e:
        print(f"Kafka not ready (attempt {attempt+1}/{max_retries}): {e}")
        time.sleep(retry_delay)
else:
    print("ERROR: Kafka did not become ready in time.")
    sys.exit(1)

TOPIC = "bookstore_events"

#MariaDB Connection

db = mysql.connector.connect(
    host=os.getenv("MARIADB_HOST"),
    port=int(os.getenv("MARIADB_PORT", "3306")),
    user=os.getenv("MARIADB_USER"),
    password=os.getenv("MARIADB_PASSWORD"),
    database=os.getenv("MARIADB_DATABASE")
)
cursor = db.cursor(dictionary=True)

#Population decay logic

def apply_popularity_decay():
    """
    Apply daily decay to real_world_popularity.
    This runs once per simulated day.
    """
    cursor.execute("""
        UPDATE books
        SET real_world_popularity = GREATEST(real_world_popularity * 0.97, 0)
    """)
    db.commit()
    print("Applied daily real_world_popularity decay.")

#MariaDB load inventory

def load_catalog():
    cursor.execute("""
        SELECT isbn, title, author, genre, price,
               base_popularity, real_world_popularity, sales_popularity
        FROM books
    """)
    rows = cursor.fetchall()

    catalog = {}
    for row in rows:
        genre = row["genre"]
        if genre not in catalog:
            catalog[genre] = []

        total_pop = (
            row["base_popularity"] +
            row["real_world_popularity"] +
            row["sales_popularity"]
        )

        catalog[genre].append({
            "isbn": row["isbn"],
            "title": row["title"],
            "author": row["author"],
            "genre": row["genre"],
            "price": float(row["price"]),
            "popularity": max(total_pop, 1)
        })

    return catalog

#Apply decay first and load catalog

apply_popularity_decay()
CATALOG = load_catalog()
print(f"Loaded {sum(len(v) for v in CATALOG.values())} books from catalog.")

#Sped-up simulation time logic

SIM_START_DATE = datetime(2026, 3, 1)
REAL_DURATION = 30 * 60
STORE_OPEN_HOUR = 8
STORE_CLOSE_HOUR = 17

STATE_FILE = os.getenv("STATE_FILE", "/app/state/day_index.txt")

#Simulation day index

def load_day_index():
    try:
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    except:
        return 0

def save_day_index(idx):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(str(idx))

#Store locations

STORES = {
    "glendale": {
        "name": "Glendale",
        "weight": 1.3,
        "genres": ["Children", "Fiction", "Non-Fiction"]
    },
    "brookfield": {
        "name": "Brookfield",
        "weight": 1.0,
        "genres": ["Non-Fiction", "Photography", "Travel", "Fiction"]
    },
    "madison": {
        "name": "Madison",
        "weight": 1.6,
        "genres": ["Textbook", "Sci-Fi", "Fantasy", "Non-Fiction"]
    }
}

EVENT_CADENCE = {
    "browse": (5, 15),
    "add_to_cart": (10, 25),
    "purchase": (20, 60),
    "return": (8 * 60, 20 * 60),
    "inventory_adjustment": (15 * 60, 45 * 60),
    "price_update": (2 * 60 * 60, 6 * 60 * 60)
}

#Simulation clock

day_index = load_day_index()
print(f"Simulating day index: {day_index}")

SIM_DAY = SIM_START_DATE + timedelta(days=day_index)
SIM_DAY_START = SIM_DAY.replace(hour=STORE_OPEN_HOUR, minute=0, second=0)
SIM_DAY_END   = SIM_DAY.replace(hour=STORE_CLOSE_HOUR, minute=0, second=0)

real_start = time.time()

def simulated_timestamp():
    real_elapsed = time.time() - real_start
    fraction = min(real_elapsed / REAL_DURATION, 1.0)
    sim_time = SIM_DAY_START + fraction * (SIM_DAY_END - SIM_DAY_START)
    return sim_time.isoformat()

def store_is_open():
    now = simulated_timestamp()
    sim_dt = datetime.fromisoformat(now)
    return STORE_OPEN_HOUR <= sim_dt.hour < STORE_CLOSE_HOUR

#Pick real books from inventory

def pick_book(genre):
    books = CATALOG.get(genre)
    if not books:
        return None

    weights = [b["popularity"] for b in books]
    book = random.choices(books, weights=weights, k=1)[0]

    return {
        "isbn": book["isbn"],
        "title": book["title"],
        "genre": book["genre"],
        "price": book["price"],
        "quantity": random.choice([1, 1, 1, 2])
    }

#Simulated business event logic

def emit_event(store_id, store, event_type):
    genre = random.choice(store["genres"])

    if event_type in ("browse", "add_to_cart", "return"):
        book = pick_book(genre)
        if not book:
            return
        items = [book]

    elif event_type == "purchase":
        items = []
        for _ in range(random.randint(1, 4)):
            g = random.choice(store["genres"])
            book = pick_book(g)
            if book:
                items.append(book)

    elif event_type == "inventory_adjustment":
        book = pick_book(genre)
        if not book:
            return
        book["quantity"] = random.randint(-5, 10)
        items = [book]

    elif event_type == "price_update":
        book = pick_book(genre)
        if not book:
            return
        book["price"] = round(book["price"] * random.uniform(0.8, 1.2), 2)
        items = [book]

    else:
        items = []

    total_amount = round(sum(i["price"] * max(i["quantity"], 0) for i in items), 2)

    event = {
        "event_type": event_type,
        "timestamp": simulated_timestamp(),
        "store_id": store_id,
        "store_name": store["name"],
        "customer_id": fake.uuid4() if event_type not in ("inventory_adjustment", "price_update") else None,
        "items": items,
        "total_amount": total_amount,
        "payment_method": random.choice(["credit_card", "cash", "mobile_pay"]) if event_type == "purchase" else None,
        "loyalty_member": random.choice([True, False]) if event_type in ("purchase", "return") else None
    }

    producer.produce(TOPIC, json.dumps(event).encode("utf-8"))
    producer.poll(0)

#Main loop

print(f"Starting bookstore simulation for {SIM_DAY.date()} (8 AM → 5 PM compressed into 30 minutes)")

# Missing function in your original script — adding it back
def schedule_next(event_type, store_id, weight):
    low, high = EVENT_CADENCE[event_type]
    return time.time() + random.uniform(low / weight, high / weight)

next_emit = {
    store_id: {
        event_type: schedule_next(event_type, store_id, store["weight"])
        for event_type in EVENT_CADENCE
    }
    for store_id, store in STORES.items()
}

try:
    while True:
        now = time.time()

        for store_id, store in STORES.items():
            for event_type, due in next_emit[store_id].items():
                if now >= due:

                    if event_type in ("browse", "add_to_cart", "purchase", "return") and not store_is_open():
                        next_emit[store_id][event_type] = schedule_next(event_type, store_id, store["weight"])
                        continue

                    emit_event(store_id, store, event_type)
                    next_emit[store_id][event_type] = schedule_next(event_type, store_id, store["weight"])

        producer.flush(0)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("Producer stopping — incrementing day index...")
    save_day_index(day_index + 1)
    producer.flush()
    sys.exit(0)
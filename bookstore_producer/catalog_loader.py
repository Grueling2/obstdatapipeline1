import os
import time
import random
import requests
import mysql.connector
from datetime import date, timedelta

NYT_API_KEY = os.getenv("NYT_API_KEY")

#Simulation day index

DAY_INDEX_PATH = "/app/state/day_index.txt"
BASE_DATE = date(2026, 3, 1)

with open(DAY_INDEX_PATH, "r") as f:
    day_index = int(f.read().strip())

SIM_DAY = BASE_DATE + timedelta(days=day_index)
SIM_DAY_STR = SIM_DAY.strftime("%Y-%m-%d")

print(f"Catalog loader using simulated day: {SIM_DAY_STR}")

#NYT lists from Dev API key

NYT_LISTS = [
    "hardcover-fiction",
    "trade-fiction-paperback",
    "hardcover-nonfiction",
    "paperback-nonfiction",
    "business-books",
    "advice-how-to-and-miscellaneous",
    "childrens-middle-grade-hardcover",
    "picture-books",
    "young-adult-hardcover",
    "science-fiction",
]

def normalize_title(title: str) -> str:
    """Convert ALL CAPS titles to Title Case, leave normal titles alone."""
    if title.isupper():
        return title.title()
    return title

def assign_price(genre: str) -> float:
    """NYT API does not provide price — assign realistic defaults."""
    ranges = {
        "Fiction": (24.99, 32.99),
        "Non-Fiction": (24.99, 34.99),
        "Children": (6.99, 12.99),
        "Sci-Fi": (14.99, 22.99),
        "Fantasy": (18.99, 29.99),
        "Photography": (29.99, 49.99),
        "Travel": (24.99, 39.99),
    }
    low, high = ranges.get(genre, (14.99, 29.99))
    return round(random.uniform(low, high), 2)

def map_genre(list_name):
    list_name = list_name.lower()

    if "science-fiction" in list_name:
        return "Sci-Fi"
    if "nonfiction" in list_name or "business" in list_name or "advice" in list_name:
        return "Non-Fiction"
    if "fiction" in list_name:
        return "Fiction"
    if "children" in list_name or "picture" in list_name:
        return "Children"

    return "Fiction"

#Popularity adjustment
def compute_real_world_popularity(rank, weeks_on_list):
    """
    Normalize real-world popularity to a 0–100 scale.
    weeks_on_list grows slowly and caps at 100.
    """
    return min(weeks_on_list / 4, 100.0)

#Connect to MariaDB
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MARIADB_HOST"),
        user=os.getenv("MARIADB_USER"),
        password=os.getenv("MARIADB_PASSWORD"),
        database=os.getenv("MARIADB_DATABASE"),
        port=int(os.getenv("MARIADB_PORT", 3306))
    )

def fetch_nyt_list(list_name):
    url = f"https://api.nytimes.com/svc/books/v3/lists/current/{list_name}.json?api-key={NYT_API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed NYT fetch for {list_name}: {response.text}")
        return None

    return response.json()

#Main catalog_loader loop

def load_books():
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO books (
            isbn, title, author, genre, price, is_classic,
            base_popularity, real_world_popularity, sales_popularity, last_updated
        )
        VALUES (%s, %s, %s, %s, %s, 0, %s, %s, 0.0, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            author = VALUES(author),
            genre = VALUES(genre),
            price = VALUES(price),
            base_popularity = VALUES(base_popularity),
            real_world_popularity = VALUES(real_world_popularity),
            last_updated = VALUES(last_updated);
    """

    for list_name in NYT_LISTS:
        print(f"Fetching NYT list: {list_name}")
        data = fetch_nyt_list(list_name)

        # Avoid NYT rate limits
        time.sleep(1)

        if not data:
            continue

        results = data.get("results", {})
        books = results.get("books", [])
        genre = map_genre(list_name)

        print(f" → Retrieved {len(books)} books from {list_name}")

        for b in books:
            isbn = b.get("primary_isbn13")
            if not isbn:
                continue

            raw_title = b.get("title", "").strip()
            title = normalize_title(raw_title)

            author = b.get("author", "").strip()

            price = b.get("price")
            if not price or float(price) == 0.0:
                price = assign_price(genre)
            else:
                price = float(price)

            rank = b.get("rank", 0)
            weeks = b.get("weeks_on_list", 0)

            # HYBRID MODEL VALUES
            real_world_pop = compute_real_world_popularity(rank, weeks)
            base_popularity = max(5.0, 40.0 - rank)

            cursor.execute(
                insert_sql,
                (
                    isbn,
                    title,
                    author,
                    genre,
                    price,
                    base_popularity,
                    real_world_pop,
                    SIM_DAY_STR
                )
            )

            print(f"   Inserted/Updated: {title} ({isbn})")

    conn.commit()
    cursor.close()
    conn.close()

    print("Catalog load complete.")

if __name__ == "__main__":
    load_books()
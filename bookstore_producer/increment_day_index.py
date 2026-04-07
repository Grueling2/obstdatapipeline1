import os

STATE_FILE = "/app/state/day_index.txt"

def load_day_index():
    try:
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    except:
        return 0

def save_day_index(idx):
    with open(STATE_FILE, "w") as f:
        f.write(str(idx))

if __name__ == "__main__":
    current = load_day_index()
    new = current + 1
    print(f"Incrementing day index: {current} → {new}")
    save_day_index(new)

"""Simulates users making transactions and publishes them to the
`transactions` Kafka/Redpanda topic.

Most transactions are "normal" for a given simulated user (amount drawn from
a per-user baseline distribution, familiar merchant categories). Periodically
we deliberately inject one of a few anomaly patterns so the detection
consumer (Phase 2) has real signal to catch:

  - a spike: a transaction far above the user's normal amount
  - a novel category: a category that user has never used before
  - a burst: several transactions from the same user in quick succession

Run with: python generator/generate_transactions.py
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

fake = Faker()

MERCHANT_CATEGORIES = [
    "groceries",
    "electronics",
    "dining",
    "travel",
    "utilities",
    "entertainment",
    "clothing",
    "healthcare",
]

# A fixed pool of simulated users, each with their own baseline spending
# profile, so the detection logic has consistent per-user history to learn.
#
# Each user's baseline_mean/baseline_stddev/home_categories is drawn from a
# random.Random SEEDED BY THEIR user_id, not the shared global `random`
# module. This makes those three fields identical every time the generator
# is (re)started -- see BUILD_LOG.md. Without this, restarting the
# generator reshuffled every user's "true" spending profile while their
# accumulated history in Postgres stayed exactly as it was, so the very
# next transaction for nearly every user looked wildly anomalous against a
# baseline that no longer had anything to do with what was actually being
# sent. `location` doesn't affect detection at all, so it's left drawing
# from the shared Faker instance -- fine for it to vary between runs.
NUM_USERS = 25
USERS = []
for _i in range(NUM_USERS):
    _user_id = f"user_{_i:03d}"
    _rng = random.Random(_user_id)  # seeded by user_id -- stable across restarts
    USERS.append(
        {
            "user_id": _user_id,
            "baseline_mean": _rng.uniform(15, 200),
            "baseline_stddev": _rng.uniform(5, 40),
            "home_categories": _rng.sample(MERCHANT_CATEGORIES, k=3),
            "location": fake.city(),
        }
    )

ANOMALY_PROBABILITY = 0.05  # ~1 in 20 transactions is a deliberate anomaly


def make_normal_transaction(user):
    amount = max(1.0, random.gauss(user["baseline_mean"], user["baseline_stddev"]))
    category = random.choice(user["home_categories"])
    return {
        "id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "amount": round(amount, 2),
        "merchant_category": category,
        "location": user["location"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_spike_transaction(user):
    txn = make_normal_transaction(user)
    txn["amount"] = round(user["baseline_mean"] + user["baseline_stddev"] * random.uniform(6, 12), 2)
    return txn


def make_novel_category_transaction(user):
    txn = make_normal_transaction(user)
    other_categories = [c for c in MERCHANT_CATEGORIES if c not in user["home_categories"]]
    txn["merchant_category"] = random.choice(other_categories)
    return txn


def emit(producer, txn):
    producer.send(TOPIC, value=txn, key=txn["user_id"].encode("utf-8"))
    print(f"published: {txn['user_id']} ${txn['amount']:.2f} {txn['merchant_category']}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k,
    )

    print(f"Publishing to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}. Ctrl+C to stop.")
    try:
        while True:
            user = random.choice(USERS)
            roll = random.random()

            if roll < ANOMALY_PROBABILITY / 2:
                emit(producer, make_spike_transaction(user))
            elif roll < ANOMALY_PROBABILITY:
                emit(producer, make_novel_category_transaction(user))
            elif roll < ANOMALY_PROBABILITY + 0.02:
                # burst: fire several rapid transactions for one user
                burst_size = random.randint(5, 8)
                print(f"-- injecting burst of {burst_size} for {user['user_id']} --")
                for _ in range(burst_size):
                    emit(producer, make_normal_transaction(user))
                    time.sleep(0.05)
            else:
                emit(producer, make_normal_transaction(user))

            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print("Stopping generator.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

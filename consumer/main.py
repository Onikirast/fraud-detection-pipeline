"""Phase 2: consumer with detection logic wired in.

Reads transactions off the Kafka/Redpanda topic, writes them to Postgres,
then runs the anomaly rules from detection.py. Flagged transactions get a
row in flagged_events; every transaction's raw record is stored regardless
so nothing is lost even if the rules change later and we want to replay
detection over history.

Run with: python consumer/main.py
"""
import json
import os
import sys
import uuid
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

# Add both this file's own directory (so `detection` resolves) and the
# project root (so `db` resolves) to sys.path explicitly. Don't rely on
# Python's "script dir goes on sys.path automatically" behavior for the
# first one — that only holds when this file is run directly as a script.
# If it's ever imported instead (e.g. `from consumer.main import ...` from
# a test or from the future API/dashboard code), that automatic behavior
# doesn't kick in and the `detection` import breaks. See BUILD_LOG.md.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)
sys.path.append(_THIS_DIR)
sys.path.append(_PROJECT_ROOT)

from db.models import transactions  # noqa: E402
from db.session import get_engine  # noqa: E402
from detection import evaluate_transaction  # noqa: E402

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")


def insert_transaction(engine, txn: dict) -> bool:
    """Idempotent insert — if the same transaction id is delivered twice
    (e.g. after a consumer restart re-reads uncommitted offsets), this is a
    no-op rather than a duplicate row or a crash. This is the kind of
    at-least-once-delivery handling worth mentioning in an interview.

    Returns True if a new row was actually inserted, False if it was a
    duplicate. Detection only runs on real inserts — otherwise a redelivered
    message would flag twice and double-update the user's rolling profile.
    """
    stmt = insert(transactions).values(
        id=uuid.UUID(txn["id"]),
        user_id=txn["user_id"],
        amount=txn["amount"],
        merchant_category=txn["merchant_category"],
        location=txn["location"],
        timestamp=datetime.fromisoformat(txn["timestamp"]),
        created_at=datetime.utcnow(),
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["id"]).returning(transactions.c.id)
    with engine.begin() as conn:
        result = conn.execute(stmt)
        return result.first() is not None


def insert_flagged_event(engine, txn: dict, verdict: dict):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO flagged_events (id, transaction_id, reason, rule_types, score, flagged_at)
                VALUES (:id, :transaction_id, :reason, :rule_types, :score, now())
                """
            ),
            {
                "id": uuid.uuid4(),
                "transaction_id": uuid.UUID(txn["id"]),
                "reason": "; ".join(verdict["reasons"]),
                "rule_types": verdict["triggered_rules"],
                "score": verdict["score"],
            },
        )


def main():
    engine = get_engine()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="fraud-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    print(f"Consuming from topic '{TOPIC}' on {BOOTSTRAP_SERVERS}. Ctrl+C to stop.")
    try:
        for message in consumer:
            txn = message.value
            is_new = insert_transaction(engine, txn)
            if not is_new:
                print(f"skipped duplicate: {txn['id']}")
                continue

            verdict = evaluate_transaction(engine, txn)
            if verdict["flagged"]:
                insert_flagged_event(engine, txn, verdict)
                print(
                    f"FLAGGED ({verdict['score']:.1f}): {txn['user_id']} "
                    f"${txn['amount']:.2f} {txn['merchant_category']} -- {'; '.join(verdict['reasons'])}"
                )
            else:
                print(f"stored: {txn['user_id']} ${txn['amount']:.2f} {txn['merchant_category']}")
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

"""Phase 4: on-demand attack-scenario injector for live demos / interviews.

The generator only injects anomalies ~5% of the time, at random, for a
randomly chosen user -- great for realistic background noise, useless for
"let me show you the detector catching something" five minutes into an
interview. This script publishes ONE deliberately crafted scenario for a
real, already-active user straight through the same Kafka topic the
generator uses -- same queue, same consumer, same detection code, same
dashboard. Nothing about the pipeline is bypassed; this is not a backdoor
into the database.

It reads the target user's ACTUAL current profile (mean_amount, m2_amount,
common_categories) from `user_profiles` and reuses the detector's own
`stddev_from_profile` function (consumer/detection.py) to size the spike --
so "anomalous" here is defined by the exact same math the detector will
use to judge it, not a guess that happens to usually work.

Requires the full pipeline already running (docker compose, generator,
consumer, API, dashboard) with at least one user past the cold-start guard
(MIN_TXNS_FOR_ZSCORE transactions) -- run --list to check who's eligible.

Usage:
    python demo/inject_scenario.py --list
    python demo/inject_scenario.py --scenario spike
    python demo/inject_scenario.py --scenario novel_category --user user_003
    python demo/inject_scenario.py --scenario burst
    python demo/inject_scenario.py --scenario combo

Note on `novel_category`: per consumer/detection.py (see BUILD_LOG.md,
Phase 2), category novelty alone is deliberately weighted BELOW the flag
threshold -- it's a severity booster for a transaction another rule
already flagged, not an independent signal. So this scenario is expected
to NOT flag by itself; the script says so rather than treating it as a
failure. Use `combo` (a spike into a novel category) to see the stacking
behavior it's actually for.
"""
import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from sqlalchemy import text

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)
sys.path.append(_THIS_DIR)
sys.path.append(_PROJECT_ROOT)

from db.session import get_engine  # noqa: E402
from consumer.detection import (  # noqa: E402
    MIN_TXNS_FOR_ZSCORE,
    VELOCITY_MAX_TXNS,
    stddev_from_profile,
)

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

# Must match generator/generate_transactions.py's list. Duplicated rather
# than imported so this script doesn't trigger that module's random
# 25-user pool as an import side effect.
MERCHANT_CATEGORIES = [
    "groceries", "electronics", "dining", "travel",
    "utilities", "entertainment", "clothing", "healthcare",
]


def eligible_users(engine):
    """Users with enough history to clear the cold-start guard, richest
    history first -- the most reliable profile to build a scenario from."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT user_id, txn_count, mean_amount, m2_amount, common_categories
                FROM user_profiles
                WHERE txn_count >= :min_txns
                ORDER BY txn_count DESC
                """
            ),
            {"min_txns": MIN_TXNS_FOR_ZSCORE},
        ).mappings().all()
    return [dict(r) for r in rows]


def pick_user(engine, requested_user_id):
    users = eligible_users(engine)
    if not users:
        sys.exit(
            f"No user has {MIN_TXNS_FOR_ZSCORE}+ transactions yet -- let the "
            "generator run a bit longer, then try again."
        )
    if requested_user_id:
        match = next((u for u in users if u["user_id"] == requested_user_id), None)
        if not match:
            sys.exit(
                f"'{requested_user_id}' isn't in user_profiles with "
                f"{MIN_TXNS_FOR_ZSCORE}+ transactions yet. Run with --list "
                "to see who's eligible."
            )
        return match
    return users[0]


def most_common_category(profile):
    categories = profile["common_categories"] or {}
    if not categories:
        return MERCHANT_CATEGORIES[0]
    return max(categories, key=categories.get)


def novel_category_for(profile):
    """A category this user has never (or not recently) bought in --
    matches rule_category_novelty's own definition of "novel"."""
    known = {c for c, n in (profile["common_categories"] or {}).items() if n > 0}
    candidates = [c for c in MERCHANT_CATEGORIES if c not in known]
    if candidates:
        return candidates[0]
    # Extremely unlikely -- would mean this user has bought in every
    # category. Fall back to their least-purchased one.
    categories = profile["common_categories"] or {}
    return min(categories, key=categories.get) if categories else MERCHANT_CATEGORIES[0]


def make_txn(user_id, amount, category):
    return {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(amount, 2),
        "merchant_category": category,
        "location": "Demo City",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def build_scenario(scenario, profile):
    """Returns a list of transaction dicts for the given scenario, each
    isolated to trigger exactly the rule it's named for: `spike` and
    `novel_category` deliberately keep the *other* attribute normal (a
    familiar category for spike; a typical amount for novel_category) so
    the demo cleanly shows one rule firing, not several at once."""
    user_id = profile["user_id"]
    familiar_category = most_common_category(profile)

    if scenario == "spike":
        stddev = stddev_from_profile(profile) or max(profile["mean_amount"] * 0.1, 1.0)
        amount = profile["mean_amount"] + stddev * random.uniform(6, 12)
        return [make_txn(user_id, amount, familiar_category)]

    if scenario == "novel_category":
        return [make_txn(user_id, profile["mean_amount"], novel_category_for(profile))]

    if scenario == "burst":
        count = VELOCITY_MAX_TXNS + 2  # comfortably past the limit
        return [make_txn(user_id, profile["mean_amount"], familiar_category) for _ in range(count)]

    if scenario == "combo":
        # A spike AND a novel category on the same transaction -- shows
        # category_novelty's real job: it can't flag alone, but it raises
        # the score of a transaction another rule already flagged.
        stddev = stddev_from_profile(profile) or max(profile["mean_amount"] * 0.1, 1.0)
        amount = profile["mean_amount"] + stddev * random.uniform(6, 12)
        return [make_txn(user_id, amount, novel_category_for(profile))]

    raise ValueError(f"unknown scenario: {scenario}")


def emit_and_verify(engine, producer, txns, scenario):
    ids = [t["id"] for t in txns]
    for t in txns:
        producer.send(TOPIC, value=t, key=t["user_id"].encode("utf-8"))
        print(f"  published: {t['user_id']} ${t['amount']:.2f} {t['merchant_category']}")
        if scenario == "burst":
            time.sleep(0.05)  # mirrors the generator's own burst pacing
    producer.flush()

    print("\nWaiting for the consumer to process it (up to 10s)...")
    deadline = time.time() + 10
    processed = False
    while time.time() < deadline:
        with engine.connect() as conn:
            processed_count = conn.execute(
                text("SELECT count(*) FROM transactions WHERE id::text = ANY(:ids)"),
                {"ids": ids},
            ).scalar()
            rows = conn.execute(
                text(
                    """
                    SELECT fe.reason, fe.rule_types, fe.score
                    FROM flagged_events fe
                    JOIN transactions t ON t.id = fe.transaction_id
                    WHERE t.id::text = ANY(:ids)
                    """
                ),
                {"ids": ids},
            ).mappings().all()
        if processed_count == len(ids):
            processed = True
        if rows:
            print(f"\nFlagged ({len(rows)} event(s)) -- check the dashboard too:")
            for r in rows:
                print(f"  score={r['score']}  rules={r['rule_types']}  {r['reason']}")
            return
        if processed:
            break  # fully processed already, no need to keep polling
        time.sleep(0.5)

    if not processed:
        print(
            "\nStill not processed after 10s. Make sure `python consumer/main.py` "
            "is actually running in another terminal."
        )
        return

    if scenario == "novel_category":
        print(
            "\nNot flagged -- and that's expected. category_novelty is weighted "
            "below FLAG_THRESHOLD on purpose: it's a severity booster for a "
            "transaction another rule already flagged, not an independent "
            "trigger (see BUILD_LOG.md, Phase 2). Try `--scenario combo` to see "
            "it stack with a spike and actually cross the threshold."
        )
    else:
        print(
            "\nProcessed but not flagged. Double check the thresholds in .env "
            "haven't drifted from what BUILD_LOG.md documents."
        )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scenario", choices=["spike", "novel_category", "burst", "combo"])
    parser.add_argument("--user", help="user_id to target (default: richest history)")
    parser.add_argument("--list", action="store_true", help="list eligible users and exit")
    args = parser.parse_args()

    engine = get_engine()

    if args.list:
        users = eligible_users(engine)
        if not users:
            print(f"No user has {MIN_TXNS_FOR_ZSCORE}+ transactions yet.")
            return
        print(f"{'user_id':<12} {'txns':>6} {'mean_amount':>12}")
        for u in users:
            print(f"{u['user_id']:<12} {u['txn_count']:>6} {u['mean_amount']:>12.2f}")
        return

    if not args.scenario:
        parser.error("--scenario is required (or use --list)")

    profile = pick_user(engine, args.user)
    print(
        f"Targeting {profile['user_id']} (history: {profile['txn_count']} txns, "
        f"mean ${profile['mean_amount']:.2f}) with scenario '{args.scenario}'"
    )

    txns = build_scenario(args.scenario, profile)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k,
    )
    try:
        emit_and_verify(engine, producer, txns, args.scenario)
    finally:
        producer.close()


if __name__ == "__main__":
    main()

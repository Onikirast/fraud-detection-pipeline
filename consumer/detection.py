"""Phase 2: anomaly detection logic.

Three independent, explainable rules, each contributing a weight to a
combined score. A transaction is flagged if the combined score crosses
FLAG_THRESHOLD.

Design choices worth being able to explain out loud:

  1. Per-user amount statistics (mean/stddev) are maintained incrementally
     with Welford's online algorithm, so we never recompute from full
     history on every transaction — O(1) update instead of O(n).
  2. The z-score rule needs a "cold start" guard: with fewer than
     MIN_TXNS_FOR_ZSCORE observations, a user's stddev estimate is too
     noisy to trust, so we skip the amount check entirely rather than
     produce a misleading score.
  3. A transaction that gets flagged does NOT feed back into that user's
     baseline (mean/stddev/category counts). If it did, a sustained attack
     could gradually shift the "normal" baseline until it no longer looked
     anomalous — the profile would be poisoned by the fraud it's supposed
     to catch. Only transactions that pass all rules update the profile.
  4. RULE_WEIGHTS/FLAG_THRESHOLD: amount_zscore and velocity are each
     weighted at the full flag threshold (1.0), so either one alone is
     sufficient to flag — they're strong, well-understood signals.
     category_novelty is deliberately weighted below the threshold (0.6):
     a first-time purchase in a new category is common and only weak
     evidence of fraud on its own (see BUILD_LOG.md — an earlier version
     of this docstring claimed weak signals could "stack" to a flag, but
     with only one sub-threshold rule that path is actually unreachable;
     a test caught this). So in the current rule set, category_novelty
     never independently decides flagged/not-flagged — its actual job is
     to raise the *severity score* of a transaction that another rule
     already flagged, which the dashboard (Phase 3) uses to help a human
     reviewer triage: a transaction with an amount spike AND a novel
     category (score 1.6) is more worth looking at first than one with
     just the spike (score 1.0). Adding a second genuinely weak rule (e.g.
     unusual time-of-day) is a natural stretch goal if you want stacking
     to actually decide outcomes rather than just rank them.
"""
import os
from datetime import timedelta

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "3.0"))
VELOCITY_WINDOW_MINUTES = int(os.getenv("VELOCITY_WINDOW_MINUTES", "10"))
VELOCITY_MAX_TXNS = int(os.getenv("VELOCITY_MAX_TXNS", "5"))

MIN_TXNS_FOR_ZSCORE = 5  # cold-start guard

RULE_WEIGHTS = {
    "amount_zscore": 1.0,
    "category_novelty": 0.6,
    "velocity": 1.0,
}
FLAG_THRESHOLD = 1.0  # any single strong rule, or weaker rules stacking


def get_or_create_profile(engine, user_id: str) -> dict:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM user_profiles WHERE user_id = :uid"),
            {"uid": user_id},
        ).mappings().first()

        if row:
            return dict(row)

        conn.execute(
            text(
                """
                INSERT INTO user_profiles
                    (user_id, txn_count, mean_amount, m2_amount, common_categories, updated_at)
                VALUES (:uid, 0, 0, 0, '{}'::jsonb, now())
                ON CONFLICT (user_id) DO NOTHING
                """
            ),
            {"uid": user_id},
        )
        row = conn.execute(
            text("SELECT * FROM user_profiles WHERE user_id = :uid"),
            {"uid": user_id},
        ).mappings().first()
        return dict(row)


def stddev_from_profile(profile: dict) -> float:
    """Welford's M2 -> sample standard deviation."""
    n = profile["txn_count"]
    if n < 2:
        return 0.0
    variance = profile["m2_amount"] / (n - 1)
    return variance ** 0.5


def rule_amount_zscore(profile: dict, amount: float):
    n = profile["txn_count"]
    if n < MIN_TXNS_FOR_ZSCORE:
        return False, None  # not enough history yet — don't guess

    stddev = stddev_from_profile(profile)
    if stddev == 0:
        # Degenerate case: every past transaction was exactly the same
        # amount. Any deviation at all is meaningful here.
        triggered = amount != profile["mean_amount"]
        reason = (
            f"amount ${amount:.2f} differs from user's constant history of "
            f"${profile['mean_amount']:.2f}"
            if triggered
            else None
        )
        return triggered, reason

    z = (amount - profile["mean_amount"]) / stddev
    if abs(z) >= ZSCORE_THRESHOLD:
        reason = (
            f"amount ${amount:.2f} is {abs(z):.1f} standard deviations from "
            f"user's mean of ${profile['mean_amount']:.2f}"
        )
        return True, reason
    return False, None


def rule_category_novelty(profile: dict, category: str):
    n = profile["txn_count"]
    if n < MIN_TXNS_FOR_ZSCORE:
        return False, None  # cold start — everything looks "novel" at first

    seen = profile["common_categories"] or {}
    if category not in seen or seen.get(category, 0) == 0:
        return True, f"category '{category}' never seen before for this user"
    return False, None


def rule_velocity(engine, user_id: str, timestamp):
    window_start = timestamp - timedelta(minutes=VELOCITY_WINDOW_MINUTES)
    with engine.connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT count(*) FROM transactions
                WHERE user_id = :uid AND "timestamp" >= :window_start
                """
            ),
            {"uid": user_id, "window_start": window_start},
        ).scalar()

    if count > VELOCITY_MAX_TXNS:
        return True, f"{count} transactions in the last {VELOCITY_WINDOW_MINUTES} min (limit {VELOCITY_MAX_TXNS})"
    return False, None


def update_profile(engine, profile: dict, amount: float, category: str):
    """Welford's online update for mean/variance, plus a category counter.
    Only called for transactions that did NOT get flagged."""
    n = profile["txn_count"] + 1
    delta = amount - profile["mean_amount"]
    new_mean = profile["mean_amount"] + delta / n
    delta2 = amount - new_mean
    new_m2 = profile["m2_amount"] + delta * delta2

    categories = dict(profile["common_categories"] or {})
    categories[category] = categories.get(category, 0) + 1

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE user_profiles
                SET txn_count = :n,
                    mean_amount = :mean,
                    m2_amount = :m2,
                    common_categories = :categories,
                    updated_at = now()
                WHERE user_id = :uid
                """
            ),
            {
                "n": n,
                "mean": new_mean,
                "m2": new_m2,
                "categories": __import__("json").dumps(categories),
                "uid": profile["user_id"],
            },
        )


def evaluate_transaction(engine, txn: dict) -> dict:
    """Runs all rules against a transaction and returns a verdict:
    {flagged: bool, score: float, reasons: [str, ...], triggered_rules: [str, ...]}.

    `reasons` is human-readable text for display; `triggered_rules` is the
    stable rule-name list (matching RULE_WEIGHTS' keys) for aggregation —
    e.g. the dashboard's "top reasons" chart groups by these names rather
    than parsing free text. See BUILD_LOG.md, Phase 3: the original design
    only stored the free-text reason, which turned out not to be queryable
    once the dashboard needed to group flagged events by rule.

    Also updates (or intentionally skips updating) the user's profile.
    """
    from datetime import datetime

    profile = get_or_create_profile(engine, txn["user_id"])
    amount = float(txn["amount"])
    category = txn["merchant_category"]
    timestamp = datetime.fromisoformat(txn["timestamp"])

    score = 0.0
    reasons = []
    triggered_rules = []

    triggered, reason = rule_amount_zscore(profile, amount)
    if triggered:
        score += RULE_WEIGHTS["amount_zscore"]
        reasons.append(reason)
        triggered_rules.append("amount_zscore")

    triggered, reason = rule_category_novelty(profile, category)
    if triggered:
        score += RULE_WEIGHTS["category_novelty"]
        reasons.append(reason)
        triggered_rules.append("category_novelty")

    triggered, reason = rule_velocity(engine, txn["user_id"], timestamp)
    if triggered:
        score += RULE_WEIGHTS["velocity"]
        reasons.append(reason)
        triggered_rules.append("velocity")

    flagged = score >= FLAG_THRESHOLD

    # Don't let a flagged (likely fraudulent) transaction poison the
    # baseline profile — see module docstring point 3.
    if not flagged:
        update_profile(engine, profile, amount, category)

    return {"flagged": flagged, "score": score, "reasons": reasons, "triggered_rules": triggered_rules}

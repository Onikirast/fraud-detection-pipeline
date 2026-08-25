# Build Log

A running record of bugs, issues, and fixes hit while building this project. Kept separate from `README.md` (which just covers setup/run instructions) so this can double as interview prep material — "tell me about a bug you ran into" is a near-guaranteed question, and having real, specific answers on hand is worth a lot more than a generic one.

Each entry: what broke, why, how it was fixed, and what it taught us. Newest entries at the top.

---

## Phase 3 — Dashboard

**Date:** 2026-08-22

### Issue 1: the schema couldn't answer the dashboard's own question

**What broke:** Building the `/stats/top-reasons` endpoint (which rules fire most often), the only data available was `flagged_events.reason` — a free-text string like `"amount $900 is 4 stddev...; category never seen before"`. There's no clean way to `GROUP BY` a concatenated sentence.

**Root cause:** Phase 2's schema was designed around what the *consumer* needed (a human-readable explanation to print/store), not around what a *downstream consumer of the data* — the dashboard — would need. Classic case of a schema that looks fine until a second, different access pattern shows up.

**Fix:** Added a `rule_types TEXT[]` column alongside the existing `reason` text — the structured, stable rule names (`amount_zscore`, `velocity`, etc.) for querying, keeping `reason` purely for display. `detection.py`'s `evaluate_transaction()` now returns both. The top-reasons query uses Postgres's `unnest()` to flatten the array into groupable rows.

**Takeaway for interviews:** A good, concrete example of "design for your data's second consumer, not just its first." Also a nice, low-effort demonstration of denormalization done on purpose — storing both a structured and a human-readable form of the same fact, because they serve different readers (the API vs. a person scanning logs).

### Issue 2 (observation, not a bug): event time vs. processing time

**What happened:** Seeding test data with backdated transaction timestamps (to simulate a spread of history) made the time-series chart show one giant spike instead of a spread — every flagged event still got `flagged_at = now()` at insert time, regardless of the transaction's own (backdated) `timestamp`.

**Root cause:** Not a bug — `flagged_at` is intentionally processing time ("when did we catch this"), while `transactions.timestamp` is event time ("when did this happen"). They're the same instant in the real pipeline (transactions are processed within moments of being generated), so this only surfaces when synthetic/replayed data decouples the two.

**Why it's worth knowing anyway:** This is the real event-time-vs-processing-time distinction from streaming systems (the same issue Kafka/Flink users deal with constantly). It matters here specifically because the build plan lists "replay detection over history" as a stretch goal — if that's ever built, it should bucket by event time, not processing time, or old data will cluster at whatever moment the replay was run. Left as-is for now since it's correct for live operation; noting it here so it isn't a surprise later.

---

## Phase 1 — Pipeline Skeleton

**Date:** 2026-08-15

**Issue:** No Docker daemon available in the environment used to build and test this phase, so the full `docker compose up` stack (Redpanda + Postgres together) couldn't be run end-to-end during development.

**Root cause:** Sandboxed build environment; not a bug in the project itself.

**Fix / workaround:** Rather than skip validation entirely, installed Postgres locally, applied `db/schema.sql` directly, and ran the consumer's `insert_transaction` function against it directly — including a deliberate duplicate-insert test to confirm the idempotency handling (`ON CONFLICT DO NOTHING`) actually works, not just that it looks correct on paper. The generator's transaction logic (normal/spike/novel-category generation) was also unit-tested in isolation, separately from Kafka.

**What's still unverified:** The actual Redpanda-in-the-loop flow (generator → queue → consumer) needs to be run on a machine with Docker to fully confirm. If you hit connection issues here, they'll most likely be Docker/networking related rather than application logic, since the logic on both ends has already been validated independently.

**Takeaway for interviews:** This is a good example of testing components in isolation when you can't test the full integrated system — a pattern that comes up constantly in real engineering work (e.g., mocking a dependency that isn't available yet, or that's expensive to spin up in CI).

---

## Phase 2 — Detection Logic

**Date:** 2026-08-20

### Issue 1: module name collided with its own package directory

**What broke:** A standalone test script did `from consumer.consumer import insert_transaction` and failed with `ModuleNotFoundError: No module named 'consumer.consumer'; 'consumer' is not a package`.

**Root cause:** The consumer package's main file was named `consumer.py`, living inside a directory *also* named `consumer/` — with no `__init__.py`. When that file was run directly (`python consumer/consumer.py`), Python auto-added its own directory to `sys.path`, so the ambiguity never surfaced during manual testing. But the moment something *external* tried to import it as `consumer.consumer` (as a proper package member), Python resolved the bare name `consumer` to the wrong thing depending on `sys.path` order, and the self-referential name made the failure mode confusing to read.

**Fix:** Renamed `consumer/consumer.py` → `consumer/main.py` (a file should not share its own package's name), added `consumer/__init__.py` and `db/__init__.py` to make both real packages, and made `main.py` explicitly append both its own directory and the project root to `sys.path` at the top rather than relying on Python's "script directory is auto-added" behavior — which only holds when the file is run directly, not when it's imported.

**Takeaway for interviews:** A good example of a bug that only shows up under a different invocation path than the one you tested first (direct script run vs. import-as-module). Also a reminder that "it works when I run it" and "it works when something else imports it" are genuinely different claims.

### Issue 2: a rule's weight made it structurally unreachable

**What broke:** Nothing crashed — a test assertion failed. An isolated test expected the `category_novelty` rule (weight 0.6) to flag a transaction on its own. It never did, in any test.

**Root cause:** `FLAG_THRESHOLD` is 1.0, and both other rules (`amount_zscore`, `velocity`) are weighted at exactly 1.0 — meaning either one alone already crosses the threshold. `category_novelty` was the *only* rule weighted below 1.0. The original design doc/docstring described "weaker signals stacking to cross the threshold," but with just one sub-threshold rule, there's nothing for it to stack *with* — that code path was unreachable by construction, and nothing in the code would have surfaced this without a test that specifically tried to trigger the rule in isolation.

**Fix:** Rather than force a fix that doesn't reflect real fraud-detection judgment (e.g. arbitrarily lowering the threshold to 0.6, which would make any first-time category purchase get flagged — too noisy), corrected the documentation to describe what the rule actually does: it can't independently decide flagged/not-flagged, but it raises the *severity score* of a transaction another rule already flagged, which is useful for ranking what a human reviewer looks at first on the dashboard. Added a test that explicitly locks in both behaviors: category alone contributes to score but doesn't flag, and category + a real trigger produces a higher score than the trigger alone.

**Takeaway for interviews:** This is a great one to have ready — it shows you validate designs against tests rather than assuming the doc/comments were right, and that when a test reveals the code doesn't match the stated intent, the fix isn't always "change the code" — sometimes the code's behavior is more defensible than the original claim, and the fix is correcting the claim.

---

*(Log continues as Phase 3+ are built.)*

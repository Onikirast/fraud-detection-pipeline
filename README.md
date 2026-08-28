# Fraud/Anomaly Detection Pipeline

See `ARCHITECTURE_AND_BUILD_PLAN.md` for the full design doc, data model, and phased build plan. See `BUILD_LOG.md` for a running record of bugs/issues hit while building and how they were fixed — good interview prep material.

## Status: Phase 1 + 2 + 3 complete, Phase 4 in progress

Transactions are generated, published to Redpanda, consumed, stored in Postgres, and run through the anomaly detection rules (rolling per-user profile, z-score / category-novelty / velocity checks). Flagged transactions are written to `flagged_events` and served by a FastAPI backend to a Streamlit dashboard (KPI tiles, a flag-volume-over-time chart, a top-triggered-rules chart, and a recent-flags table). Phase 4 (demo scenarios + polish) is underway — see "Demoing a specific scenario" and "Design trade-offs" below.

### Running it

1. Copy the env file and adjust if needed:
   ```
   cp .env.example .env
   ```

2. Start the infrastructure:
   ```
   docker compose up -d
   ```
   This starts Redpanda (Kafka-API-compatible broker) and Postgres. Postgres auto-applies `db/schema.sql` on first boot.

3. Create a Python virtual environment and install dependencies:
   ```
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

4. Start each piece in its own terminal (4 total), from the project root with the venv active in each:
   ```
   # terminal 1
   python consumer/main.py

   # terminal 2
   python generator/generate_transactions.py

   # terminal 3
   uvicorn api.main:app --reload --port 8000

   # terminal 4
   streamlit run dashboard/app.py
   ```

5. Open the dashboard the `streamlit run` command prints a URL for (typically `http://localhost:8501`). The FastAPI interactive docs are at `http://localhost:8000/docs`.

Note the generator only injects deliberate anomalies ~5% of the time, and the detection rules need at least 5 transactions of history for a user before the amount/category checks activate (a "cold start" guard — see `consumer/detection.py`), so give it a few minutes running before the dashboard shows flagged events.

You can also verify directly in Postgres at any point:
```
docker exec -it fraud_postgres psql -U fraud -d frauddb -c "SELECT count(*) FROM transactions;"
docker exec -it fraud_postgres psql -U fraud -d frauddb -c "SELECT reason, rule_types, score, flagged_at FROM flagged_events ORDER BY flagged_at DESC LIMIT 10;"
```

### Demoing a specific scenario

The generator only injects a deliberate anomaly about 5% of the time, for a random user — fine for realistic background noise, not great when you want to show a rule catching something *right now*. With the full pipeline running (all four terminals from above), use `demo/inject_scenario.py` to trigger one on demand for a real, already-active user:

```
python demo/inject_scenario.py --list                              # see who has enough history to target
python demo/inject_scenario.py --scenario spike                    # one transaction, way above that user's normal amount
python demo/inject_scenario.py --scenario novel_category           # a category that user has never bought in
python demo/inject_scenario.py --scenario burst                    # several transactions in quick succession
python demo/inject_scenario.py --scenario combo                    # a spike AND a novel category, together
```

It publishes through the same Kafka topic the generator uses — nothing bypasses the pipeline — then polls the database for a few seconds and prints what got flagged. One thing worth knowing going in: `novel_category` on its own is expected to **not** flag (see `BUILD_LOG.md`, Phase 2 and Phase 4) — that rule is deliberately weighted as a severity booster, not an independent trigger. Run `combo` to see it actually cross the threshold stacked with a spike.

### What's been verified

**Phase 1:** schema, SQLAlchemy models, and the consumer's idempotent-insert logic tested end-to-end against a real Postgres instance; generator transaction logic unit-verified in isolation.

**Phase 2:** the three detection rules (z-score, category novelty, velocity) were each tested in isolation against a real Postgres instance to confirm they fire independently and correctly, plus the cold-start guard, the profile-poisoning avoidance (flagged transactions don't update the baseline), and duplicate-delivery safety after the detection logic was wired in.

**Phase 3:** all four API endpoints were exercised against real data produced by running many simulated users through the actual detection pipeline (not hand-written rows). The Streamlit dashboard was loaded in a real headless browser to confirm it renders the KPI tiles, both charts, and the table with correct values and no console/runtime errors, and was visually inspected via screenshot.

See `BUILD_LOG.md` for real issues this testing caught across all three phases and how they were fixed — including a schema change prompted by the dashboard needing structured (not free-text) rule data, and a debounce fix for the velocity rule re-flagging every transaction during a busy stretch instead of just the one that crosses the threshold.

Running the full Docker Compose stack (Redpanda + Postgres + all four processes together) still needs to be done on your machine, since it requires a Docker daemon — the build environment used here doesn't have one. Everything above was validated with Postgres running directly (no queue) and, for the generator/consumer, by feeding data through their real functions directly rather than through Kafka — see `BUILD_LOG.md`, Phase 1.

## Design trade-offs

Notes on the decisions behind this project, and their honest limits — written to double as interview prep, not just documentation.

**Why a message queue instead of the generator calling the detector directly.** Redpanda decouples the two: the detector can be slow, crash, or restart without losing transactions or blocking the generator, and multiple consumer instances could read from the same topic to scale detection horizontally (partitioned by `user_id`, so all of one user's transactions land on the same consumer and its rolling profile stays consistent). None of that is exercised by this project's single-consumer setup, but the decoupling is real and the architecture supports it without changes.

**Why rule-based detection instead of a trained model.** Every flag here comes with a plain-English reason (`amount $900 is 4.2 stddev from user's mean of $62`), which matters more than raw accuracy for a system whose output a human reviewer has to act on — an ML model's flags are much harder to explain to that reviewer, and harder to debug when they're wrong. The trade-off: this needs no labeled fraud data (which is genuinely hard to get — confirmed fraud is rare and confirmation is slow), but it also can't learn subtle multi-feature patterns a model might catch. `RULE_WEIGHTS`/`FLAG_THRESHOLD` in `consumer/detection.py` are hand-set, not fit to data, because there's no labeled data here to fit them to.

**How duplicate delivery is handled.** Kafka guarantees at-least-once delivery, not exactly-once — a consumer restart can replay already-processed messages. `insert_transaction()`'s `ON CONFLICT DO NOTHING ... RETURNING` makes the insert idempotent and skips detection entirely on a redelivered message, so a replay can't double-flag a transaction or double-update a user's rolling profile.

**How false positives would actually get reduced in production.** Three levers, in the order they're worth trying: (1) retune `RULE_WEIGHTS`/thresholds against real outcomes once you have any labeled data at all — even a few hundred confirmed fraud/not-fraud cases beats hand-picked numbers; (2) add a human-in-the-loop review queue instead of auto-blocking on every flag, so a wrong flag costs a reviewer a minute instead of a declined legitimate purchase; (3) feed reviewer verdicts back into the weights over time. None of this is implemented — it's the natural next step once the system has real usage to learn from, and it's a good answer to "how would you improve this."

**What would break at real scale, and what wouldn't.** The per-user rolling profile (Welford's algorithm) is O(1) per transaction regardless of history size, so that part scales fine. The velocity rule's `COUNT(*) ... WHERE timestamp >= window_start` query does not — it re-scans recent rows on every transaction, which is fine at this project's throughput but would need a different structure (a sliding counter in Redis, or a materialized recent-count column) well before real production volume. This project doesn't need that yet; a system handling millions of users would.

**Two known, documented limitations, not fixed here on purpose (see `BUILD_LOG.md` for the fuller story on both):** the velocity rule's debounce is stateless edge-detection against a single window snapshot, not full per-user hysteresis, so it can still occasionally double-flag when traffic oscillates right at the threshold; and `category_novelty` can never independently flag a transaction by design (see `demo/inject_scenario.py`'s `novel_category` vs. `combo` scenarios for a live demonstration of exactly that). Both are explained rather than silently left as mysteries — a good rule for any project going on a resume: know the edges of what you built, and be ready to say so plainly.

**Optional stretch goal, not built:** swapping the z-score rule for scikit-learn's Isolation Forest and comparing its flags against the rule-based ones would be a natural "I evaluated two approaches" story, if there's time to add it later.

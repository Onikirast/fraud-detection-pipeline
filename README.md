# Fraud/Anomaly Detection Pipeline

See `ARCHITECTURE_AND_BUILD_PLAN.md` for the full design doc, data model, and phased build plan. See `BUILD_LOG.md` for a running record of bugs/issues hit while building and how they were fixed — good interview prep material.

## Status: Phase 1 + 2 + 3 complete

Transactions are generated, published to Redpanda, consumed, stored in Postgres, and run through the anomaly detection rules (rolling per-user profile, z-score / category-novelty / velocity checks). Flagged transactions are written to `flagged_events` and served by a FastAPI backend to a Streamlit dashboard (KPI tiles, a flag-volume-over-time chart, a top-triggered-rules chart, and a recent-flags table). Phase 4 (demo scenarios + polish) is next.

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

### What's been verified

**Phase 1:** schema, SQLAlchemy models, and the consumer's idempotent-insert logic tested end-to-end against a real Postgres instance; generator transaction logic unit-verified in isolation.

**Phase 2:** the three detection rules (z-score, category novelty, velocity) were each tested in isolation against a real Postgres instance to confirm they fire independently and correctly, plus the cold-start guard, the profile-poisoning avoidance (flagged transactions don't update the baseline), and duplicate-delivery safety after the detection logic was wired in.

**Phase 3:** all four API endpoints were exercised against real data produced by running many simulated users through the actual detection pipeline (not hand-written rows). The Streamlit dashboard was loaded in a real headless browser to confirm it renders the KPI tiles, both charts, and the table with correct values and no console/runtime errors, and was visually inspected via screenshot.

See `BUILD_LOG.md` for real issues this testing caught across all three phases and how they were fixed — including a schema change prompted by the dashboard needing structured (not free-text) rule data.

Running the full Docker Compose stack (Redpanda + Postgres + all four processes together) still needs to be done on your machine, since it requires a Docker daemon — the build environment used here doesn't have one. Everything above was validated with Postgres running directly (no queue) and, for the generator/consumer, by feeding data through their real functions directly rather than through Kafka — see `BUILD_LOG.md`, Phase 1.

## Next: Phase 4 — resume/interview polish

Seed attack scenarios for a live demo, expand this README with trade-off notes, optional Isolation Forest comparison stretch goal.

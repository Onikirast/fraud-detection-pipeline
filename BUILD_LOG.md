# Build Log

A running record of bugs and issues hit while building this project, what caused them, and how they were fixed. See `README.md` for setup and run instructions.

Newest entries at the top.

---

## Gitignored .env kept reverting inside the OneDrive-synced folder

**Date:** 2026-09-05

**What broke:** After correcting `.env`'s velocity thresholds (1 min / limit 8) and restarting the consumer, the dashboard still showed the old, uncalibrated "10 min (limit 5)" behavior. Re-checking `.env` directly showed it had silently reverted to the stale values, with no edit made by hand.

**Root cause:** `.env` is the one file in this project not tracked by git (`.gitignore` excludes it, correctly, since it can hold secrets) — so unlike every other file, nothing was protecting it from being overwritten by a stale cached copy from OneDrive's cloud sync, which appears to have periodically re-synced an old local version back down over the corrected one.

**Fix:** Rather than fight the sync behavior, exported the detection thresholds as shell environment variables from a small script kept outside the OneDrive folder entirely, sourced before starting the consumer. `python-dotenv` never overrides a variable that's already set in the environment, so this takes priority over whatever `.env` says regardless of sync state. Confirmed working: `.env` still showed the stale values afterward, but the running consumer correctly used the exported ones.

---

## Committed requirements.txt never actually got the kafka-python fix

**Date:** 2026-09-05

**What broke:** Running `pip install -r requirements.txt` into a freshly created venv for the first real end-to-end run installed `kafka-python==2.0.2` — the exact pre-fix version the "Local setup (Windows/WSL2)" entry below documents replacing weeks earlier, because `2.0.2` breaks under Python 3.12 (`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`).

**Root cause:** The version bump was made to the working file at the time, but that change never actually made it into the `requirements.txt` that got `git add`ed into the initial commit — the commit captured whatever was on disk at that moment, which turned out to still be the old pin. Nothing caught this until now because every phase up to this point was validated by exercising the generator/consumer logic directly, never by installing dependencies fresh into a clean venv.

**Fix:** Re-pinned `requirements.txt` to `kafka-python==2.3.2` and reinstalled. Verified `from kafka import KafkaProducer` imports cleanly under Python 3.12.

---

## Generator restarts silently invalidated everyone's profile

**Date:** 2026-08-27

**What broke:** After stopping and restarting the pipeline on a later day, the flag rate spiked to roughly 40% almost immediately, dominated by `amount_zscore` flags with extreme z-scores (some over 40 standard deviations) across nearly every user. This happened twice, on two separate days.

**Root cause:** `generator/generate_transactions.py`'s `USERS` list — each simulated user's `baseline_mean`, `baseline_stddev`, and `home_categories` — was built from the shared global `random` module at import time, meaning every restart of the generator re-randomized every user's "true" spending profile, while `docker compose up -d` only restarts the containers — it doesn't touch the data already sitting in the Postgres volume from the previous run. So `user_009`'s stored profile still reflected the old baseline, but the generator was now sending transactions matching a freshly-randomized baseline for that same `user_id`, so nearly everything looked (correctly) wildly anomalous against a baseline that no longer matched reality. The first time this happened it was fixed operationally (truncate the tables before restarting); the second time it happened again because that fix wasn't durable.

**Fix:** Made each user's baseline deterministic — derived from a `random.Random` instance seeded by that user's `user_id`, instead of the shared global `random` module — so restarting the generator now produces the exact same baseline for `user_005` every time, verified identical across two independent process runs. Restarting the generator can no longer invalidate history already accumulated in Postgres. `location` is left random per restart since it's cosmetic and never read by any detection rule.

---

## Phase 4 — Demo scenario injector

**Date:** 2026-08-26

**What happened:** Built `demo/inject_scenario.py` so a specific anomaly can be triggered on demand during a live demo, instead of waiting on the generator's random ~5% anomaly rate. It reads a real user's current profile out of `user_profiles` and reuses the detector's own `stddev_from_profile()` to size a spike, so the crafted transaction is guaranteed to be anomalous by the exact same math the detector judges it with. A first version included a `novel_category` scenario meant to demonstrate the category-novelty rule flagging on its own.

**What broke:** It didn't flag. Isolated testing against `evaluate_transaction` directly caught it immediately: `category_novelty` is weighted 0.6 against a `FLAG_THRESHOLD` of 1.0, so it can never flag by itself — the same design decision from the Phase 2 entry below (`category_novelty` as a severity booster, not an independent trigger), rediscovered from a different angle.

**Fix:** A `novel_category` demo run that doesn't flag now says so explicitly and points at this log entry, and a new `combo` scenario (a spike into a novel category) demonstrates the rule's actual job: stacking with a real trigger for a higher severity score (1.6 vs. 1.0 for the spike alone).

---

## Velocity rule dominated the "top triggered rules" chart — alert flapping

**Date:** 2026-08-25

**What broke:** After the calibration fix below, the pipeline ran cleanly (no more near-universal flagging), but the dashboard's "most frequently triggered rules" chart still showed `velocity` far ahead of the other rules (~750-800 triggers vs. ~150 for `amount_zscore`, out of 855 flagged events from 4,696 transactions). The threshold itself was right — something else was inflating the count.

**Root cause:** `rule_velocity()` re-evaluates the rolling window on every transaction and flags whenever `count > VELOCITY_MAX_TXNS`. That condition stays true for as long as the window stays busy, so one naturally busier-than-average minute didn't produce one alert — it produced a cascade, flagging every transaction for the rest of that busy stretch. Same failure shape as a monitoring alert that fires on every scrape while a metric is above threshold, instead of once when it crosses.

**Fix:** Added debounce to `rule_velocity()` in `consumer/detection.py`. The rule already computes `count` (transactions in the window, including the current one). `count - 1` is the count of every other transaction in that window — how full it was without this new arrival. If that was already over the limit, this transaction is just one more on top of an already-flagged streak, so it's suppressed. If it wasn't, this transaction is the one that tips the window over, so it's the only one that flags.

Verified against a real local Postgres instance with two scenarios: a sustained burst of 15 transactions confirming exactly 1 flag fires instead of the ~7 that would have fired before, and a burst → window-clears → second-burst sequence confirming the fix re-arms correctly rather than being a one-shot "only ever flag once per user" hack.

**Honest limitation:** This is edge-detection against a single window snapshot, not fully stateful per-user hysteresis. If the count oscillates right at the boundary (9, 8, 9, 8...) it can still re-flag more than once, because each check only asks "was the window full without me," not "has this user been continuously over the limit since the last flag." A zero-flap version would need to persist per-user alert state (e.g. a `last_velocity_flag_at` column) and only re-arm once the count has stayed under the limit for a full window's worth of time. Didn't build that here — a burst now produces exactly 1 flagged event instead of 5-8, which is enough for what this project needs.

---

## First real end-to-end run — velocity rule flagged almost everything

**Date:** 2026-08-24

**What broke:** Running the full pipeline live for the first time, nearly every transaction for nearly every user got flagged with `velocity` as the reason, and the "N transactions in the last 10 min" count climbed continuously instead of staying low and occasionally spiking. The z-score and category-novelty flags mixed in were all correct, real injected anomalies — velocity was the only rule producing noise.

**Root cause:** A threshold that was never wrong in testing, but was never tested against the actual traffic volume it would face. Phase 2's isolated tests deliberately spaced synthetic transactions far apart, so the velocity rule was only ever exercised against hand-picked scenarios, never against the generator's real throughput. The generator publishes at roughly 1-2 transactions/second across a pool of just 25 users, chosen at random each time — which works out to each user accumulating around 40 transactions every 10 minutes from normal background activity alone. The velocity threshold was 5 per 10 minutes, so ordinary traffic was already 8x over the limit before any deliberate burst was injected.

**Fix:** Recalibrated using the actual measured numbers instead of a guessed threshold. With ~4 transactions/minute/user at baseline (occasional natural variance up to ~8-10 in a given minute), a 1-minute window with a threshold of 8 keeps normal noise below the line while a deliberate burst reliably pushes well past it. The general fix isn't "these exact numbers" — it's always deriving a rate-based threshold from the actual expected base rate, the same way you'd size an API rate limiter against real traffic rather than intuition.

---

## Local setup (Windows/WSL2) — dependency broke on a newer Python than the build environment used

**Date:** 2026-08-23

**What broke:** `pip install -r requirements.txt` succeeded, but `import kafka` failed with `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`.

**Root cause:** `requirements.txt` pinned `kafka-python==2.0.2` (from 2021), which vendors an old copy of the `six` compatibility library using a module-aliasing trick that breaks under Python 3.12's import system. The build/test environment happened to run Python 3.11, so the incompatibility never surfaced until run on a machine with Python 3.12 (Ubuntu 24.04's default, which WSL2 installs).

**Fix:** Confirmed via the project's GitHub issues/releases that this is a known, already-fixed problem — Python 3.12 support landed in `kafka-python` 2.0.3, and the package has since matured through the 2.x line (a 3.0.x rewrite also exists but changes the transport layer significantly, so it wasn't worth adopting here). Bumped the pin to `kafka-python==2.3.2` — same synchronous `KafkaProducer`/`KafkaConsumer` API, just with the Python 3.12 fix included.

**Honest caveat:** This fix was not re-verified end-to-end against a live Redpanda broker from the build environment (no Docker daemon available there) — the version bump was based on documented Python 3.12 support and an unchanged public API within the 2.x series, not a rerun of the full pipeline. The generator/consumer actually running against Redpanda on your machine was the first real end-to-end confirmation.

---

## Phase 3 — Dashboard

**Date:** 2026-08-22

### Issue 1: the schema couldn't answer the dashboard's own question

**What broke:** Building the `/stats/top-reasons` endpoint (which rules fire most often), the only data available was `flagged_events.reason` — a free-text string like `"amount $900 is 4 stddev...; category never seen before"`. There's no clean way to `GROUP BY` a concatenated sentence.

**Root cause:** Phase 2's schema was designed around what the consumer needed (a human-readable explanation to print/store), not around what a downstream consumer of the data — the dashboard — would need.

**Fix:** Added a `rule_types TEXT[]` column alongside the existing `reason` text — structured, stable rule names (`amount_zscore`, `velocity`, etc.) for querying, keeping `reason` purely for display. `detection.py`'s `evaluate_transaction()` now returns both, and the top-reasons query uses Postgres's `unnest()` to flatten the array into groupable rows.

### Issue 2 (observation, not a bug): event time vs. processing time

**What happened:** Seeding test data with backdated transaction timestamps made the time-series chart show one giant spike instead of a spread — every flagged event still got `flagged_at = now()` at insert time, regardless of the transaction's own (backdated) `timestamp`.

**Why:** Not a bug — `flagged_at` is intentionally processing time ("when did we catch this"), while `transactions.timestamp` is event time ("when did this happen"). They're the same instant in the real pipeline, so this only surfaces when synthetic/replayed data decouples the two. Worth noting for any future replay-detection-over-history feature, which should bucket by event time, not processing time.

---

## Phase 1 — Pipeline Skeleton

**Date:** 2026-08-15

**Issue:** No Docker daemon available in the environment used to build and test this phase, so the full `docker compose up` stack (Redpanda + Postgres together) couldn't be run end-to-end during development.

**Fix / workaround:** Installed Postgres locally, applied `db/schema.sql` directly, and ran the consumer's `insert_transaction` function against it directly — including a deliberate duplicate-insert test to confirm the idempotency handling (`ON CONFLICT DO NOTHING`) actually works. The generator's transaction logic was also unit-tested in isolation, separately from Kafka.

**What's still unverified:** The actual Redpanda-in-the-loop flow (generator → queue → consumer) needed to be run on a machine with Docker to fully confirm. (This was later run end to end — see the 2026-08-24 and 2026-09-05 entries above.)

---

## Phase 2 — Detection Logic

**Date:** 2026-08-20

### Issue 1: module name collided with its own package directory

**What broke:** A standalone test script did `from consumer.consumer import insert_transaction` and failed with `ModuleNotFoundError: No module named 'consumer.consumer'; 'consumer' is not a package`.

**Root cause:** The consumer package's main file was named `consumer.py`, living inside a directory also named `consumer/`, with no `__init__.py`. Running it directly worked because Python auto-adds a script's own directory to `sys.path`, so the ambiguity never surfaced during manual testing — but importing it as `consumer.consumer` resolved the bare name `consumer` to the wrong thing.

**Fix:** Renamed `consumer/consumer.py` → `consumer/main.py`, added `consumer/__init__.py` and `db/__init__.py` to make both real packages, and made `main.py` explicitly append both its own directory and the project root to `sys.path` at the top, rather than relying on the auto-add behavior that only applies when a file is run directly.

### Issue 2: a rule's weight made it structurally unreachable

**What broke:** Nothing crashed — a test assertion failed. An isolated test expected the `category_novelty` rule (weight 0.6) to flag a transaction on its own. It never did.

**Root cause:** `FLAG_THRESHOLD` is 1.0, and both other rules (`amount_zscore`, `velocity`) are weighted at exactly 1.0, so either one alone already crosses the threshold. `category_novelty` was the only rule weighted below 1.0. The original design doc described "weaker signals stacking to cross the threshold," but with only one sub-threshold rule, there's nothing for it to stack with — that code path was unreachable by construction.

**Fix:** Rather than force a fix that doesn't reflect real fraud-detection judgment (e.g. arbitrarily lowering the threshold, which would flag any first-time category purchase), corrected the documentation to describe what the rule actually does: it can't independently flag a transaction, but it raises the severity score of one another rule already flagged, useful for ranking what a reviewer looks at first. Added a test that locks in both behaviors: category alone contributes to score but doesn't flag, and category + a real trigger produces a higher score than the trigger alone.

---

*(Log continues as the project is extended further.)*

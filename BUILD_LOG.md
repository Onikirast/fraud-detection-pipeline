# Build Log

A running record of bugs, issues, and fixes hit while building this project. Kept separate from `README.md` (which just covers setup/run instructions) so this can double as interview prep material — "tell me about a bug you ran into" is a near-guaranteed question, and having real, specific answers on hand is worth a lot more than a generic one.

Each entry: what broke, why, how it was fixed, and what it taught us. Newest entries at the top.

---

## Committed requirements.txt never actually got the kafka-python fix

**Date:** 2026-09-05

**What broke:** Running `pip install -r requirements.txt` into a freshly created venv for the first real end-to-end run installed `kafka-python==2.0.2` — the exact pre-fix version the "Local setup (Windows/WSL2)" entry below documents replacing weeks earlier, specifically because `2.0.2` breaks under Python 3.12 (`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`).

**Root cause:** The version bump was made to the working file at the time, but that change never actually made it into the `requirements.txt` that got `git add`ed into the initial commit — the commit captured whatever was on disk at that moment, which turned out to still be the old pin. Nothing caught this until now because every phase up to this point was validated by exercising the generator/consumer logic directly (calling their functions in-process), never by installing dependencies fresh from `requirements.txt` into a clean venv and letting Python actually `import kafka` — precisely the gap the "still needs to be done on your machine" caveat in `README.md` existed to flag.

**Fix:** Re-pinned `requirements.txt` to `kafka-python==2.3.2` and reinstalled. Verified `from kafka import KafkaProducer` imports cleanly under this venv's Python 3.12.

**Takeaway for interviews:** A concrete example of why "I fixed it" and "the fix is actually in what ships" are two different claims — a change made to a live working file doesn't count as shipped until it's confirmed present in what actually gets committed and installed from clean. It's also exactly the class of bug that only running the full pipeline from a clean install (not testing components in isolation) was ever going to catch, which is the whole reason that run mattered even this late in the project.

---

## Generator restarts silently invalidated everyone's profile

**Date:** 2026-08-27

**What broke:** After stopping and restarting the pipeline on a later day (Docker containers, consumer, generator), the flag rate spiked to roughly 40% almost immediately, dominated by `amount_zscore` flags with extreme z-scores (some over 40 standard deviations) across nearly every user. This happened twice, on two separate days.

**Root cause:** `generator/generate_transactions.py`'s `USERS` list — each simulated user's `baseline_mean`, `baseline_stddev`, and `home_categories` — was built from the shared global `random` module at import time, meaning **every restart of the generator re-randomized every user's "true" spending profile**, while `docker compose up -d` only restarts the containers, it doesn't touch the data already sitting in the Postgres volume from the previous run. So `user_009`'s profile in `user_profiles` still reflected the *old* baseline from before, but the generator was now sending transactions matching a completely different, freshly-randomized baseline for that same `user_id` — meaning nearly everything looked wildly anomalous, correctly, against a baseline that no longer had anything to do with reality. The first time this happened it was diagnosed and fixed operationally (truncate the tables before restarting); the second time it happened again, because that operational fix wasn't durable — nothing in the code prevented forgetting that step.

**Fix:** Made each user's baseline deterministic — derived from a `random.Random` instance seeded by that user's `user_id`, instead of the shared global `random` module — so restarting the generator now produces the *exact same* `baseline_mean`/`baseline_stddev`/`home_categories` for `user_005` every time, verified identical across two fully independent process invocations. Restarting the generator can no longer invalidate history that's already accumulated in Postgres. `location` is left random per restart since it's cosmetic and never read by any detection rule.

**Takeaway for interviews:** The first fix (truncate before restarting) treated the symptom; this fix addresses the actual cause — a stateful simulation whose "ground truth" wasn't pinned to the identity (`user_id`) it was supposed to represent. It's the same category of bug as a test suite that uses `random` without a fixed seed: it passes today and fails tomorrow for a reason that has nothing to do with the code path being tested. The general lesson — when something reproduces intermittently across restarts/reruns of the same nominal input, check whether "the same input" is actually the same, or just looks the same — generalizes well beyond this project.

---

## Phase 4 — Demo scenario injector

**Date:** 2026-08-26

**What happened:** Built `demo/inject_scenario.py` so a specific anomaly can be triggered on demand during a live demo, instead of waiting on the generator's random ~5% anomaly rate. It reads a real user's current profile out of `user_profiles` and reuses the detector's own `stddev_from_profile()` to size a spike, so the crafted transaction is guaranteed to be anomalous by the exact same math the detector judges it with — not a hand-tuned guess. A first version included a `novel_category` scenario meant to demonstrate the category-novelty rule flagging on its own.

**What broke:** It didn't flag. Isolated testing (same pattern as every other phase — real Postgres, no live broker available in this build environment, so the Kafka publish step goes through unchanged, already-proven code while the actual new logic gets verified directly against `evaluate_transaction`) caught it immediately: `category_novelty` is weighted 0.6 against a `FLAG_THRESHOLD` of 1.0, so it can never flag by itself — this is the exact design decision from the Phase 2 entry above (`category_novelty` as a severity booster, not an independent trigger), just rediscovered from a different angle while building a tool that assumed otherwise.

**Fix:** Rather than treat "not flagged" as the script failing, it now says so explicitly — a `novel_category` demo run that doesn't flag prints why, pointing at this log entry — and a new `combo` scenario (a spike into a novel category) demonstrates the rule's actual job: stacking with a real trigger for a higher severity score (1.6 vs. 1.0 for the spike alone), which is what a human reviewer would actually use to triage.

**Takeaway for interviews:** A good example of a tool built on top of a system surfacing a fact about that system's own design, rather than a bug — and a reminder that "isolated logic testing" earns its keep every time you build something new on top of already-tested code, not just the first time you write that code.

---

## Velocity rule dominated the "top triggered rules" chart — alert flapping

**Date:** 2026-08-25

**What broke:** After the calibration fix above, the pipeline ran cleanly (no more near-universal flagging), but the dashboard's "most frequently triggered rules" chart still showed `velocity` far ahead of the other rules (~750-800 triggers vs. ~150 for `amount_zscore`, out of 855 flagged events from 4,696 transactions). This wasn't the same bug — the threshold itself was right — something else was inflating the count.

**Root cause:** `rule_velocity()` re-evaluates the rolling window on *every* transaction and flags whenever `count > VELOCITY_MAX_TXNS`. That condition stays true for as long as the window stays busy — so one naturally busier-than-average minute (arrival rates are Poisson-distributed, so this happens on its own sometimes) didn't produce one alert, it produced a cascade: every single transaction for the rest of that busy stretch got flagged again, one after another, until the window quieted back down. Same failure shape as a monitoring alert that fires on every scrape while a metric is above threshold instead of once when it crosses — "alert flapping."

**Fix:** Added debounce/hysteresis to `rule_velocity()` in `consumer/detection.py`. The rule already computes `count` (transactions in the window, including the current one, since it's already been inserted by the time detection runs). `count - 1` is therefore the count of every *other* transaction in that same window — i.e. "how full was the window without this new arrival?" If that was already over the limit, this transaction is just one more on top of a streak that already got flagged, so it's suppressed. If it wasn't, this transaction is the one that tips the window over — the actual crossing point — so it's the only one that flags.

Verified against a real local Postgres instance with two scenarios: (1) a sustained burst of 15 transactions well past the limit, confirming exactly 1 flag fires (at the crossing transaction) instead of the ~7 that would have fired before; (2) a first burst, then a jump far enough forward that the window fully clears, then a second burst — confirming the fix re-arms correctly and isn't a one-shot "only ever flag once per user" hack, it's a real per-episode debounce.

**Honest limitation:** This is edge-detection against a single window snapshot, not fully stateful per-user hysteresis. If the count oscillates right at the boundary (9, 8, 9, 8...) it can still re-flag more than once, because each check only asks "was the window full without me," not "has this user been continuously over the limit since the last flag." A zero-flap version would need to persist per-user alert state (e.g. a `last_velocity_flag_at` column) and only re-arm once the count has stayed under the limit for a full window's worth of time. Didn't build that here — it's a stateful-column stretch goal for a problem this simpler, stateless check already solves for the case that actually mattered: a burst now produces exactly 1 flagged event instead of 5-8.

**Takeaway for interviews:** A good second half to the calibration bug above — that one was "pick the right threshold," this one is "a correct threshold can still misbehave if the *rule* doesn't distinguish between crossing a line and staying past it." It's the same pattern behind real alerting/monitoring systems needing debounce windows, and a good prompt to talk about the difference between a stateless heuristic (what's here) and a fully stateful one (what production-grade version would need), including naming the concrete trade-off instead of claiming the simple fix is a complete solution.

---

## First real end-to-end run — velocity rule flagged almost everything

**Date:** 2026-08-24

**What broke:** Running the full pipeline live for the first time (generator → Redpanda → consumer → detection), nearly every transaction for nearly every user got flagged with `velocity` as the reason, and the "N transactions in the last 10 min" count climbed continuously (into the 20s and 30s) instead of staying low and occasionally spiking. The z-score and category-novelty flags mixed in were all correct — real injected anomalies. Velocity was the only rule producing noise.

**Root cause:** A threshold that was never wrong in testing, but was never tested against the actual traffic volume it would face. Phase 2's isolated tests deliberately spaced synthetic transactions far apart (specifically to test each rule in isolation without cross-contamination — see that entry above), so the velocity rule was only ever exercised against hand-picked scenarios, never against the generator's real throughput. The generator publishes at roughly 1-2 transactions/second across a pool of just 25 users, chosen at random each time — which works out to each individual user accumulating around **40 transactions every 10 minutes from normal background activity alone**. The velocity threshold was 5 per 10 minutes. Ordinary, non-anomalous traffic was already 8x over the limit before any deliberate burst was injected.

**Fix:** Recalibrated using the actual numbers instead of a guessed threshold. With ~4 transactions/minute/user at baseline (Poisson-distributed, so occasional natural variance up to ~8-10 in a given minute), a **1-minute window with a threshold of 8** keeps normal noise below the line most of the time while a deliberate burst (5-8 transactions in well under a second, per the generator's burst injection) reliably pushes well past it. The general fix isn't "these exact numbers" — it's *always deriving a rate-based threshold from the actual expected base rate*, the same way you'd size an API rate limiter against real traffic, not intuition.

**Takeaway for interviews:** This is probably the single best bug to talk about from this whole project. It's not a coding mistake — the code did exactly what it was told. It's a calibration mistake, and specifically the kind that *passes every unit test* because unit tests exercise the rule in isolation, not against the real system's actual load characteristics. The fix generalizes well beyond this project: any rate/velocity-based threshold (API rate limits, alerting thresholds, autoscaling triggers) needs to be derived from measured real-world base rates, not chosen intuitively — and a rule that looks correct in isolated tests can still be wrong the moment it meets real traffic volume.

---

## Local setup (Windows/WSL2) — dependency broke on a newer Python than the build environment used

**Date:** 2026-08-23

**What broke:** `pip install -r requirements.txt` succeeded, but `import kafka` failed with `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`.

**Root cause:** `requirements.txt` pinned `kafka-python==2.0.2` (from 2021), which vendors an old copy of the `six` compatibility library using a module-aliasing trick that breaks under Python 3.12's import system. This project's own build/test environment happened to run Python 3.11, so the incompatibility never surfaced during my testing — it only showed up once run on a machine with Python 3.12 (Ubuntu 24.04's default, which is what WSL2 installs). This is a real gap in the testing described earlier in this log: "tested end-to-end" so far always meant *my* environment, not the one the project would actually be run in.

**Fix:** Confirmed via the project's GitHub issues/releases that this is a known, already-fixed problem — Python 3.12 support landed in `kafka-python` 2.0.3, and the package has since matured through the 2.x line (a 3.0.x rewrite also exists but changes the transport layer significantly, so it wasn't worth adopting here). Bumped the pin to `kafka-python==2.3.2` — same synchronous `KafkaProducer`/`KafkaConsumer` API this project uses, just with the Python 3.12 fix included. Verified via the package's own changelog/release history rather than guessing, since a plausible-sounding fix that isn't actually confirmed is worse than admitting uncertainty.

**Honest caveat:** This fix has NOT been re-verified end-to-end against a live Redpanda broker from this build environment (no Docker daemon available here, per the Phase 1 entry) — the version bump is based on documented Python 3.12 support and an unchanged public API within the 2.x series, not a rerun of the full pipeline. The generator/consumer actually running against Redpanda on your machine is the first real end-to-end confirmation.

**Takeaway for interviews:** A good, honest example of the limits of "tested end-to-end" — it's only ever tested end-to-end *in the environments it was tested in*. Python version mismatches between dev and deployment are a classic real-world source of "works on my machine," and pinning exact dependency versions (as this project does) makes the failure reproducible and diagnosable instead of mysterious, even though it doesn't prevent the mismatch itself.

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

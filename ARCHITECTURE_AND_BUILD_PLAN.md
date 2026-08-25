# Fraud/Anomaly Detection Pipeline — Architecture & Build Plan

## Goal

Build a small but real event-driven system that ingests a stream of transactions, flags ones that look anomalous based on a user's historical behavior, and surfaces the flagged events on a dashboard. The point isn't to build production-grade fraud detection — it's to build something small enough to finish, but with real architectural decisions you can defend in an interview: why a queue, why this detection approach, how you'd scale it, and what trade-offs you made.

## 1. System Overview

```
[Transaction Generator] --> [Message Queue] --> [Detection Consumer] --> [Postgres]
                                (Kafka/                (Python)              |
                                 Redpanda)                                   v
                                                                       [Dashboard API]
                                                                              |
                                                                              v
                                                                       [Dashboard UI]
```

**Components:**

1. **Transaction generator** — a script that simulates users making transactions (amount, merchant category, timestamp, location) at a configurable rate, occasionally injecting deliberately anomalous transactions so you have something to detect.
2. **Message queue** — Redpanda (Kafka-API-compatible, much lighter to run locally via Docker) decouples the generator from the detector, so you can talk about backpressure and consumer scaling even though it's a single-machine demo.
3. **Detection consumer** — a Python service that reads each transaction, maintains a rolling per-user profile (mean/stddev of transaction amount, typical merchant categories, typical times), and flags transactions that deviate significantly.
4. **Storage** — Postgres holding both raw transactions and flagged events, plus the per-user rolling stats.
5. **Dashboard** — a small FastAPI backend serving flagged events and summary stats, with a lightweight frontend (Streamlit is fastest to build, or a simple HTML/JS page if you want it to look more like a "real" web app on your resume).

## 2. Tech Stack (Python)

- **Queue:** Redpanda (via Docker Compose) + `confluent-kafka` or `kafka-python` client
- **Detection service:** plain Python (no heavy ML needed — this is a feature, not a limitation, see Section 4)
- **Database:** PostgreSQL + SQLAlchemy
- **API:** FastAPI
- **Dashboard:** Streamlit (fastest) or a small React/HTML page if you want more frontend polish for your resume
- **Orchestration:** Docker Compose to run everything with one command — this alone is a good interview talking point (service composition, environment config)

## 3. Data Model

**`transactions`**
| column | type | notes |
|---|---|---|
| id | UUID | |
| user_id | string | |
| amount | float | |
| merchant_category | string | e.g. "groceries", "electronics" |
| location | string | simplified — city or country |
| timestamp | datetime | |

**`user_profiles`** (rolling stats, updated incrementally)
| column | type | notes |
|---|---|---|
| user_id | string | |
| mean_amount | float | running mean |
| stddev_amount | float | running stddev |
| common_categories | list | top categories seen |
| txn_count | int | used to weight updates |

**`flagged_events`**
| column | type | notes |
|---|---|---|
| id | UUID | |
| transaction_id | UUID | FK |
| reason | string | e.g. "amount 4.2 stddev above user mean" |
| score | float | how anomalous, for ranking |
| flagged_at | datetime | |

## 4. Detection Approach (keep it simple and explainable)

Resist the urge to reach for a full ML model — a transparent rule-based/statistical approach is *better* for this project because you can explain exactly why each decision was made, which is what interviewers actually want to hear. Start with:

- **Z-score on amount:** flag if a transaction's amount is more than N standard deviations from that user's rolling mean (e.g., N=3).
- **Category novelty:** flag if the merchant category has never (or rarely) appeared in that user's history.
- **Velocity check:** flag if there are too many transactions from the same user in a short window (e.g., 5+ in 10 minutes) — classic card-testing fraud pattern.
- **Combine into a score:** each rule contributes to a score; flag if the combined score crosses a threshold. This gives you a natural interview answer for "how would you reduce false positives" — you'd tune per-rule weights and thresholds against labeled data.

Stretch goal (optional, only if time allows): swap the z-score rule for an actual lightweight model like Isolation Forest from scikit-learn, and compare its flags against your rule-based ones. This gives you a great "I evaluated two approaches and here's the trade-off" story without much extra work.

## 5. Build Plan (suggested order)

**Phase 1 — Skeleton (get something running end-to-end fast)**
1. Docker Compose file with Redpanda + Postgres.
2. Transaction generator that publishes fake transactions to the queue at a fixed rate.
3. A bare-bones consumer that just reads messages and writes them to Postgres (no detection logic yet). Confirm the whole pipe works before adding intelligence.

**Phase 2 — Detection logic**
4. Add the rolling user-profile table and the update logic (mean/stddev updated incrementally as each transaction arrives).
5. Implement the z-score rule, then category novelty, then velocity — add them one at a time so you can test each in isolation.
6. Write flagged events to the `flagged_events` table with a reason string.

**Phase 3 — Dashboard**
7. FastAPI endpoints: list recent flagged events, summary stats (flag rate over time, top reasons).
8. Streamlit or simple HTML page to visualize: a table of recent flags, and a chart of flag volume over time (this is where the `dataviz` skill-level polish helps if you want it to look sharp).

**Phase 4 — Polish for resume/interview**
9. Seed the generator with a few "attack scenarios" (a sudden high-value purchase, a burst of transactions, a new category) so you can demo detection working live.
10. Write a short README explaining the architecture, the trade-offs you made, and what you'd do differently at scale (this doubles as your interview prep notes).
11. (Optional stretch) Add the Isolation Forest comparison from Section 4, or add horizontal scaling by running multiple consumer instances in the same Kafka consumer group and showing partition-based load distribution.

## 6. Interview Talking Points to Prepare

- Why a message queue instead of direct calls from generator to detector (decoupling, buffering, replay).
- How you'd handle a consumer crashing mid-processing (offset commits, idempotency of writes).
- Why rule-based over ML first (explainability, no training data needed, easy to reason about false positives).
- How you'd scale detection to millions of users (partitioning by user_id so all of one user's transactions land on the same consumer, keeping the rolling state simple).
- How you'd reduce false positives in production (threshold tuning, human-in-the-loop review queue, feedback loop from confirmed fraud/not-fraud back into the rules).

## 7. Estimated Timeline

Roughly 1.5–2 weeks part-time: Phase 1 (2-3 days), Phase 2 (3-4 days), Phase 3 (2-3 days), Phase 4 (2-3 days, can be extended if you want the stretch goals).

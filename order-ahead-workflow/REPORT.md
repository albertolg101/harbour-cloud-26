# Order-Ahead Fulfillment Workflow — design report

## Engine choice & justification

**Temporal (Python SDK).** Temporal is the actively-developed successor to
Cadence (same model: durable, fault-oblivious workflow code), so it fits the
assignment while being easier to run and maintain.

Why Temporal over the alternatives:
- **vs Step Functions (ASL):** the order logic is genuinely branchy and
  long-lived (signals that change behaviour *depending on the current state*,
  several timers, compensation). Expressing that as real code is far clearer
  than a JSON state machine where the real logic gets pushed into Lambdas.
- **vs Cadence:** same programming model, but Temporal has a one-command local
  dev server, a built-in time-skipping test server, and a maintained Python SDK.
- **Why Python:** matches the rest of this course's code; the SDK runs workflows
  as plain modules (no bundler), so it is quick to run and to verify.

What the engine gives us vs what stays our job: Temporal gives **durability**
(state survives crashes/deploys via event-sourced history). **Idempotency and
business compensation are still ours** — the engine will happily replay a
workflow, but it won't un-charge a card for us.

## Determinism explanation

A Temporal workflow is replayed from its event history after any crash, deploy,
or worker restart: the engine re-runs the workflow function and expects it to
make the **exact same decisions** it made before, then continues from where it
left off. For that to hold, the workflow code must be **deterministic** — given
the same history it must do the same thing.

So in `workflows.py` we never do IO, never read the clock, and never use
randomness directly. Concretely:
- All side effects (payment, inventory, notifications, loyalty) are
  **activities**. Their results are recorded in history, so on replay they are
  *not* re-executed — the recorded result is returned. This is also why a crash
  can't double-charge: a completed payment activity is replayed from history,
  not re-run.
- All waiting uses **workflow timers / `wait_condition`**, not `time.sleep`. The
  timer firings and signal arrivals are events in history, reproduced identically.
- Timer durations are passed in the workflow **input** (`TimerConfig`), not read
  from `os.environ` or `datetime.now()`, so they are part of the deterministic
  record.

The payment **Idempotency-Key** is derived from the order id (`pay-<order_id>`),
so it is stable across activity retries *and* across worker crashes — the
Payments API dedupes, giving exactly one charge (scenarios F1 and F6).

## Compensation / cancellation policy

The workflow tracks two facts: `charged` and `inventory_reserved`. Compensation
runs only what actually happened, and every compensating activity is idempotent
(safe to re-run on replay):

| Situation | Compensation | Final state |
|-----------|--------------|-------------|
| Payment permanently declined (F2) | release inventory; notify; **no loyalty** | `FAILED` |
| Out of stock, substitution declined / no response by deadline (F3) | release inventory (+ refund if already charged) | `CANCELLED` |
| Cancel **before** drinks are made (F4) | refund + release inventory | `CANCELLED` |
| Cancel **after** drinks are made | escalate (waste policy), **no refund** — drinks already made | `CANCELLED` |
| Never collected (F5) | escalate (waste / refund policy) | `ABANDONED` |
| Brew SLA breached | escalate (comp / notify manager), then keep waiting | stays `BREWING` → `READY` |

Cancellation is modelled as a `cancel` **signal** plus a boolean the workflow
checks at each step and inside every `wait_condition` predicate, so a cancel is
honoured at the next safe checkpoint. The before-vs-after-made distinction is
exactly the point of F4: once we've consumed real resources (made the drinks)
the refund policy changes.

This is the **saga** pattern: a sequence of local steps, each with a
compensating action, trading ACID atomicity for availability — there is no
global lock across payment + inventory + barista.

## Failure scenarios (all verified in `demo.py`)

| # | Scenario | Result |
|---|----------|--------|
| F1 | Payment times out twice, then succeeds | retried with backoff; one charge; `COMPLETED` |
| F2 | Payment permanently declined | inventory released, `FAILED`, no loyalty |
| F3 | Out of stock | substitution offer; decline/timeout → `CANCELLED`; accept → `COMPLETED` |
| F4 | Cancel before drinks made | refund + release → `CANCELLED` |
| F5 | Never collected | pickup-expiry timer → `ABANDONED` |
| F6 | Worker crashes mid-order | resumes from history on restart; no double charge (see README) |

# Order-Ahead Fulfillment Workflow (Temporal, Python)

A durable workflow for the StarHarbour mobile order-ahead system: one workflow
instance is the single source of truth for an order from *placed* to
*collected* (or *cancelled/abandoned*), surviving worker crashes and deploys
without losing state or double-charging.

- `shared.py` — data types (input, state, timer config, test hooks)
- `activities.py` — all IO/side effects (payment with Idempotency-Key, inventory, notify, loyalty…)
- `workflows.py` — the durable workflow (signals, queries, timers, compensation)
- `worker.py` — hosts the workflow + activities
- `client.py` — CLI to start orders and send signals
- `demo.py` — automated run of every scenario (uses an ephemeral test server)
- `REPORT.md`, `STATE_DIAGRAM.md` — design report and state diagram

See `REPORT.md` for the engine choice, determinism explanation and
compensation policy.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick verification (no separate server needed)

Runs F1–F5 (and both substitution branches) end to end and asserts each final
state. Downloads an ephemeral Temporal test server on first run.

```bash
python demo.py
```

## Running the real engine + worker

```bash
# 1) start the Temporal dev server (Web UI at http://localhost:8233)
temporal server start-dev
#    install the CLI first if needed:
#      macOS:   brew install temporal
#      other:   download from github.com/temporalio/cli/releases

# 2) in another terminal, start the worker
python worker.py

# 3) drive scenarios from a third terminal with the client (see below)
```

Optional: set `PAYMENTS_URL=http://localhost:9091` so the payment activity calls
the real hw1 Payments API (with the Idempotency-Key) instead of simulating.

## Triggering each scenario with the client

`--fast` shrinks the timers (substitution / brew-SLA / pickup) to ~10s so the
timer-based scenarios finish quickly.

```bash
# Happy path
python client.py start order1
python client.py signal order1 ready       # barista marks ready
python client.py signal order1 collect     # customer collects  -> COMPLETED
python client.py status order1

# F1 — payment times out twice then succeeds (one charge), order proceeds
python client.py start order-f1 --scenario F1
python client.py signal order-f1 ready
python client.py signal order-f1 collect   # -> COMPLETED

# F2 — payment permanently declined
python client.py start order-f2 --scenario F2   # -> FAILED (inventory released, no loyalty)

# F3 — out of stock -> substitution offer
python client.py start order-f3 --scenario F3 --fast
python client.py signal order-f3 sub-decline    # -> CANCELLED (+ refund)
#   (or: python client.py signal order-f3 sub-accept ; then ready ; then collect -> COMPLETED)
#   (or: send nothing; after the deadline -> CANCELLED)

# F4 — cancel before drinks are made
python client.py start order-f4
python client.py signal order-f4 cancel     # -> CANCELLED (refund + release)

# F5 — customer never collects
python client.py start order-f5 --fast
python client.py signal order-f5 ready      # drinks made, then nobody collects
#   after the pickup-expiry timer -> ABANDONED   (check with: status order-f5)
```

## F6 — worker crash mid-order (durability)

```bash
# start an order and get it to the BREWING wait
python client.py start order-f6
python client.py status order-f6            # status=PAID/BREWING, payment=pay-order-f6

# kill the worker (Ctrl+C in its terminal), then restart it
python worker.py

# the workflow resumed from history — same payment id, no second charge
python client.py status order-f6
python client.py signal order-f6 ready
python client.py signal order-f6 collect    # -> COMPLETED
```

Because the completed payment activity is replayed from history (not re-run) and
the Idempotency-Key is stable, the restart never produces a second charge.

`demo_f6.py` automates exactly this against a running `temporal server start-dev`:
it drives one order to `BREWING`, kills worker A, starts worker B, finishes the
order, and asserts the payment activity ran **once** across the crash:

```bash
temporal server start-dev   # in one terminal
python demo_f6.py           # in another
```

No Temporal CLI installed? `python demo_f6_local.py` runs the same crash test
against an ephemeral local server (downloaded on first run, like `demo.py`).

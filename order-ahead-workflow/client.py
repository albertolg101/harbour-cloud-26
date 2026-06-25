"""CLI to start orders and drive the demo scenarios.

Usage:
  python client.py start <order_id> [--scenario happy|F1|F2|F3] [--fast]
  python client.py signal <order_id> <ready|collect|cancel|sub-accept|sub-decline>
  python client.py status <order_id>

--fast shrinks the timers (substitution/SLA/pickup) so F5 etc. happen in seconds.
"""
import asyncio
import os
import sys

from temporalio.client import Client

from shared import OrderInput, OrderItem, TimerConfig, TestHooks
from workflows import OrderWorkflow

TASK_QUEUE = "order-ahead"

SCENARIO_HOOKS = {
    "happy": TestHooks(),
    "F1": TestHooks(payment_timeouts=2),   # times out twice, then succeeds
    "F2": TestHooks(payment_declined=True),
    "F3": TestHooks(out_of_stock=True),
}


async def connect() -> Client:
    return await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))


async def cmd_start(order_id: str, scenario: str, fast: bool) -> None:
    timers = TimerConfig(substitution_deadline_secs=10, brew_sla_secs=10, pickup_expiry_secs=10) if fast else TimerConfig()
    order = OrderInput(
        order_id=order_id,
        store_id="store-london-01",
        items=[OrderItem("LATTE", 2), OrderItem("COLD_BREW", 1)],
        card_token="tok_saved_card",
        loyalty_card_id="loyalty-123",
        timers=timers,
        hooks=SCENARIO_HOOKS.get(scenario, TestHooks()),
    )
    client = await connect()
    handle = await client.start_workflow(
        OrderWorkflow.run, order, id=order_id, task_queue=TASK_QUEUE,
    )
    print(f"started order {order_id} (scenario {scenario}); workflow run {handle.first_execution_run_id}")


SIGNALS = {
    "ready": ("barista_ready", None),
    "collect": ("collect", None),
    "cancel": ("cancel", None),
    "sub-accept": ("substitution_response", True),
    "sub-decline": ("substitution_response", False),
}


async def cmd_signal(order_id: str, name: str) -> None:
    if name not in SIGNALS:
        sys.exit(f"unknown signal '{name}'; one of {list(SIGNALS)}")
    signal_name, arg = SIGNALS[name]
    client = await connect()
    handle = client.get_workflow_handle(order_id)
    if arg is None:
        await handle.signal(signal_name)
    else:
        await handle.signal(signal_name, arg)
    print(f"sent signal '{name}' to order {order_id}")


async def cmd_status(order_id: str) -> None:
    client = await connect()
    handle = client.get_workflow_handle(order_id)
    state = await handle.query(OrderWorkflow.status)
    print(f"order {order_id}: status={state.status} price={state.price} "
          f"payment={state.payment_id} note={state.note!r}")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    cmd = args[0]
    if cmd == "start":
        order_id = args[1]
        scenario = "happy"
        if "--scenario" in args:
            scenario = args[args.index("--scenario") + 1]
        asyncio.run(cmd_start(order_id, scenario, "--fast" in args))
    elif cmd == "signal":
        asyncio.run(cmd_signal(args[1], args[2]))
    elif cmd == "status":
        asyncio.run(cmd_status(args[1]))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()

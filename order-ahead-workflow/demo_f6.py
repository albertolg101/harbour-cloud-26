"""F6 — worker crash mid-order (durability).

Brings an order to the BREWING state (payment already taken), then kills the
worker and starts a fresh one. The order resumes from history with no lost
state, and the payment activity is NOT re-executed — proven by a counter that
stays at 1 across the restart.

Requires a running Temporal server:  temporal server start-dev
"""
import asyncio
import contextlib

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

import activities
from workflows import OrderWorkflow
from shared import OrderInput, OrderItem, PaymentRequest, TimerConfig, BREWING

TASK_QUEUE = "order-ahead-f6"

# Counts how many times the payment activity actually executes (across both
# worker lifetimes — same process, so the count survives the "restart").
charge_count = 0


@activity.defn(name="take_payment")
async def counting_take_payment(req: PaymentRequest) -> str:
    global charge_count
    charge_count += 1
    return await activities.take_payment(req)


ACTS = [
    activities.validate_and_price, activities.reserve_inventory,
    activities.release_inventory, counting_take_payment,
    activities.refund_payment, activities.notify_customer,
    activities.hand_to_barista, activities.accrue_loyalty,
    activities.escalate_order,
]


def new_worker(client: Client) -> Worker:
    return Worker(client, task_queue=TASK_QUEUE, workflows=[OrderWorkflow], activities=ACTS)


async def main() -> None:
    client = await Client.connect("localhost:7233")
    order = OrderInput(
        order_id="order-f6", store_id="store-london-01",
        items=[OrderItem("LATTE", 2), OrderItem("COLD_BREW", 1)],
        card_token="tok", loyalty_card_id="loyalty-123",
        timers=TimerConfig(),  # default brew SLA (10 min) -> it waits at BREWING
    )

    # clean any leftover run from a previous attempt (same workflow id)
    with contextlib.suppress(Exception):
        await client.get_workflow_handle("order-f6").terminate("reset for demo")

    print("1) starting worker A and the order...")
    worker_a = new_worker(client)
    task_a = asyncio.create_task(worker_a.run())
    handle = await client.start_workflow(
        OrderWorkflow.run, order, id="order-f6", task_queue=TASK_QUEUE,
    )

    # wait until payment is done and it is waiting for the barista
    for _ in range(100):
        s = (await handle.query(OrderWorkflow.status)).status
        if s == BREWING:
            break
        if s in ("FAILED", "CANCELLED", "ABANDONED"):
            raise RuntimeError(f"order ended early in {s} (expected to reach BREWING)")
        await asyncio.sleep(0.3)
    st = await handle.query(OrderWorkflow.status)
    print(f"   order reached {st.status}; payment={st.payment_id}; "
          f"payment executed {charge_count} time(s)")

    print("2) CRASHING worker A mid-order (kill)...")
    task_a.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task_a
    print("   worker A is down - order state lives on the Temporal server")

    await asyncio.sleep(1)

    print("3) restarting with a fresh worker B...")
    worker_b = new_worker(client)
    task_b = asyncio.create_task(worker_b.run())
    await asyncio.sleep(1)
    st = await handle.query(OrderWorkflow.status)
    print(f"   resumed: status={st.status}; payment={st.payment_id} "
          f"(same payment, replayed from history)")

    print("4) finishing the order (barista ready -> collect)...")
    await handle.signal("barista_ready")
    await asyncio.sleep(0.3)
    await handle.signal("collect")
    result = await handle.result()

    print(f"\n   final status: {result.status}; payment: {result.payment_id}")
    verdict = "NO double charge" if charge_count == 1 else f"BUG: charged {charge_count}x"
    print(f"   payment activity executed {charge_count} time(s) across the crash -> {verdict}")
    assert charge_count == 1 and result.status == "COMPLETED"
    print("\nF6 passed: order survived the worker crash with exactly one charge.")

    await worker_b.shutdown()
    with contextlib.suppress(asyncio.CancelledError):
        await task_b


if __name__ == "__main__":
    asyncio.run(main())

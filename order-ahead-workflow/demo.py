"""Automated scenario runner / verification.

Spins up an ephemeral Temporal server (downloaded on first run), runs the
worker in-process, and drives every failure scenario end to end, asserting the
final order state. This is the quickest way to verify the workflow; the README
shows how to run the same scenarios against a real `temporal server start-dev`.
"""
import asyncio

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import activities
from shared import (
    OrderInput, OrderItem, TimerConfig, TestHooks,
    COMPLETED, FAILED, CANCELLED, ABANDONED,
)
from workflows import OrderWorkflow

TASK_QUEUE = "order-ahead"
FAST = TimerConfig(substitution_deadline_secs=3, brew_sla_secs=3, pickup_expiry_secs=3)


def make_order(order_id: str, hooks: TestHooks) -> OrderInput:
    return OrderInput(
        order_id=order_id, store_id="store-london-01",
        items=[OrderItem("LATTE", 2), OrderItem("COLD_BREW", 1)],
        card_token="tok", loyalty_card_id="loyalty-123",
        timers=FAST, hooks=hooks,
    )


async def start(client: Client, order_id: str, hooks: TestHooks):
    return await client.start_workflow(
        OrderWorkflow.run, make_order(order_id, hooks),
        id=order_id, task_queue=TASK_QUEUE,
    )


def check(name: str, got: str, expected: str) -> None:
    ok = "PASS" if got == expected else "FAIL"
    print(f"  [{ok}] {name}: got {got}, expected {expected}")
    assert got == expected, f"{name}: expected {expected}, got {got}"


async def main() -> None:
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client, task_queue=TASK_QUEUE, workflows=[OrderWorkflow],
            activities=[
                activities.validate_and_price, activities.reserve_inventory,
                activities.release_inventory, activities.take_payment,
                activities.refund_payment, activities.notify_customer,
                activities.hand_to_barista, activities.accrue_loyalty,
                activities.escalate_order,
            ],
        ):
            c = env.client
            print("Running scenarios...\n")

            # Happy path: barista ready -> collected -> COMPLETED
            h = await start(c, "happy", TestHooks())
            await asyncio.sleep(0.5)
            await h.signal("barista_ready")
            await asyncio.sleep(0.3)
            await h.signal("collect")
            check("happy path", (await h.result()).status, COMPLETED)

            # F1: payment times out twice then succeeds -> still COMPLETED (one charge)
            h = await start(c, "F1", TestHooks(payment_timeouts=2))
            await asyncio.sleep(0.5)
            await h.signal("barista_ready")
            await asyncio.sleep(0.3)
            await h.signal("collect")
            check("F1 payment retried then succeeds", (await h.result()).status, COMPLETED)

            # F2: payment permanently declined -> FAILED
            h = await start(c, "F2", TestHooks(payment_declined=True))
            check("F2 payment declined", (await h.result()).status, FAILED)

            # F3a: out of stock, customer declines substitution -> CANCELLED
            h = await start(c, "F3-decline", TestHooks(out_of_stock=True))
            await asyncio.sleep(0.4)
            await h.signal("substitution_response", False)
            check("F3 substitution declined", (await h.result()).status, CANCELLED)

            # F3b: out of stock, customer accepts substitution -> COMPLETED
            h = await start(c, "F3-accept", TestHooks(out_of_stock=True))
            await asyncio.sleep(0.4)
            await h.signal("substitution_response", True)
            await asyncio.sleep(0.4)
            await h.signal("barista_ready")
            await asyncio.sleep(0.3)
            await h.signal("collect")
            check("F3 substitution accepted", (await h.result()).status, COMPLETED)

            # F4: cancel before drinks made -> CANCELLED (refund + release)
            h = await start(c, "F4", TestHooks())
            await asyncio.sleep(0.5)
            await h.signal("cancel")
            check("F4 cancel before made", (await h.result()).status, CANCELLED)

            # F5: never collected -> pickup expiry -> ABANDONED
            h = await start(c, "F5", TestHooks())
            await asyncio.sleep(0.5)
            await h.signal("barista_ready")  # drinks made, but never collected
            check("F5 never collected", (await h.result()).status, ABANDONED)

            print("\nAll scenarios passed.")


if __name__ == "__main__":
    asyncio.run(main())

"""Runs the worker: hosts the OrderWorkflow and its activities, polling the
'order-ahead' task queue. Kill it and restart it any time — in-flight orders
resume from history with no double charge (scenario F6)."""
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

import activities
from workflows import OrderWorkflow

TASK_QUEUE = "order-ahead"


async def main() -> None:
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[
            activities.validate_and_price,
            activities.reserve_inventory,
            activities.release_inventory,
            activities.take_payment,
            activities.refund_payment,
            activities.notify_customer,
            activities.hand_to_barista,
            activities.accrue_loyalty,
            activities.escalate_order,
        ],
    )
    print(f"worker polling task queue '{TASK_QUEUE}' (Ctrl+C to stop)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

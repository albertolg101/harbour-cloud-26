"""Run the F6 crash scenario against an ephemeral local server (no CLI needed).

Reuses demo_f6's counting payment activity + worker helpers, but swaps the
localhost connection for WorkflowEnvironment.start_local(). Verifies the order
survives a worker crash with exactly one charge.
"""
import asyncio
import contextlib

from temporalio.testing import WorkflowEnvironment

import demo_f6
from workflows import OrderWorkflow
from shared import OrderInput, OrderItem, TimerConfig, BREWING


async def main() -> None:
    async with await WorkflowEnvironment.start_local() as env:
        client = env.client
        order = OrderInput(
            order_id="order-f6", store_id="store-london-01",
            items=[OrderItem("LATTE", 2), OrderItem("COLD_BREW", 1)],
            card_token="tok", loyalty_card_id="loyalty-123",
            timers=TimerConfig(),  # default brew SLA (10 min) -> waits at BREWING
        )

        print("1) starting worker A and the order...")
        worker_a = demo_f6.new_worker(client)
        task_a = asyncio.create_task(worker_a.run())
        handle = await client.start_workflow(
            OrderWorkflow.run, order, id="order-f6", task_queue=demo_f6.TASK_QUEUE,
        )

        for _ in range(100):
            s = (await handle.query(OrderWorkflow.status)).status
            if s == BREWING:
                break
            if s in ("FAILED", "CANCELLED", "ABANDONED"):
                raise RuntimeError(f"order ended early in {s}")
            await asyncio.sleep(0.3)
        st = await handle.query(OrderWorkflow.status)
        print(f"   reached {st.status}; payment={st.payment_id}; "
              f"payment executed {demo_f6.charge_count} time(s)")

        print("2) CRASHING worker A...")
        task_a.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task_a

        await asyncio.sleep(1)

        print("3) restarting with fresh worker B...")
        worker_b = demo_f6.new_worker(client)
        task_b = asyncio.create_task(worker_b.run())
        await asyncio.sleep(1)
        st = await handle.query(OrderWorkflow.status)
        print(f"   resumed: status={st.status}; payment={st.payment_id}")

        print("4) finishing (ready -> collect)...")
        await handle.signal("barista_ready")
        await asyncio.sleep(0.3)
        await handle.signal("collect")
        result = await handle.result()

        print(f"\n   final: status={result.status}; payment={result.payment_id}")
        print(f"   payment executed {demo_f6.charge_count} time(s) across the crash")
        assert demo_f6.charge_count == 1, f"DOUBLE CHARGE: {demo_f6.charge_count}"
        assert result.status == "COMPLETED", result.status
        print("\nF6 PASS: survived worker crash with exactly one charge.")

        await worker_b.shutdown()
        with contextlib.suppress(asyncio.CancelledError):
            await task_b


if __name__ == "__main__":
    asyncio.run(main())

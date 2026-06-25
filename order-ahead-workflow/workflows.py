"""The durable Order workflow.

This is the single source of truth for an order's state. It coordinates
activities, reacts to external signals (barista ready, customer collect/cancel,
substitution response), drives timers (substitution deadline, brew SLA, pickup
expiry) and runs compensations (refund, release inventory) when things go wrong.

Determinism: this function may be replayed from history at any time (after a
crash or deploy), so it does no IO, no clocks, and no randomness directly — all
of that is in activities. Waiting is done with workflow timers, which are
recorded in history and reproduced exactly on replay.
"""
import asyncio
from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    import activities
    from shared import (
        OrderInput, OrderState, PaymentRequest, TestHooks,
        VALIDATED, AWAITING_SUBSTITUTION, INVENTORY_RESERVED, PAID,
        BREWING, READY, COLLECTED, COMPLETED, FAILED, CANCELLED, ABANDONED,
    )

RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=5,
)
ONCE = RetryPolicy(maximum_attempts=1)
ACT_TIMEOUT = timedelta(seconds=10)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.state = OrderState()
        self.cancelled = False
        self.collected = False
        self.barista_ready = False
        self.substitution_accepted: bool | None = None
        self.charged = False
        self.inventory_reserved = False

    # ---- signals & query --------------------------------------------------
    @workflow.signal(name="cancel")
    def cancel(self) -> None:
        self.cancelled = True

    @workflow.signal(name="barista_ready")
    def barista_ready_signal(self) -> None:
        self.barista_ready = True

    @workflow.signal(name="collect")
    def collect(self) -> None:
        self.collected = True

    @workflow.signal(name="substitution_response")
    def substitution_response(self, accept: bool) -> None:
        self.substitution_accepted = accept

    @workflow.query(name="status")
    def status(self) -> OrderState:
        return self.state

    # ---- main flow --------------------------------------------------------
    @workflow.run
    async def run(self, order: OrderInput) -> OrderState:
        async def notify(msg: str) -> None:
            await workflow.execute_activity(
                activities.notify_customer, args=[order.order_id, msg],
                start_to_close_timeout=ACT_TIMEOUT,
            )

        async def compensate(final_status: str, note: str) -> None:
            if self.charged and self.state.payment_id:
                await workflow.execute_activity(
                    activities.refund_payment, self.state.payment_id,
                    start_to_close_timeout=ACT_TIMEOUT, retry_policy=RETRY,
                )
            if self.inventory_reserved:
                await workflow.execute_activity(
                    activities.release_inventory, order.order_id,
                    start_to_close_timeout=ACT_TIMEOUT, retry_policy=RETRY,
                )
            await notify(note)
            self.state.status = final_status
            self.state.note = note

        # 1) validate & price
        self.state.price = await workflow.execute_activity(
            activities.validate_and_price, order,
            start_to_close_timeout=ACT_TIMEOUT, retry_policy=RETRY,
        )
        self.state.status = VALIDATED

        # 2) reserve inventory (with out-of-stock -> substitution offer)
        try:
            await workflow.execute_activity(
                activities.reserve_inventory, order,
                start_to_close_timeout=ACT_TIMEOUT, retry_policy=ONCE,
            )
            self.inventory_reserved = True
        except ActivityError:
            self.state.status = AWAITING_SUBSTITUTION
            await notify("An item is out of stock — accept a substitution?")
            try:
                await workflow.wait_condition(
                    lambda: self.substitution_accepted is not None or self.cancelled,
                    timeout=timedelta(seconds=order.timers.substitution_deadline_secs),
                )
            except asyncio.TimeoutError:
                pass
            if self.cancelled or self.substitution_accepted is not True:
                await compensate(CANCELLED, "no substitution response/declined -> cancelled + refunded")
                return self.state
            # accepted: reserve the substitute (force success by clearing hooks)
            await workflow.execute_activity(
                activities.reserve_inventory, replace(order, hooks=TestHooks()),
                start_to_close_timeout=ACT_TIMEOUT, retry_policy=ONCE,
            )
            self.inventory_reserved = True
        self.state.status = INVENTORY_RESERVED

        if self.cancelled:
            await compensate(CANCELLED, "cancelled before payment -> released inventory")
            return self.state

        # 3) take payment (retried with backoff, idempotent)
        try:
            self.state.payment_id = await workflow.execute_activity(
                activities.take_payment,
                PaymentRequest(
                    order_id=order.order_id, amount=self.state.price or 0.0,
                    currency=order.currency, store_id=order.store_id,
                    loyalty_card_id=order.loyalty_card_id,
                    idempotency_key=f"pay-{order.order_id}",
                    payment_timeouts=order.hooks.payment_timeouts,
                    payment_declined=order.hooks.payment_declined,
                ),
                start_to_close_timeout=ACT_TIMEOUT, retry_policy=RETRY,
            )
            self.charged = True
            self.state.status = PAID
        except ActivityError:
            await compensate(FAILED, "payment declined -> inventory released, no loyalty")
            return self.state

        await notify("Payment taken — we're making your drinks")

        # 4) hand to barista, wait for ready under a brew-SLA timer
        await workflow.execute_activity(
            activities.hand_to_barista, order.order_id, start_to_close_timeout=ACT_TIMEOUT,
        )
        self.state.status = BREWING
        try:
            await workflow.wait_condition(
                lambda: self.barista_ready or self.cancelled,
                timeout=timedelta(seconds=order.timers.brew_sla_secs),
            )
        except asyncio.TimeoutError:
            await workflow.execute_activity(
                activities.escalate_order, args=[order.order_id, "brew SLA breached"],
                start_to_close_timeout=ACT_TIMEOUT,
            )
            await workflow.wait_condition(lambda: self.barista_ready or self.cancelled)

        if self.cancelled:
            # cancel BEFORE drinks made -> full refund + release
            await compensate(CANCELLED, "cancelled before drinks made -> refunded + released")
            return self.state
        self.state.status = READY

        # 5) notify ready, wait for pickup under an expiry timer
        await notify("Ready for pickup!")
        try:
            await workflow.wait_condition(
                lambda: self.collected or self.cancelled,
                timeout=timedelta(seconds=order.timers.pickup_expiry_secs),
            )
        except asyncio.TimeoutError:
            # F5: never collected
            await workflow.execute_activity(
                activities.escalate_order,
                args=[order.order_id, "pickup expired -> waste/refund policy"],
                start_to_close_timeout=ACT_TIMEOUT,
            )
            self.state.status = ABANDONED
            self.state.note = "never collected -> abandoned (waste policy)"
            return self.state

        if self.cancelled:
            # cancel AFTER drinks made -> drinks wasted, no refund
            await workflow.execute_activity(
                activities.escalate_order,
                args=[order.order_id, "cancelled after drinks made -> waste policy"],
                start_to_close_timeout=ACT_TIMEOUT,
            )
            self.state.status = CANCELLED
            self.state.note = "cancelled after drinks made (waste policy, no refund)"
            return self.state
        self.state.status = COLLECTED

        # 6) accrue loyalty and close
        await workflow.execute_activity(
            activities.accrue_loyalty,
            args=[order.loyalty_card_id, int(self.state.price or 0)],
            start_to_close_timeout=ACT_TIMEOUT, retry_policy=RETRY,
        )
        self.state.status = COMPLETED
        return self.state

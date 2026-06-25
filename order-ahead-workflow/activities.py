"""Activities: the only place that does IO / side effects.

Everything that can fail, time out, or be slow lives here so the workflow stays
deterministic. The payment activity carries an Idempotency-Key so retries (and
worker crashes) never double-charge.
"""
import json
import os
from urllib import request, error

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared import OrderInput, PaymentRequest

KNOWN_COFFEE = {
    "ESPRESSO", "DOUBLE_ESPRESSO", "AMERICANO", "LATTE", "CAPPUCCINO",
    "FLAT_WHITE", "MOCHA", "CORTADO", "MACCHIATO", "COLD_BREW",
}
PRICES = {"ESPRESSO": 2.0, "LATTE": 3.5, "CAPPUCCINO": 3.2, "COLD_BREW": 4.1}
DEFAULT_PRICE = 3.5


@activity.defn
async def validate_and_price(order: OrderInput) -> float:
    total = 0.0
    for item in order.items:
        if item.coffee_type not in KNOWN_COFFEE:
            # bad data: retrying will never help
            raise ApplicationError(
                f"unknown coffee type: {item.coffee_type}", non_retryable=True
            )
        total += PRICES.get(item.coffee_type, DEFAULT_PRICE) * item.quantity
    activity.logger.info(f"priced order {order.order_id}: {total} {order.currency}")
    return round(total, 2)


@activity.defn
async def reserve_inventory(order: OrderInput) -> None:
    if order.hooks.out_of_stock:
        raise ApplicationError("item out of stock", type="OutOfStock", non_retryable=True)
    activity.logger.info(f"reserved inventory for {order.order_id}")


@activity.defn
async def release_inventory(order_id: str) -> None:
    # compensation — must be idempotent (safe to call even if nothing reserved)
    activity.logger.info(f"released inventory for {order_id}")


@activity.defn
async def take_payment(req: PaymentRequest) -> str:
    attempt = activity.info().attempt

    # F1: fail transiently a few times, then succeed (retryable error)
    if req.payment_timeouts and attempt <= req.payment_timeouts:
        raise RuntimeError(f"payment gateway timeout (attempt {attempt})")

    # F2: permanent decline (non-retryable)
    if req.payment_declined:
        raise ApplicationError("payment declined", type="PaymentDeclined", non_retryable=True)

    payments_url = os.getenv("PAYMENTS_URL")
    if payments_url:
        # Real hw1 Payments API. The Idempotency-Key makes this exactly-once
        # even though the activity may run more than once.
        body = json.dumps({
            "coffeeType": "LATTE", "price": req.amount,
            "currency": req.currency, "loyaltyCardId": req.loyalty_card_id,
        }).encode()
        http_req = request.Request(
            f"{payments_url.rstrip('/')}/api/v1/payments", data=body, method="POST",
            headers={
                "Content-Type": "application/json",
                "Store-Id": req.store_id,
                "Idempotency-Key": req.idempotency_key,
            },
        )
        try:
            with request.urlopen(http_req, timeout=5) as resp:
                payment_id = json.loads(resp.read()).get("paymentId", req.idempotency_key)
        except error.HTTPError as e:
            if 400 <= e.code < 500 and e.code != 429:
                raise ApplicationError(f"payment rejected: HTTP {e.code}", non_retryable=True)
            raise RuntimeError(f"payment HTTP {e.code}")  # retryable
        activity.logger.info(f"charged order {req.order_id} -> {payment_id}")
        return payment_id

    # Simulated charge (default for demos): the idempotency key IS the payment id.
    activity.logger.info(f"charged order {req.order_id} (simulated, attempt {attempt})")
    return req.idempotency_key


@activity.defn
async def refund_payment(payment_id: str) -> None:
    # compensation — idempotent
    activity.logger.info(f"refunded payment {payment_id}")


@activity.defn
async def notify_customer(order_id: str, message: str) -> None:
    activity.logger.info(f"[push -> customer of {order_id}] {message}")


@activity.defn
async def hand_to_barista(order_id: str) -> None:
    activity.logger.info(f"ticket {order_id} handed to barista queue")


@activity.defn
async def accrue_loyalty(loyalty_card_id: str, points: int) -> None:
    activity.logger.info(f"accrued {points} loyalty points to {loyalty_card_id}")


@activity.defn
async def escalate_order(order_id: str, reason: str) -> None:
    activity.logger.info(f"[ESCALATION] order {order_id}: {reason}")

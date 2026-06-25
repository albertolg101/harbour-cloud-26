"""Shared data types for the order-ahead workflow.

Kept free of any non-deterministic / IO code so it can be imported by both the
workflow (deterministic sandbox) and the activities/client.
"""
from dataclasses import dataclass, field


# Order lifecycle states (the workflow is the single source of truth).
PLACED = "PLACED"
VALIDATED = "VALIDATED"
AWAITING_SUBSTITUTION = "AWAITING_SUBSTITUTION"
INVENTORY_RESERVED = "INVENTORY_RESERVED"
PAID = "PAID"
BREWING = "BREWING"
READY = "READY"
COLLECTED = "COLLECTED"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
ABANDONED = "ABANDONED"


@dataclass
class TimerConfig:
    """Durations the workflow waits on. Passed in the input so they are part of
    the (deterministic) workflow history; the demo uses short values."""
    substitution_deadline_secs: int = 30
    brew_sla_secs: int = 600       # 10 min
    pickup_expiry_secs: int = 1800  # 30 min


@dataclass
class TestHooks:
    """Knobs to force the graded failure scenarios deterministically."""
    payment_timeouts: int = 0       # F1: this many transient failures, then success
    payment_declined: bool = False  # F2: permanent decline
    out_of_stock: bool = False      # F3: reservation fails -> substitution offer


@dataclass
class OrderItem:
    coffee_type: str
    quantity: int


@dataclass
class OrderInput:
    order_id: str
    store_id: str
    items: list[OrderItem]
    card_token: str
    loyalty_card_id: str
    currency: str = "EUR"
    timers: TimerConfig = field(default_factory=TimerConfig)
    hooks: TestHooks = field(default_factory=TestHooks)


@dataclass
class PaymentRequest:
    order_id: str
    amount: float
    currency: str
    store_id: str
    loyalty_card_id: str
    idempotency_key: str  # stable per order -> exactly-once charge across retries
    payment_timeouts: int = 0
    payment_declined: bool = False


@dataclass
class OrderState:
    status: str = PLACED
    price: float | None = None
    payment_id: str | None = None
    note: str = ""

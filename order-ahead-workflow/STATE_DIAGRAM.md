# Order workflow — state diagram

```mermaid
stateDiagram-v2
    [*] --> PLACED
    PLACED --> VALIDATED: validate & price
    VALIDATED --> FAILED: unknown coffee / store closed

    VALIDATED --> INVENTORY_RESERVED: reserve ok
    VALIDATED --> AWAITING_SUBSTITUTION: out of stock (F3)
    AWAITING_SUBSTITUTION --> INVENTORY_RESERVED: customer accepts
    AWAITING_SUBSTITUTION --> CANCELLED: declines / deadline (refund + release)

    INVENTORY_RESERVED --> CANCELLED: cancel signal (F4) / refund + release
    INVENTORY_RESERVED --> PAID: payment ok (retried, idempotent — F1)
    INVENTORY_RESERVED --> FAILED: payment declined (F2) / release inventory

    PAID --> BREWING: hand to barista
    BREWING --> BREWING: brew-SLA timer -> escalate, keep waiting
    BREWING --> CANCELLED: cancel before made (F4) / refund + release
    BREWING --> READY: barista_ready signal

    READY --> COLLECTED: collect signal
    READY --> ABANDONED: pickup-expiry timer (F5) / waste policy
    READY --> CANCELLED: cancel after made / waste policy, no refund

    COLLECTED --> COMPLETED: accrue loyalty

    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
    ABANDONED --> [*]
```

**Signals** (external events): `cancel`, `barista_ready`, `collect`,
`substitution_response(accept)`.
**Timers**: substitution deadline, brew SLA (escalate), pickup expiry (abandon).
**Query**: `status` returns the current `OrderState` at any time.

Terminal states: `COMPLETED`, `FAILED`, `CANCELLED`, `ABANDONED`.

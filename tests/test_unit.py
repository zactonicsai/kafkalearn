#!/usr/bin/env python3
"""
Offline unit tests for the store engine — no Kafka or HTTP required.
Run:  python3 tests/test_unit.py   (from repo root)
"""
import importlib
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

import store_state  # noqa: E402

failures = 0


def expect(name, cond):
    global failures
    print(f"  [{'ok' if cond else 'XX'}] {name}")
    if not cond:
        failures += 1


def fresh():
    importlib.reload(store_state)
    s = store_state.store
    s.emit = lambda t, p, k: s.handle(t, p)  # synchronous cascade
    return s


print("== unit: sale ==")
s = fresh()
s.handle("grocery.sales", {"event_type": "sale",
         "payload": {"sku": "MILK-001", "qty": 5}, "ts": 0})
snap = s.snapshot()
milk = next(p for p in snap["products"] if p["sku"] == "MILK-001")
expect("inventory decremented", milk["qty"] == 35)
expect("revenue booked", abs(snap["revenue"] - 5 * 3.49) < 0.01)
expect("profit booked", abs(snap["profit"] - 5 * (3.49 - 2.10)) < 0.01)

print("== unit: cascade to shipment ==")
s = fresh()
s.handle("grocery.sales", {"event_type": "sale",
         "payload": {"sku": "COFFEE-005", "qty": 12}, "ts": 0})
snap = s.snapshot()
expect("reorder created", any(o["sku"] == "COFFEE-005" for o in snap["pending_orders"]))
expect("shipment created", any(x["sku"] == "COFFEE-005" for x in snap["shipments"]))

print("== unit: delivery restock ==")
trace = list(s.shipments.keys())[0]
before = s.inventory["COFFEE-005"]
s.handle("grocery.logistics", {"event_type": "shipment_update",
         "payload": {"trace": trace, "sku": "COFFEE-005", "qty": 20,
                     "status": "delivered"}, "ts": 0})
expect("restocked", s.inventory["COFFEE-005"] == before + 20)
expect("order cleared", not s.snapshot()["pending_orders"])

print("== unit: employee ==")
s = fresh()
s.handle("grocery.employee", {"event_type": "clock_in",
         "payload": {"employee_id": "E9", "name": "A", "register": 1}, "ts": 0})
expect("clocked in", s.snapshot()["employees"][0]["status"] == "in")

print(f"\n{'PASSED' if failures == 0 else f'{failures} FAILURES'}")
sys.exit(1 if failures else 0)

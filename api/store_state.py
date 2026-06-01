"""
Store state engine.

A single source of truth that the Kafka consumer feeds. Each incoming event
mutates state and may emit downstream events (the cascade that makes the demo
feel alive):

  SALE        -> decrement inventory, add revenue/profit, maybe emit INVENTORY low
  INVENTORY   -> if below reorder point, emit ORDERING request
  ORDERING    -> emit VENDOR purchase order
  VENDOR      -> emit LOGISTICS shipment
  LOGISTICS   -> on 'delivered', restock INVENTORY + emit SHELF placement
  EMPLOYEE    -> track who is clocked in / register access
  OWNER       -> rollup feed (consumes the summary)

State is in-memory (demo). In production this would be a DB / KTable.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections import deque

from domain import CATALOG, CATALOG_BY_SKU, Topic


class StoreState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        # inventory qty per sku, seeded at 2x reorder so there is headroom
        self.inventory = {p["sku"]: p["reorder"] * 2 for p in CATALOG}
        self.revenue = 0.0
        self.profit = 0.0
        self.units_sold = 0
        self.sales_by_sku: dict[str, int] = {p["sku"]: 0 for p in CATALOG}
        self.shelf = {p["sku"]: p["shelf"] for p in CATALOG}
        self.employees: dict[str, dict] = {}
        self.pending_orders: dict[str, dict] = {}   # sku -> order
        self.shipments: dict[str, dict] = {}        # trace -> shipment
        self.flow = deque(maxlen=60)                 # recent event flow for UI
        self.emit = None  # injected publish callback (topic, payload, key)

    # ---------------------------------------------------------------- helpers
    def _record_flow(self, frm: str, to: str, label: str) -> None:
        self.flow.append({"from": frm, "to": to, "label": label,
                          "ts": time.time()})

    def snapshot(self) -> dict:
        with self.lock:
            margin = (self.profit / self.revenue * 100.0) if self.revenue else 0.0
            products = []
            for p in CATALOG:
                sku = p["sku"]
                qty = self.inventory[sku]
                status = ("OUT" if qty <= 0 else
                          "LOW" if qty <= p["reorder"] else "OK")
                products.append({
                    **p,
                    "qty": qty,
                    "status": status,
                    "sold": self.sales_by_sku[sku],
                    "unit_profit": round(p["price"] - p["cost"], 2),
                    "shelf_current": self.shelf[sku],
                })
            return {
                "revenue": round(self.revenue, 2),
                "profit": round(self.profit, 2),
                "margin_pct": round(margin, 1),
                "units_sold": self.units_sold,
                "products": products,
                "employees": list(self.employees.values()),
                "pending_orders": list(self.pending_orders.values()),
                "shipments": list(self.shipments.values()),
                "flow": list(self.flow),
            }

    # ---------------------------------------------------------------- handlers
    def handle(self, topic: str, evt: dict) -> None:
        et = evt.get("event_type")
        p = evt.get("payload", {})
        if topic == Topic.SALES.value and et == "sale":
            self._on_sale(p)
        elif topic == Topic.INVENTORY.value and et == "stock_check":
            self._on_stock_check(p)
        elif topic == Topic.ORDERING.value and et == "reorder_request":
            self._on_reorder(p)
        elif topic == Topic.VENDOR.value and et == "purchase_order":
            self._on_vendor_po(p)
        elif topic == Topic.LOGISTICS.value and et == "shipment_update":
            self._on_logistics(p)
        elif topic == Topic.SHELF.value and et == "placement":
            self._on_shelf(p)
        elif topic == Topic.EMPLOYEE.value:
            self._on_employee(et, p)

    def _on_sale(self, p: dict) -> None:
        sku = p["sku"]; qty = int(p.get("qty", 1))
        prod = CATALOG_BY_SKU.get(sku)
        if not prod:
            return
        with self.lock:
            avail = self.inventory[sku]
            sold = min(qty, max(avail, 0))
            self.inventory[sku] -= sold
            self.units_sold += sold
            self.sales_by_sku[sku] += sold
            self.revenue += sold * prod["price"]
            self.profit += sold * (prod["price"] - prod["cost"])
            self._record_flow("Sales", "Inventory", f"-{sold} {sku}")
            low = self.inventory[sku] <= prod["reorder"]
        # emit owner rollup + maybe a stock check downstream
        if self.emit:
            self.emit(Topic.INVENTORY.value,
                      {"event_type": "stock_check", "topic": Topic.INVENTORY.value,
                       "payload": {"sku": sku, "qty": self.inventory[sku]},
                       "ts": time.time()}, sku)
            self.emit(Topic.OWNER.value,
                      {"event_type": "rollup", "topic": Topic.OWNER.value,
                       "payload": {"revenue": round(self.revenue, 2),
                                   "profit": round(self.profit, 2)},
                       "ts": time.time()}, None)

    def _on_stock_check(self, p: dict) -> None:
        sku = p["sku"]; prod = CATALOG_BY_SKU.get(sku)
        if not prod:
            return
        with self.lock:
            self._record_flow("Inventory", "Inventory", f"check {sku}")
            need = self.inventory[sku] <= prod["reorder"] and sku not in self.pending_orders
        if need and self.emit:
            self._record_flow("Inventory", "Ordering", f"low {sku}")
            self.emit(Topic.ORDERING.value,
                      {"event_type": "reorder_request", "topic": Topic.ORDERING.value,
                       "payload": {"sku": sku, "qty": prod["reorder"] * 2,
                                   "vendor": prod["vendor"]},
                       "ts": time.time()}, sku)

    def _on_reorder(self, p: dict) -> None:
        sku = p["sku"]
        with self.lock:
            self.pending_orders[sku] = {"sku": sku, "qty": p["qty"],
                                        "vendor": p["vendor"], "status": "ordered"}
            self._record_flow("Ordering", "Vendor", f"PO {sku}")
        if self.emit:
            self.emit(Topic.VENDOR.value,
                      {"event_type": "purchase_order", "topic": Topic.VENDOR.value,
                       "payload": {"sku": sku, "qty": p["qty"],
                                   "vendor": p["vendor"]}, "ts": time.time()}, sku)

    def _on_vendor_po(self, p: dict) -> None:
        sku = p["sku"]; trace = str(uuid.uuid4())[:8]
        with self.lock:
            self.shipments[trace] = {"trace": trace, "sku": sku, "qty": p["qty"],
                                     "vendor": p["vendor"], "status": "in_transit"}
            self._record_flow("Vendor", "Logistics", f"ship {sku}")
        if self.emit:
            self.emit(Topic.LOGISTICS.value,
                      {"event_type": "shipment_update", "topic": Topic.LOGISTICS.value,
                       "payload": {"trace": trace, "sku": sku, "qty": p["qty"],
                                   "status": "in_transit"}, "ts": time.time()}, trace)

    def _on_logistics(self, p: dict) -> None:
        trace = p["trace"]; sku = p["sku"]; status = p["status"]
        with self.lock:
            if trace in self.shipments:
                self.shipments[trace]["status"] = status
            self._record_flow("Logistics", "Logistics", f"{status} {sku}")
            if status == "delivered":
                self.inventory[sku] += int(p["qty"])
                self.pending_orders.pop(sku, None)
                self.shipments.pop(trace, None)
                self._record_flow("Logistics", "Inventory", f"+{p['qty']} {sku}")
        if status == "delivered" and self.emit:
            self.emit(Topic.SHELF.value,
                      {"event_type": "placement", "topic": Topic.SHELF.value,
                       "payload": {"sku": sku, "shelf": CATALOG_BY_SKU[sku]["shelf"]},
                       "ts": time.time()}, sku)

    def _on_shelf(self, p: dict) -> None:
        with self.lock:
            self.shelf[p["sku"]] = p["shelf"]
            self._record_flow("Shelf", "Shelf", f"{p['sku']}->{p['shelf']}")

    def _on_employee(self, et: str, p: dict) -> None:
        eid = p.get("employee_id", "?")
        with self.lock:
            self.employees[eid] = {"employee_id": eid, "name": p.get("name", eid),
                                   "role": p.get("role", "clerk"),
                                   "status": "in" if et == "clock_in" else "out",
                                   "register": p.get("register")}
            self._record_flow("Employee", "Register",
                              f"{eid} {et.replace('clock_', '')}")


store = StoreState()

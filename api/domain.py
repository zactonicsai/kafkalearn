"""
Domain model for the grocery event system.

The store is modeled as a set of business domains that each own a Kafka topic.
Events flow between them: a SALE drains INVENTORY, INVENTORY low-stock triggers
ORDERING, ORDERING confirms create VENDOR deliveries, deliveries move through
LOGISTICS, and arrivals restock INVENTORY + update SHELF placement. EMPLOYEE
events gate register access, and OWNER consumes everything for profit tracking.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Topic(str, Enum):
    SALES = "grocery.sales"
    INVENTORY = "grocery.inventory"
    SHELF = "grocery.shelf"
    EMPLOYEE = "grocery.employee"
    OWNER = "grocery.owner"
    VENDOR = "grocery.vendor"
    LOGISTICS = "grocery.logistics"
    ORDERING = "grocery.ordering"

    @classmethod
    def all(cls) -> list[str]:
        return [t.value for t in cls]


# Topics are created with replication so that either broker can serve them if
# the other dies. With a 2-broker cluster we use RF=2, min.insync.replicas=1
# so a single surviving broker keeps producers and consumers alive.
TOPIC_CONFIG = {
    Topic.SALES.value: {"partitions": 3, "replication": 2},
    Topic.INVENTORY.value: {"partitions": 3, "replication": 2},
    Topic.SHELF.value: {"partitions": 1, "replication": 2},
    Topic.EMPLOYEE.value: {"partitions": 1, "replication": 2},
    Topic.OWNER.value: {"partitions": 1, "replication": 2},
    Topic.VENDOR.value: {"partitions": 2, "replication": 2},
    Topic.LOGISTICS.value: {"partitions": 2, "replication": 2},
    Topic.ORDERING.value: {"partitions": 2, "replication": 2},
}


# Seed product catalog. cost = what the store pays the vendor, price = retail.
# profit per unit = price - cost. shelf is the physical location code.
CATALOG = [
    {"sku": "MILK-001", "name": "Whole Milk 1gal", "cost": 2.10, "price": 3.49,
     "shelf": "D1-A", "reorder": 20, "vendor": "DairyCo"},
    {"sku": "BREAD-002", "name": "Sourdough Loaf", "cost": 1.05, "price": 2.99,
     "shelf": "B2-C", "reorder": 15, "vendor": "BakeHouse"},
    {"sku": "EGGS-003", "name": "Eggs Dozen", "cost": 1.80, "price": 3.29,
     "shelf": "D1-B", "reorder": 25, "vendor": "DairyCo"},
    {"sku": "APPLE-004", "name": "Gala Apples lb", "cost": 0.60, "price": 1.49,
     "shelf": "P1-A", "reorder": 40, "vendor": "FreshFarms"},
    {"sku": "COFFEE-005", "name": "Ground Coffee 12oz", "cost": 4.20, "price": 8.99,
     "shelf": "G3-D", "reorder": 10, "vendor": "BeanSupply"},
    {"sku": "SODA-006", "name": "Cola 12pk", "cost": 3.50, "price": 6.49,
     "shelf": "G1-A", "reorder": 30, "vendor": "BevDist"},
    {"sku": "CHKN-007", "name": "Chicken Breast lb", "cost": 2.40, "price": 4.99,
     "shelf": "M1-A", "reorder": 18, "vendor": "MeatWorks"},
    {"sku": "RICE-008", "name": "White Rice 5lb", "cost": 3.10, "price": 5.99,
     "shelf": "G2-B", "reorder": 12, "vendor": "GrainCo"},
]

CATALOG_BY_SKU = {p["sku"]: p for p in CATALOG}


@dataclass
class EventEnvelope:
    """Every message on every topic uses this envelope so consumers can
    route generically and the frontend flow view can render uniformly."""
    event_type: str
    topic: str
    payload: dict
    ts: float
    source: str = "api"
    trace_id: str | None = None

"""
FastAPI application.

Responsibilities:
  * expose REST endpoints the frontend calls to inject simulated events
  * publish those events to Kafka (failover-aware gateway)
  * run a background consumer that tails ALL topics and feeds StoreState
  * serve the snapshot (inventory / profit / shelf / tracking) for the dashboard
  * serve the static frontend
  * report cluster health for the outage banner

Run:  uvicorn main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
import os
import random
import time

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from domain import CATALOG, CATALOG_BY_SKU, Topic
from kafka_gateway import gateway
from store_state import store

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

app = FastAPI(title="Grocery Kafka Demo API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

FRONTEND_DIR = os.getenv("FRONTEND_DIR", "/app/frontend")


# ---- request models --------------------------------------------------------
class SaleReq(BaseModel):
    sku: str
    qty: int = 1


class EmployeeReq(BaseModel):
    employee_id: str
    name: str = ""
    role: str = "clerk"
    register_no: int | None = None    # 'register' shadows BaseModel internals
    action: str = "clock_in"          # clock_in | clock_out

    def to_payload(self) -> dict:
        return {"employee_id": self.employee_id, "name": self.name,
                "role": self.role, "register": self.register_no}


class ShelfReq(BaseModel):
    sku: str
    shelf: str


class DeliverReq(BaseModel):
    trace: str


# ---- helpers ---------------------------------------------------------------
def _publish(topic: str, payload: dict, key: str | None) -> dict:
    try:
        return gateway.publish(topic, payload, key)
    except Exception as e:
        raise HTTPException(status_code=503,
                            detail=f"Kafka unavailable: {e}") from e


def _evt(event_type: str, topic: str, payload: dict) -> dict:
    return {"event_type": event_type, "topic": topic,
            "payload": payload, "ts": time.time(), "source": "api"}


# ---- lifecycle -------------------------------------------------------------
@app.on_event("startup")
def startup() -> None:
    # wire the store's downstream emitter to the kafka gateway
    store.emit = lambda topic, payload, key: _safe_emit(topic, payload, key)
    gateway.ensure_topics()
    gateway.tail(Topic.all(), group="grocery-api", on_message=store.handle)
    log.info("API started; tailing %s", Topic.all())


def _safe_emit(topic: str, payload: dict, key: str | None) -> None:
    try:
        gateway.publish(topic, payload, key)
    except Exception as e:
        log.warning("downstream emit failed (%s): %s", topic, e)


# ---- health & catalog ------------------------------------------------------
@app.get("/api/health")
def health() -> dict:
    return {"status": "ok" if gateway.connected else "degraded",
            "kafka_connected": gateway.connected,
            "last_error": gateway.last_error,
            "bootstrap": os.getenv("KAFKA_BOOTSTRAP", "kafka1:19092,kafka2:19092")}


@app.get("/api/catalog")
def catalog() -> dict:
    return {"products": CATALOG}


@app.get("/api/state")
def state() -> dict:
    return store.snapshot()


# ---- event injection -------------------------------------------------------
@app.post("/api/sale")
def post_sale(req: SaleReq) -> dict:
    if req.sku not in CATALOG_BY_SKU:
        raise HTTPException(404, "unknown sku")
    meta = _publish(Topic.SALES.value,
                    _evt("sale", Topic.SALES.value,
                         {"sku": req.sku, "qty": req.qty}), req.sku)
    return {"published": meta}


@app.post("/api/employee")
def post_employee(req: EmployeeReq) -> dict:
    et = "clock_in" if req.action == "clock_in" else "clock_out"
    meta = _publish(Topic.EMPLOYEE.value,
                    _evt(et, Topic.EMPLOYEE.value, req.to_payload()),
                    req.employee_id)
    return {"published": meta}


@app.post("/api/shelf")
def post_shelf(req: ShelfReq) -> dict:
    meta = _publish(Topic.SHELF.value,
                    _evt("placement", Topic.SHELF.value, req.model_dump()),
                    req.sku)
    return {"published": meta}


@app.post("/api/deliver")
def post_deliver(req: DeliverReq) -> dict:
    """Force a pending shipment to 'delivered' to complete the cascade."""
    ship = store.shipments.get(req.trace)
    if not ship:
        raise HTTPException(404, "unknown trace")
    meta = _publish(Topic.LOGISTICS.value,
                    _evt("shipment_update", Topic.LOGISTICS.value,
                         {"trace": req.trace, "sku": ship["sku"],
                          "qty": ship["qty"], "status": "delivered"}),
                    req.trace)
    return {"published": meta}


@app.post("/api/simulate")
def simulate(n: int = 12) -> dict:
    """Fire a burst of randomized sales to drive the whole cascade."""
    out = []
    for _ in range(max(1, min(n, 100))):
        prod = random.choice(CATALOG)
        qty = random.randint(1, 5)
        out.append(_publish(Topic.SALES.value,
                            _evt("sale", Topic.SALES.value,
                                 {"sku": prod["sku"], "qty": qty}), prod["sku"]))
        time.sleep(0.02)
    return {"count": len(out)}


# ---- static frontend (mounted last) ---------------------------------------
if os.path.isdir(FRONTEND_DIR):
    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

    app.mount("/", StaticFiles(directory=FRONTEND_DIR), name="static")

"""
Observability layer.

Two jobs:
  1. Prometheus metrics — counters/gauges Prometheus scrapes at /metrics, so
     Grafana can chart produced vs consumed message rates, errors, and lag-ish
     signals alongside the JVM/Kafka metrics from the broker exporters.
  2. In-memory ring buffers the dashboard reads directly (no Prometheus needed
     for the live UI): a per-topic sent/received tracker, a recent-message feed,
     and a rolling application log the "View logs" panel renders.

Everything here is process-local; in production the buffers would be a real log
sink and the counters would be the single source the UI also reads from
Prometheus. For the demo this keeps the dashboard instant and dependency-free
while still feeding Grafana.
"""
from __future__ import annotations

import logging
import time
from collections import deque, defaultdict
from threading import Lock

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ---- Prometheus metrics ----------------------------------------------------
MSG_SENT = Counter("freshchain_messages_sent_total",
                   "Messages produced to Kafka", ["topic"])
MSG_RECV = Counter("freshchain_messages_received_total",
                   "Messages consumed from Kafka", ["topic"])
MSG_ERRORS = Counter("freshchain_publish_errors_total",
                     "Failed publish attempts", ["topic"])
PUBLISH_LATENCY = Histogram("freshchain_publish_seconds",
                            "Publish round-trip latency", ["topic"])
KAFKA_UP = Gauge("freshchain_kafka_connected",
                 "1 if the API currently has a live Kafka connection")
INVENTORY_QTY = Gauge("freshchain_inventory_qty", "On-hand quantity", ["sku"])
REVENUE = Gauge("freshchain_revenue_dollars", "Cumulative revenue")
PROFIT = Gauge("freshchain_profit_dollars", "Cumulative profit")


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST


# ---- dashboard buffers -----------------------------------------------------
class Tracker:
    """Live message tracker + log buffer the dashboard reads via /api/metrics
    and /api/logs."""

    def __init__(self) -> None:
        self.lock = Lock()
        self.sent: dict[str, int] = defaultdict(int)
        self.received: dict[str, int] = defaultdict(int)
        self.recent = deque(maxlen=100)   # recent messages (both directions)
        self.logs = deque(maxlen=300)     # app log lines for the UI
        self.started = time.time()

    def on_sent(self, topic: str, key: str | None = None) -> None:
        MSG_SENT.labels(topic=topic).inc()
        with self.lock:
            self.sent[topic] += 1
            self.recent.appendleft({"dir": "sent", "topic": topic,
                                    "key": key, "ts": time.time()})

    def on_received(self, topic: str, key: str | None = None) -> None:
        MSG_RECV.labels(topic=topic).inc()
        with self.lock:
            self.received[topic] += 1
            self.recent.appendleft({"dir": "received", "topic": topic,
                                    "key": key, "ts": time.time()})

    def on_error(self, topic: str) -> None:
        MSG_ERRORS.labels(topic=topic).inc()
        self.log("ERROR", f"publish failed on {topic}")

    def log(self, level: str, msg: str) -> None:
        with self.lock:
            self.logs.appendleft({"level": level, "msg": msg, "ts": time.time()})

    def snapshot(self) -> dict:
        with self.lock:
            total_sent = sum(self.sent.values())
            total_recv = sum(self.received.values())
            per_topic = sorted(
                ({"topic": t,
                  "sent": self.sent.get(t, 0),
                  "received": self.received.get(t, 0)}
                 for t in set(self.sent) | set(self.received)),
                key=lambda r: r["topic"])
            return {
                "total_sent": total_sent,
                "total_received": total_recv,
                "uptime_s": round(time.time() - self.started, 1),
                "per_topic": per_topic,
                "recent": list(self.recent)[:40],
            }

    def log_snapshot(self, level: str | None = None, limit: int = 200) -> list[dict]:
        with self.lock:
            items = list(self.logs)
        if level and level != "ALL":
            items = [x for x in items if x["level"] == level]
        return items[:limit]


tracker = Tracker()


# ---- bridge python logging into the UI log buffer --------------------------
class TrackerLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            tracker.log(record.levelname, self.format(record))
        except Exception:
            pass


def attach_log_capture() -> None:
    h = TrackerLogHandler()
    h.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    h.setLevel(logging.INFO)
    logging.getLogger().addHandler(h)

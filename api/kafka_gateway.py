"""
Kafka integration layer.

Wraps kafka-python with failover-friendly settings:
  * bootstrap points at BOTH brokers, so the client reconnects to whichever
    is alive.
  * producer acks="all" + retries so a broker failover does not drop messages.
  * consumers use group ids so partitions rebalance automatically when a
    broker or consumer instance dies.

If Kafka is unreachable the API stays up in DEGRADED mode and buffers nothing
silently -- it reports the failure via /health and the publish endpoints return
503 so the frontend can show the outage instead of pretending success.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Callable

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from domain import TOPIC_CONFIG

log = logging.getLogger("kafka")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka1:19092,kafka2:19092")


def _servers() -> list[str]:
    return [s.strip() for s in BOOTSTRAP.split(",") if s.strip()]


class KafkaGateway:
    def __init__(self) -> None:
        self._producer: KafkaProducer | None = None
        self._lock = threading.Lock()
        self.connected = False
        self.last_error: str | None = None

    # ---- producer ---------------------------------------------------------
    def _ensure_producer(self) -> KafkaProducer:
        with self._lock:
            if self._producer is not None:
                return self._producer
            self._producer = KafkaProducer(
                bootstrap_servers=_servers(),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",            # wait for all in-sync replicas
                retries=5,             # survive a leader election on failover
                retry_backoff_ms=300,
                request_timeout_ms=8000,
                linger_ms=20,
            )
            self.connected = True
            return self._producer

    def publish(self, topic: str, payload: dict, key: str | None = None) -> dict:
        from metrics import tracker, PUBLISH_LATENCY, KAFKA_UP
        t0 = time.time()
        try:
            prod = self._ensure_producer()
            fut = prod.send(topic, value=payload, key=key)
            meta = fut.get(timeout=8)
            self.connected = True
            self.last_error = None
            PUBLISH_LATENCY.labels(topic=topic).observe(time.time() - t0)
            KAFKA_UP.set(1)
            tracker.on_sent(topic, key)        # count produced message
            return {"topic": meta.topic, "partition": meta.partition,
                    "offset": meta.offset}
        except KafkaError as e:
            self.connected = False
            self.last_error = str(e)
            KAFKA_UP.set(0)
            tracker.on_error(topic)
            # drop the dead producer so the next call rebuilds against a live broker
            with self._lock:
                self._producer = None
            raise

    # ---- admin / topic bootstrap -----------------------------------------
    def ensure_topics(self, retries: int = 30) -> bool:
        for attempt in range(retries):
            try:
                admin = KafkaAdminClient(bootstrap_servers=_servers(),
                                         request_timeout_ms=5000)
                new = [
                    NewTopic(name=name,
                             num_partitions=cfg["partitions"],
                             replication_factor=cfg["replication"],
                             topic_configs={"min.insync.replicas": "1"})
                    for name, cfg in TOPIC_CONFIG.items()
                ]
                try:
                    admin.create_topics(new)
                    log.info("created topics: %s", list(TOPIC_CONFIG))
                except TopicAlreadyExistsError:
                    log.info("topics already exist")
                admin.close()
                self.connected = True
                return True
            except Exception as e:  # broker not up yet
                self.last_error = str(e)
                log.warning("topic bootstrap attempt %s failed: %s", attempt + 1, e)
                time.sleep(3)
        return False

    # ---- consumer (background tailer) ------------------------------------
    def tail(self, topics: list[str], group: str,
             on_message: Callable[[str, dict], None]) -> threading.Thread:
        def _run() -> None:
            while True:
                try:
                    consumer = KafkaConsumer(
                        *topics,
                        bootstrap_servers=_servers(),
                        group_id=group,
                        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                        auto_offset_reset="latest",
                        enable_auto_commit=True,
                        consumer_timeout_ms=1000,
                    )
                    for msg in consumer:
                        from metrics import tracker
                        key = msg.key.decode() if msg.key else None
                        tracker.on_received(msg.topic, key)   # count consumed
                        try:
                            on_message(msg.topic, msg.value)
                        except Exception:
                            log.exception("handler error")
                except Exception as e:
                    log.warning("consumer reconnecting after error: %s", e)
                    time.sleep(3)

        t = threading.Thread(target=_run, name=f"tail-{group}", daemon=True)
        t.start()
        return t


gateway = KafkaGateway()

"""Self-contained live test: boots the real FastAPI app with a stubbed Kafka
gateway (synchronous cascade) and exercises every endpoint over real HTTP."""
import os
import sys
import threading
import time
import json
import http.client
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "api"))
os.environ["FRONTEND_DIR"] = os.path.join(HERE, "..", "frontend")
os.chdir(os.path.join(HERE, "..", "api"))

import kafka_gateway
import store_state


class StubGW:
    connected = True
    last_error = None

    def __init__(self):
        self._store = None

    def publish(self, topic, payload, key=None):
        from metrics import tracker
        tracker.on_sent(topic, key)
        if self._store:
            tracker.on_received(topic, key)
            self._store.handle(topic, payload)
        return {"topic": topic, "partition": 0, "offset": 0}

    def ensure_topics(self, *a, **k):
        return True

    def tail(self, *a, **k):
        return type("T", (), {})()


stub = StubGW()
kafka_gateway.gateway = stub
import main
main.gateway = stub
store_state.store.emit = lambda t, p, k: stub.publish(t, p, k)
stub._store = store_state.store

import uvicorn
cfg = uvicorn.Config(main.app, host="127.0.0.1", port=8077, log_level="error")
srv = uvicorn.Server(cfg)
threading.Thread(target=srv.run, daemon=True).start()
time.sleep(3)


def req(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request("http://127.0.0.1:8077" + path, data=data,
                               method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=8) as resp:
        return resp.status, json.loads(resp.read())


ok = tot = 0


def chk(n, c, d=""):
    global ok, tot
    tot += 1
    ok += 1 if c else 0
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" — {d}" if d else ""))


print("== health =="); st, b = req("/api/health"); chk("health 200", st == 200)
print("== catalog =="); st, b = req("/api/catalog"); chk("8+ products", len(b["products"]) >= 8)
print("== frontend ==")
c = http.client.HTTPConnection("127.0.0.1", 8077); c.request("GET", "/"); r = c.getresponse()
body = r.read().decode(); chk("index served", r.status == 200 and "FreshChain" in body)
print("== cascade ==")
_, s0 = req("/api/state")
st, _ = req("/api/sale", "POST", {"sku": "COFFEE-005", "qty": 12}); chk("sale ok", st == 200)
time.sleep(0.5); _, s = req("/api/state")
chk("revenue up", s["revenue"] > s0["revenue"], f"{s0['revenue']}->{s['revenue']}")
chk("reorder triggered", any(o["sku"] == "COFFEE-005" for o in s["pending_orders"]))
ships = [x for x in s["shipments"] if x["sku"] == "COFFEE-005"]
chk("shipment made", len(ships) > 0)
if ships:
    trace = ships[0]["trace"]; before = next(p["qty"] for p in s["products"] if p["sku"] == "COFFEE-005")
    req("/api/deliver", "POST", {"trace": trace}); time.sleep(0.5)
    _, s2 = req("/api/state"); after = next(p["qty"] for p in s2["products"] if p["sku"] == "COFFEE-005")
    chk("restocked on delivery", after > before, f"{before}->{after}")
print("== employee ==")
req("/api/employee", "POST", {"employee_id": "T1", "name": "Tess", "register_no": 3, "action": "clock_in"}); time.sleep(0.3)
_, s = req("/api/state"); chk("clocked in", any(e["employee_id"] == "T1" and e["status"] == "in" for e in s["employees"]))
print("== shelf ==")
req("/api/shelf", "POST", {"sku": "MILK-001", "shelf": "Z9-Z"}); time.sleep(0.3)
_, s = req("/api/state"); chk("shelf moved", next(p["shelf_current"] for p in s["products"] if p["sku"] == "MILK-001") == "Z9-Z")
print("== simulate ==")
st, b = req("/api/simulate?n=10", "POST"); chk("simulate burst", st == 200 and b["count"] == 10)

print("== metrics tracker ==")
_, m = req("/api/metrics"); chk("metrics sent>0", m["total_sent"] > 0, f"sent={m['total_sent']}")
chk("metrics received>0", m["total_received"] > 0, f"recv={m['total_received']}")
chk("per-topic populated", len(m["per_topic"]) > 0)
chk("recent feed", len(m["recent"]) > 0)
print("== prometheus ==")
import urllib.request as u
raw = u.urlopen("http://127.0.0.1:8077/metrics", timeout=8).read().decode()
chk("prom exposes sent counter", "freshchain_messages_sent_total" in raw)
chk("prom exposes recv counter", "freshchain_messages_received_total" in raw)
print("== logs ==")
_, lg = req("/api/logs?limit=50"); chk("logs present", len(lg["logs"]) > 0, f"n={len(lg['logs'])}")

print(f"\n{ok}/{tot} PASSED")
sys.exit(0 if ok == tot else 1)

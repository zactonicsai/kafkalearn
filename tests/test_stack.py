#!/usr/bin/env python3
"""
CLI integration tests for the FreshChain stack.

Tests three layers:
  1. API health + Kafka connectivity
  2. End-to-end event cascade (sale -> inventory -> reorder -> deliver -> restock)
  3. Failover (optional, --failover): kill a broker mid-run and assert continuity

Usage:
  python3 test_stack.py --base http://localhost:8000
  python3 test_stack.py --base http://<ec2-ip>:8000 --failover

Exit code 0 = all pass. Designed to run from CLI against Docker OR AWS.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
import urllib.request
import urllib.error
import json

PASS, FAIL = "\033[92mPASS\033[0m", "\033[91mFAIL\033[0m"
results: list[tuple[str, bool, str]] = []


def _req(base: str, path: str, method: str = "GET", body: dict | None = None):
    url = base.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read().decode())


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, cond, detail))
    print(f"  [{PASS if cond else FAIL}] {name}" + (f" — {detail}" if detail else ""))


def test_health(base: str) -> None:
    print("\n== Layer 1: health ==")
    try:
        st, body = _req(base, "/api/health")
        check("API responds 200", st == 200, f"status={st}")
        check("Kafka connected", body.get("kafka_connected") is True,
              f"last_error={body.get('last_error')}")
    except Exception as e:
        check("API reachable", False, str(e))


def test_catalog(base: str) -> None:
    print("\n== Layer 1: catalog ==")
    st, body = _req(base, "/api/catalog")
    check("catalog non-empty", len(body.get("products", [])) >= 8)


def test_cascade(base: str) -> None:
    print("\n== Layer 2: event cascade ==")
    # baseline
    _, s0 = _req(base, "/api/state")
    rev0 = s0["revenue"]
    # drive coffee below reorder to trigger the chain (reorder=10, start=20)
    st, _ = _req(base, "/api/sale", "POST", {"sku": "COFFEE-005", "qty": 12})
    check("sale accepted", st == 200, f"status={st}")
    # let the async consumer process the cascade
    pending = []
    for _ in range(20):
        time.sleep(0.5)
        _, s = _req(base, "/api/state")
        pending = [o["sku"] for o in s["pending_orders"]]
        if "COFFEE-005" in pending:
            break
    check("revenue increased", s["revenue"] > rev0, f"{rev0} -> {s['revenue']}")
    check("reorder triggered", "COFFEE-005" in pending, f"pending={pending}")
    ships = [x for x in s["shipments"] if x["sku"] == "COFFEE-005"]
    check("shipment created", len(ships) > 0, f"shipments={len(s['shipments'])}")
    if ships:
        trace = ships[0]["trace"]
        qty_before = next(p["qty"] for p in s["products"] if p["sku"] == "COFFEE-005")
        _req(base, "/api/deliver", "POST", {"trace": trace})
        restocked = False
        for _ in range(20):
            time.sleep(0.5)
            _, s2 = _req(base, "/api/state")
            qty_after = next(p["qty"] for p in s2["products"] if p["sku"] == "COFFEE-005")
            if qty_after > qty_before:
                restocked = True
                break
        check("delivery restocked inventory", restocked,
              f"{qty_before} -> {qty_after}")


def test_employee(base: str) -> None:
    print("\n== Layer 2: employee/register ==")
    _req(base, "/api/employee", "POST",
         {"employee_id": "TEST01", "name": "Tester", "register": 9,
          "action": "clock_in"})
    for _ in range(10):
        time.sleep(0.4)
        _, s = _req(base, "/api/state")
        if any(e["employee_id"] == "TEST01" and e["status"] == "in"
               for e in s["employees"]):
            check("employee clocked in", True)
            return
    check("employee clocked in", False)


def test_failover(base: str, broker: str = "fc-kafka1") -> None:
    print(f"\n== Layer 3: failover (stopping {broker}) ==")
    try:
        subprocess.run(["docker", "stop", broker], check=True,
                       capture_output=True, timeout=30)
    except Exception as e:
        check("stop broker", False, str(e))
        return
    time.sleep(8)  # allow leader election
    try:
        st, _ = _req(base, "/api/sale", "POST", {"sku": "MILK-001", "qty": 1})
        check("publish survives one broker down", st == 200, f"status={st}")
    except Exception as e:
        check("publish survives one broker down", False, str(e))
    finally:
        subprocess.run(["docker", "start", broker], capture_output=True, timeout=30)
        print(f"  restarted {broker}")


def test_monitoring(base: str) -> None:
    print("\n== Layer 1: monitoring ==")
    try:
        st, m = _req(base, "/api/metrics")
        check("metrics endpoint", st == 200 and "total_sent" in m)
        check("messages tracked", m["total_sent"] > 0, f"sent={m['total_sent']}")
        st2, lg = _req(base, "/api/logs?limit=20")
        check("logs endpoint", st2 == 200 and "logs" in lg)
        # prometheus exposition
        import urllib.request as u
        raw = u.urlopen(base.rstrip("/") + "/metrics", timeout=8).read().decode()
        check("prometheus /metrics", "freshchain_messages_sent_total" in raw)
    except Exception as e:
        check("monitoring reachable", False, str(e))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--failover", action="store_true",
                    help="run docker broker-kill failover test")
    ap.add_argument("--broker", default="fc-kafka1")
    args = ap.parse_args()

    print(f"Testing {args.base}")
    test_health(args.base)
    test_catalog(args.base)
    test_cascade(args.base)
    test_employee(args.base)
    test_monitoring(args.base)
    if args.failover:
        test_failover(args.base, args.broker)

    passed = sum(1 for _, c, _ in results if c)
    total = len(results)
    print(f"\n{'='*40}\n{passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())

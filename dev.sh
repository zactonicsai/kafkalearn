#!/usr/bin/env bash
#
# Local dev helper for the FreshChain stack (Docker only, no AWS).
#
#   ./dev.sh up        build + start the 2-broker cluster and API
#   ./dev.sh test      run CLI tests against localhost:8000
#   ./dev.sh failover  run tests including the broker-kill failover check
#   ./dev.sh logs      tail all service logs
#   ./dev.sh down      stop and remove containers + volumes
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

case "${1:-up}" in
  up)
    docker compose pull
    docker compose up -d --build
    echo "waiting for API health..."
    for i in $(seq 1 40); do
      if curl -fsS http://localhost:8000/api/health 2>/dev/null | grep -q '"kafka_connected":true'; then
        echo "ready:"
        echo "  Web UI / API   -> http://localhost:8000"
        echo "  Grafana        -> http://localhost:3000  (admin/admin)"
        echo "  Prometheus     -> http://localhost:9090"
        echo "  kafka-exporter -> http://localhost:9308/metrics"
        exit 0
      fi
      sleep 5
    done
    echo "timed out waiting for health; check: docker compose logs"; exit 1 ;;
  test)
    python3 tests/test_stack.py --base http://localhost:8000 ;;
  failover)
    python3 tests/test_stack.py --base http://localhost:8000 --failover --broker fc-kafka1 ;;
  logs)
    docker compose logs -f ;;
  down)
    docker compose down -v ;;
  *)
    echo "usage: $0 {up|test|failover|logs|down}"; exit 1 ;;
esac

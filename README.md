# FreshChain — Grocery Event Streaming Demo

An event-driven grocery operation built on a **two-broker Apache Kafka cluster**
with a **Python FastAPI** gateway and an **IBM Carbon-styled** web console. A
single sale cascades through eight business domains — inventory, ordering,
vendor purchasing, logistics, shelf placement, employee/register, and owner
profit tracking — all wired together over Kafka topics.

The repo ships everything to run it locally in Docker and to deploy it to AWS
EC2 via CloudFormation + Ansible, with CLI test suites for both.

---

## Table of contents

1. [Architecture](#architecture)
2. [How the event cascade works](#how-the-event-cascade-works)
3. [Quick start (local Docker)](#quick-start-local-docker)
4. [Service-by-service walkthrough](#service-by-service-walkthrough)
5. [The frontend console](#the-frontend-console)
6. [Failover behavior](#failover-behavior)
7. [Testing](#testing)
8. [AWS deployment](#aws-deployment)
9. [API reference](#api-reference)
10. [Project layout](#project-layout)
11. [Future: GitLab CI/CD](#future-gitlab-cicd)

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
   Browser  ───────▶│  FastAPI (api/, port 8000)               │
   (frontend)       │  • serves the static console              │
                    │  • REST endpoints publish events          │
                    │  • background consumer tails ALL topics   │
                    │  • maintains in-memory StoreState         │
                    └───────────────┬───────────────────────────┘
                                    │ produce / consume
                    ┌───────────────▼───────────────┐
                    │   Kafka cluster (KRaft mode)    │
                    │   kafka1 :9092   kafka2 :9094   │
                    │   replication factor = 2        │
                    └─────────────────────────────────┘
```

* **Two brokers** run in KRaft mode (no ZooKeeper). Topics are created with
  replication factor 2 and `min.insync.replicas=1`, so the cluster keeps
  serving if a single broker dies.
* **The API is both producer and consumer.** Endpoints publish events; a
  background thread consumes every topic and folds messages into a single
  in-memory state object the dashboard reads.
* **Latest images** are used for Kafka (`bitnami/kafka:latest`) and Python
  (`python:3.12-slim`).

---

## How the event cascade works

Eight domains each own a topic. One sale sets off a chain reaction:

| Step | Topic | Event | Effect |
|------|-------|-------|--------|
| 1 | `grocery.sales` | `sale` | decrement inventory, book revenue + profit |
| 2 | `grocery.inventory` | `stock_check` | if below reorder point → emit reorder |
| 3 | `grocery.ordering` | `reorder_request` | create a purchase order |
| 4 | `grocery.vendor` | `purchase_order` | dispatch to vendor → shipment |
| 5 | `grocery.logistics` | `shipment_update` | track in-transit → delivered |
| 6 | `grocery.inventory` | (restock) | delivered shipment refills stock |
| 7 | `grocery.shelf` | `placement` | restocked goods get a shelf location |
| — | `grocery.employee` | `clock_in/out` | gate registers for sales attribution |
| — | `grocery.owner` | `rollup` | running revenue/profit feed |

You can watch this live on the **Event Flow** tab, where edges light up as
messages traverse them and a message trail logs each hop.

---

## Quick start (local Docker)

Prerequisites: Docker with the Compose v2 plugin.

```bash
./dev.sh up        # pull latest images, build API, start everything
# open http://localhost:8000

./dev.sh test      # run the CLI integration suite against localhost
./dev.sh failover  # same, plus kills a broker mid-run to prove failover
./dev.sh logs      # tail all logs
./dev.sh down      # tear down + remove volumes
```

On first boot the API waits for both brokers to report healthy, creates the
eight topics, then starts tailing them. The console shows a green
**cluster healthy** badge once Kafka is connected.

---

## Service-by-service walkthrough

### `kafka1` / `kafka2` — the broker cluster
KRaft-mode brokers that are also controllers (combined role), forming a quorum
of two. Each advertises an **INTERNAL** listener (`:19092`) for container-to-
container traffic and an **EXTERNAL** listener (`localhost:9092` / `:9094`) so
host tools and the test suite can reach them directly. Data persists in named
volumes. Healthchecks use `kafka-broker-api-versions.sh` so dependents wait
until the brokers actually accept connections.

### `api` — FastAPI gateway (`api/`)
* **`domain.py`** — defines the eight topics, their partition/replication
  config, the seed product catalog (cost, price, reorder point, vendor, shelf),
  and the common event envelope.
* **`kafka_gateway.py`** — a failover-aware wrapper around `kafka-python`. The
  producer uses `acks="all"` + retries so a leader election during failover
  doesn't drop messages; on error it discards the dead producer and rebuilds
  against a live broker. `ensure_topics()` retries until the cluster is up.
  `tail()` runs a self-healing background consumer that reconnects on error.
* **`store_state.py`** — the brain. Consumes events and mutates a single
  `StoreState`: inventory levels, revenue/profit, shelf map, employees, pending
  orders, shipments, and a rolling flow log. Each handler may emit downstream
  events, which is what produces the cascade.
* **`main.py`** — wires it together: REST endpoints publish events, the startup
  hook bootstraps topics and the consumer, `/api/state` returns the dashboard
  snapshot, `/api/health` reports cluster status, and the static frontend is
  mounted at `/`.

### frontend (`frontend/index.html`)
A single self-contained page (Tailwind via CDN, IBM Plex fonts) served by the
API. No build step. Polls `/api/state` every 2.5s and `/api/health` every 5s.

---

## The frontend console

Built to the requested IBM aesthetic: **white background, IBM-blue / near-black
typography**, with:

* **Light & dark modes** — toggle in the header (Carbon-style palettes via CSS
  variables; SVG charts recolor on switch).
* **Font-size control** — `−` / `+` buttons scale the root font from **14px to
  36px**; the whole layout scales with it.
* **Mobile friendly** — responsive grids, horizontal-scrolling tabs and tables,
  touch-friendly controls.
* **Tabs** — Actions, Inventory & Shelf, Charts, Event Flow, Ops & Tracking.
* **Pop-up details** — every action card has a `?` that opens a modal with an
  explanation and **links to references** (Kafka docs, Carbon, FastAPI).
* **SVG graphs** — hand-built (no chart library): a units-sold bar chart, a
  profit-contribution donut, and an inventory-vs-reorder grouped bar chart.
* **Flow diagram** — an SVG pipeline of the domains with animated edges that
  light up as messages flow, plus a live message trail.
* **Simulated actions** — sell a chosen SKU, fire a burst of 12 random sales,
  clock employees in/out on a register, reassign shelves, and force-deliver
  shipments to complete the loop.

---

## Failover behavior

The cluster is built to survive one broker going down:

* Topics use **replication factor 2**, so every partition has a copy on each
  broker; `min.insync.replicas=1` lets a single survivor keep accepting writes.
* The producer points at **both** brokers and retries through leader elections.
* Consumers belong to a group and **rebalance automatically** when a broker or
  consumer instance disappears.
* If Kafka is entirely unreachable the API doesn't fake success — publish
  endpoints return **503** and `/api/health` reports `degraded`, which the
  console surfaces as a red banner.

Prove it:

```bash
./dev.sh failover     # stops fc-kafka1 mid-test, asserts publishes still work,
                      # then restarts it
```

---

## Testing

Three layers, all runnable from the CLI:

| Suite | File | Needs | What it covers |
|-------|------|-------|----------------|
| Unit | `tests/test_unit.py` | nothing | store logic + full cascade, offline |
| Live (stub) | `tests/test_live_stub.py` | nothing | every HTTP endpoint with a stubbed broker |
| Integration | `tests/test_stack.py` | running stack | real cluster: health, cascade, employee, optional failover |

```bash
python3 tests/test_unit.py                              # offline
python3 tests/test_live_stub.py                         # offline, real HTTP
python3 tests/test_stack.py --base http://localhost:8000            # docker
python3 tests/test_stack.py --base http://<ec2-ip>:8000 --failover  # aws
```

All unit and live-stub checks pass with no external services. The integration
suite is what `dev.sh test` and the AWS deployer run.

---

## AWS deployment

Deployment is **env-driven** and runs from the CLI. CloudFormation provisions
the host (using the SSM parameter that always resolves to the **latest Amazon
Linux 2023 AMI**); Ansible installs Docker and brings up the same compose stack;
the test suite verifies the live endpoint.

### 1. Configure
```bash
cp deploy/.env.example deploy/.env
# edit: AWS_REGION, KEY_NAME, KEY_PATH (path to your .pem)
```

### 2. Deploy
```bash
deploy/scripts/deploy.sh up
```
This will:
1. validate prerequisites and AWS credentials,
2. `aws cloudformation deploy` the `freshchain-host` stack,
3. wait for SSH, generate `deploy/ansible/inventory.ini` from the stack's
   public IP,
4. run `ansible-playbook` (installs Docker + compose, copies the repo, pulls
   latest images, `docker compose up -d --build`, waits for `/api/health`),
5. run the CLI integration tests against `http://<public-ip>:8000`.

### Other subcommands
```bash
deploy/scripts/deploy.sh test    # re-run tests against the live stack
deploy/scripts/deploy.sh info    # print stack outputs (IP, URL, SSH command)
deploy/scripts/deploy.sh down    # delete the CloudFormation stack
```

### What gets created
* **CloudFormation** (`deploy/cloudformation/freshchain-host.yaml`): one EC2
  instance (default `t3.large`, 30 GB gp3), a security group exposing SSH
  (lockable to your IP), 8000 (web/API) and 9092/9094 (Kafka external).
* **Ansible** (`deploy/ansible/site.yml`): idempotent Docker install + compose
  deploy with a health gate.

---

## API reference

| Method | Path | Body | Purpose |
|--------|------|------|---------|
| GET | `/api/health` | — | cluster status, `kafka_connected`, last error |
| GET | `/api/catalog` | — | product catalog |
| GET | `/api/state` | — | full dashboard snapshot |
| POST | `/api/sale` | `{sku, qty}` | publish a sale |
| POST | `/api/employee` | `{employee_id, name, register_no, action}` | clock in/out |
| POST | `/api/shelf` | `{sku, shelf}` | reassign shelf location |
| POST | `/api/deliver` | `{trace}` | mark a shipment delivered (restocks) |
| POST | `/api/simulate?n=12` | — | fire N random sales |

`action` is `clock_in` or `clock_out`. Publish endpoints return **503** if the
cluster is unreachable.

---

## Project layout

```
grocery-kafka/
├── docker-compose.yml          # 2-broker Kafka + API
├── dev.sh                      # local docker helper (up/test/failover/logs/down)
├── .gitlab-ci.yml              # future CI/CD pipeline
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── domain.py               # topics, catalog, envelope
│   ├── kafka_gateway.py        # failover-aware producer/consumer/admin
│   ├── store_state.py          # event handlers + cascade + state
│   └── main.py                 # FastAPI app
├── frontend/
│   └── index.html              # IBM Carbon console (dark/light, font scale, SVG)
├── tests/
│   ├── test_unit.py            # offline logic
│   ├── test_live_stub.py       # offline HTTP (stubbed broker)
│   └── test_stack.py           # live integration + failover
└── deploy/
    ├── .env.example
    ├── cloudformation/freshchain-host.yaml
    ├── ansible/{site.yml, inventory.ini}
    └── scripts/deploy.sh        # env-driven AWS deployer
```

---

## Future: GitLab CI/CD

`.gitlab-ci.yml` defines the intended pipeline once the repo lives in GitLab:

* **lint** — advisory `ruff`,
* **test** — unit + live-stub suites on every push,
* **integration** — `docker compose` stack in Docker-in-Docker, run
  `test_stack.py` against it,
* **build** — build and push the API image to the GitLab registry on `main`,
* **deploy** — manual job that runs `deploy/scripts/deploy.sh up` using
  protected AWS variables.

Commits then trigger tests automatically; deploys stay one click away.
"# kafkalearn" 

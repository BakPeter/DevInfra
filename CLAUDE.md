# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A local development infrastructure sandbox: one Docker Compose stack of backing services (Kafka, Seq, Elasticsearch/Kibana, MinIO) plus three polyglot proof-of-concept REST services (Node.js, Python, .NET) that exercise it. The services deliberately implement the *same* contract in three stacks, so changes to one are usually meant to be mirrored in the others.

## Commands

All commands run from the repo root unless noted.

```bat
infra.bat start             :: create infra-net if missing, then compose up --build -d
infra.bat stop              :: compose down, then remove infra-net
infra.bat start --dry-run   :: print what would run, change nothing
```

`infra.bat` is a convenience wrapper, not a requirement: compose owns the `infra-net` lifecycle, so `docker compose --env-file Infra/infra.env -f Infra/docker-compose-infra.yml up --build -d` is equivalent. The `--env-file` is not optional though — the compose file is almost entirely `${...}` references and resolves to nothing useful without it.

Running a service directly on the host (bypassing its container):

```bat
cd Services\service-nodejs && npm install && npm start
cd Services\service-python && pip install -r requirements.txt && python app.py
cd Services\service-dotnet && dotnet run --project service-dotnet\service-dotnet.csproj
```

Inspecting the database:

```bat
docker exec -it postgres psql -U postgres -d devinfra
```

There is no test suite, linter, or formatter configured in this repo. `npm test` in `service-nodejs` maps to `node --test` but no test files exist yet; if you add tests there, `node --test <file>` runs a single one.

## Configuration model

`Infra/infra.env` is the single source of truth for ports, topic names, and credentials, consumed by `infra.bat` via `--env-file`. Two things to know before editing it:

- **Derived topic names.** Per-service topics interpolate the base: `KAFKA_TOPIC_ITEMS_PYTHON_SERVICE=${KAFKA_TOPIC_ITEMS}.python`. Change `KAFKA_TOPIC_ITEMS` and every per-service topic moves with it.
- **`MOUNT_DATA`** (`C:\Mounts\Infra`) is the host root for all bind-mounted volumes and must exist before `infra.bat start`. Every stateful service persists under it, Kafka and ZooKeeper included — if the broker ever refuses to start with a cluster-id mismatch, delete `${MOUNT_DATA}/kafka` and `${MOUNT_DATA}/zookeeper` *together*, never one alone.
- **Image tags** go through `*_IMG_TAG` variables. `MINIO_IMG_TAG` is stuck on `latest` on purpose: the `minio/minio` Docker Hub repo is no longer pullable at any tag, so that one service runs from a locally cached image and will fail on a fresh machine.
- The top of the file holds **host-run defaults** (`SEQ_SERVER_URL`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`) that compose never consumes — they matter only when a service runs directly on the host.

### In-container vs. host addressing (most common source of breakage)

The Kafka broker advertises two listeners, and Seq has two names, so the correct address depends on where the code runs:

| Target | From a compose service | From a host process |
| --- | --- | --- |
| Kafka | `kafka-broker:29092` (`PLAINTEXT_INTERNAL`) | `localhost:9092` (`${KAFKA_IP}`) |
| Seq | `http://seq` | `http://localhost:5341` |
| Postgres | `postgres:5432` | `localhost:${POSTGRES_PORT}` |

Note the Postgres row: compose passes `POSTGRES_PORT=5432` (the in-container port) to the services, *not* `${POSTGRES_PORT}` from `infra.env` — that variable is the host-side mapping. Same split as Kafka's two listeners.

The compose file injects the in-container values; the `infra.env` values and the code defaults are the host ones. A service that connects in Docker but not from `npm start` (or vice versa) is almost always this.

Ports: Node 3001, Python 3002, .NET 3003 in compose / 5079+7044 from `launchSettings.json`, Seq 5341, Kafka 9092, kafka-ui 8080, Elasticsearch 9200, Kibana 5601, MinIO 9004 API / 9005 console, Postgres 5432, pgAdmin 5050.

`postgres` and `kafka-broker` have healthchecks and their dependents wait on `condition: service_healthy`, so the first `up` takes a while before the app services start. Seq deliberately has none — logging failures are non-fatal everywhere.

## Architecture

### Event topology

Each service owns an outbound topic and publishes an envelope of `{ action, item, occurredAt }` keyed by item id on every mutating (and, for `fetched`, read) operation:

- `service-python` → `items.events.python`
- `service-nodejs` → `items.events.nodejs`, and **consumes** `NODEJS_SERVICE_KAFKA_CONSUMER_TOPICS` (currently `items.events.python`), logging what it receives.

So the only wired-up cross-service flow is Python-produces → Node-consumes. Adding a consumer means adding a `*_KAFKA_CONSUMER_TOPICS` variable in `infra.env` *and* passing it through in `docker-compose-infra.yml` — the compose `environment:` blocks enumerate each variable explicitly, so a new `infra.env` entry is invisible to a container until it is listed there.

Producers connect lazily and are cached (`getProducer()` in Node, `AppState.get_producer()` behind an `asyncio.Lock` in Python), and publish failures are logged but never fail the HTTP request — the REST API stays up when Kafka is down.

#### State heartbeat

Separately, **all three** services publish a periodic self-state event to their own topic, every `<NAME>_SERVICE_STATE_INTERVAL_SECONDS` (default 30):

- `service-nodejs` → `service-nodejs.state`
- `service-python` → `service-python.state`
- `service-dotnet` → `service-dotnet.state`

The Kafka key is always the literal string `ServiceState`; the value is `{ publishedAt, originService, healthState, checks }`, where `healthState` is derived from live probes of Postgres and Kafka — `Healthy`, `Degraded` (one failing) or `Unhealthy` (both). Nothing consumes these topics yet.

Each event fans out to three sinks — Kafka, Seq and a Postgres table. Three properties make that work, and all three were added because the obvious implementation failed a broker-outage test. Keep them when touching `publishServiceState` / `publish_service_state` / `ServiceStatePublisher`:

1. **The Kafka probe must be a real metadata round-trip.** Asking the cached producer whether it is connected reports a *stopped* broker as `Healthy` — kafkajs and aiokafka both hand back the cached connection, so the event claims health while the publish right after it fails. Node uses a second `Kafka` client (`kafkaProbe`, zero retries, 2s timeouts, logging off) with `admin.listTopics()`; Python uses `producer.client.fetch_all_metadata()`; .NET uses `IAdminClient.GetMetadata()`.
2. **Every probe and sink is time-bounded** (3s probes, 5s sinks). Unbounded, a dead broker parks the tick: kafkajs retries for ~30s, and Confluent's `MessageTimeoutMs` defaults to *five minutes*, which silently skipped the Postgres write entirely.
3. **The sinks run concurrently**, each with its own error handling (`Promise.allSettled` / `asyncio.gather(return_exceptions=True)` / `Task.WhenAll`). Sequential sinks are failure-isolated but not time-isolated — a slow Kafka still delays the Seq and Postgres writes.

The payoff: with Kafka stopped, a `Degraded` event still reaches Seq and Postgres on schedule, and the services recover to `Healthy` on their own when the broker returns — no restart needed.

### Shared service contract

Node and Python both expose the same surface over a Postgres-backed `items` table:

```
GET    /health            -> { status: "ok" }
GET    /api/items
GET    /api/items/:id     -> 404 { message: "Item not found" }
POST   /api/items         -> 201, body { name }, 400 { message: "name is required" }
PUT    /api/items/:id
DELETE /api/items/:id     -> 204
```

The error *shapes* are part of the contract: Python's `http_exception_handler` exists purely to rewrite FastAPI's default `{"detail": ...}` into Express's `{"message": ...}`. Keep the two in sync when touching either. Responses from 3001 and 3002 should stay byte-identical, which is the quickest way to check a change didn't drift one implementation.

### Storage

Each service owns its **own schema** in the shared `devinfra` database (`POSTGRES_SCHEMA`, defaulting to `nodejs` / `python` / `dotnet`), mirroring the per-service Kafka topic convention. Each schema holds an `items` table (Node and Python only) and a `service_state` table (all three, append-only heartbeat history — at the default 30s that is ~2,880 rows/day/service). Both create the schema, create `items`, and seed `alpha`/`beta`/`gamma` on startup, guarded so the seed only fires when the table is empty — Node in `initDb()`, Python in `AppState.init_db()` off the lifespan hook. Writing to one service's items does not affect the other's.

Two things to preserve when touching this:

- `id` is `GENERATED BY DEFAULT AS IDENTITY`, so the database assigns ids and the seed inserts names only. Don't reintroduce an application-side counter; the old in-memory Node version started its counter at 3 while an item with id 3 already existed, so its first `POST` returned a duplicate id.
- A schema name cannot be a bound parameter, so it is interpolated into the SQL. Both services validate `POSTGRES_SCHEMA` against `^[a-z_][a-z0-9_]*$` and fail fast at startup. Keep that guard if you add queries.

`service-dotnet` is a containerized stack member (Dockerfile + compose entry, port 3003) but implements **only** the state heartbeat — it still has no `/api/items` and still exposes the scaffold's `WeatherForecastController`. Bringing it onto the shared CRUD contract is outstanding work, not an existing feature.

Two .NET-specific notes: its Dockerfile uses the **Debian-based** `mcr.microsoft.com/dotnet/*:10.0` images rather than Alpine, because Confluent.Kafka's native librdkafka needs extra packages under musl; and `UseHttpsRedirection()` is guarded to Development, since the container serves plain HTTP on `ASPNETCORE_URLS`.

### Logging

Every service logs structured events to Seq (`winston` + `@datalust/winston-seq`, `seqlog` + `StructuredLogger`, `Serilog.Sinks.Seq`) and to the console, with a request-logging middleware emitting method/path/status/duration. Message templates use Seq's `{named}` placeholder style, so keep placeholders named rather than positional.

Two non-obvious constraints:

- Python passes `log_config=None` to `uvicorn.run`; without it Uvicorn replaces the logging config and closes the Seq handler. Don't remove it.
- Node reads `NODEJS_SERVICE_KAFKA_CONSUMER_TOPICS` with no fallback and calls `.split(',')` on it, so the process throws at import time if that variable is unset — relevant whenever running it outside compose.

# EventRail

**Idempotent Event Ingestion & Replay System**

EventRail is a event ingestion and delivery system designed to safely ingest, persist, deliver, retry, and replay events under real-world failure conditions.

It provides idempotency guarantees, durable storage, fan-out delivery, retry with backoff, dead-letter queues, and replay/backfill capabilities using a clean, API-first design.

> Think of EventRail as a simplified, developer-friendly alternative to internal event pipelines.

## Docker Image

EventRail is published as a Docker image via GitHub Container Registry.

```bash
docker pull ghcr.io/krishgondaliya/eventrail:latest

---

## Architecture Overview

```
Client
  │
  │  POST /events  (Idempotency-Key)
  ▼
API (Go)
  │
  │  Durable write
  ▼
PostgreSQL  ◄── source of truth
  │
  │  Publish (best-effort)
  ▼
Redis Streams
  │
  │  Consumer Groups
  ▼
Workers
  │
  ├──► Retry (ZSET + backoff)
  │
  └──► Dead-Letter Queue
```

### Key Principles

| Principle | Description |
|-----------|-------------|
| Durability first | DB commit before delivery |
| Idempotency at the boundary | Safe duplicate handling |
| At-least-once delivery | With exactly-once ingestion |
| Replayable by design | Full historical recovery |

---

## Features

### Event Ingestion
- API-first REST interface
- Database-enforced idempotency using `Idempotency-Key`
- Safe under retries, crashes, and concurrent requests

### Durable Storage
- PostgreSQL as the authoritative event store
- Indexed for time-range queries and replay

### Delivery & Fan-out
- Redis Streams for append-only delivery
- Consumer groups for fan-out
- Per-consumer offsets tracked by Redis

### Failure Handling
- Automatic retries with exponential backoff
- Configurable retry limits
- Dead-letter queue for permanently failed events

### Replay & Backfill
- Replay events by time range
- Reset consumer offsets to reprocess history
- No downtime required

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go |
| API | net/http (REST) |
| Database | PostgreSQL |
| Streaming | Redis Streams |
| Containerization | Docker & Docker Compose |

---

## Getting Started

### Prerequisites

Make sure you have:
- Docker
- Docker Compose
- Git

> No local Go or Postgres installation required.

### Clone the Repository

```bash
git clone https://github.com/krishgondaliya/eventrail-ingestion.git
cd eventrail-ingestion
```

### Start the System

```bash
docker compose up --build
```

You should see logs like:

```
EventRail API starting on :8080
stream worker started (group=eventrail.cg consumer=api-1)
```

### Health Check

```bash
curl http://localhost:8080/health
```

Expected response:

```json
{
  "status": "ok",
  "postgres": "ok",
  "redis": "ok"
}
```

---

## Usage

### Create an Event (Idempotent)

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/events `
  -Headers @{ "Idempotency-Key" = "example-123" } `
  -ContentType "application/json" `
  -Body '{
    "event_type": "user.created",
    "source": "auth-service",
    "payload": { "user_id": 42 }
  }'
```

> Retrying this request with the same `Idempotency-Key` will return the same event ID without creating duplicates.

### Fetch an Event

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri http://localhost:8080/events/<EVENT_ID>
```

### Replay Events (Backfill)

Re-publish historical events from Postgres into Redis Streams:

```powershell
$from = (Get-Date).AddDays(-1).ToString("o")
$to   = (Get-Date).ToString("o")

Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/replay `
  -ContentType "application/json" `
  -Body "{ `"from`": `"$from`", `"to`": `"$to`", `"limit`": 1000 }"
```

### Consumer Replay (Reset Offsets)

Force consumers to reprocess events from the beginning:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/consumer-groups/set-cursor `
  -ContentType "application/json" `
  -Body '{ "start_id": "0" }'
```

### Inspect Dead-Letter Queue

```bash
docker exec -it eventrail-redis redis-cli
XRANGE eventrail.events.dlq - +
```

---

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `CONSUMER_NAME` | Worker name | `api-1` |
| `MAX_RETRIES` | Max delivery retries | `5` |
| `BASE_BACKOFF_MS` | Initial retry delay (ms) | `500` |

---

## Design Guarantees

- Exactly-once ingestion
- At-least-once delivery
- Deterministic behavior under retries
- Safe recovery after crashes
- Replay without downtime

---

**Every failure mode is explicitly handled.**

---

## License

MIT

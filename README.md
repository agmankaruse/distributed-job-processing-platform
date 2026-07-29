# Distributed Job Processing Platform

A small job platform I built to learn the parts that make background processing reliable instead of stopping at a basic Redis queue

The API accepts jobs and stores their state in PostgreSQL while Redis handles delivery retries leases heartbeats and the dead letter queue

## What it does

- submits jobs through a FastAPI API
- runs jobs across as many worker processes as needed
- tracks queued running succeeded failed and cancelled states
- retries failed jobs with exponential backoff
- uses expiring leases so abandoned jobs can be claimed again
- publishes worker heartbeats so the API can show which workers are alive
- supports idempotency keys to prevent duplicate submissions
- moves jobs that run out of retries to a dead letter queue
- supports cooperative cancellation for running jobs

## Stack

- Python 3.12
- FastAPI
- Redis
- PostgreSQL
- SQLAlchemy
- Docker Compose
- Pytest

## Run it

Copy the environment file if you want to change the defaults

```bash
cp .env.example .env
```

Start the API Redis PostgreSQL and three workers

```bash
docker compose up --build --scale worker=3
```

The API is available at `http://localhost:8000` and the Swagger page is at `http://localhost:8000/docs`

## Try a job

Submit some work

```bash
curl -X POST http://localhost:8000/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: total-001" \
  -d '{"task_type":"add_numbers","payload":{"numbers":[10,20,30]},"max_attempts":3}'
```

Use the returned id to check progress

```bash
curl http://localhost:8000/v1/jobs/JOB_ID
```

Get the result once the job succeeds

```bash
curl http://localhost:8000/v1/jobs/JOB_ID/result
```

Cancel a queued or running job

```bash
curl -X DELETE http://localhost:8000/v1/jobs/JOB_ID
```

See active workers and failed jobs

```bash
curl http://localhost:8000/v1/workers
curl http://localhost:8000/v1/dead-letters
```

## Included task types

`echo`

```json
{"task_type":"echo","payload":{"value":"hello"}}
```

`add_numbers`

```json
{"task_type":"add_numbers","payload":{"numbers":[1,2,3]}}
```

`sleep`

```json
{"task_type":"sleep","payload":{"seconds":5}}
```

`fail` is there to test retries and the dead letter queue

```json
{"task_type":"fail","payload":{"message":"testing retries"},"max_attempts":3}
```

## How jobs move through the system

```text
client
  |
  v
FastAPI -> PostgreSQL
  |
  v
Redis ready queue -> worker -> result
                        |
                        +-> delayed retry queue
                        |
                        +-> dead letter queue
```

Workers claim a job and attach an expiring lease while it runs

The lease is renewed in the background and another worker puts the job back on the ready queue if the lease expires

Job completion checks the worker id before saving a result so a worker with an old lease cannot overwrite newer work

## Reliability choices

Redis delivery is at least once so task handlers should be safe to run more than once

The idempotency key protects job creation but it does not make side effects inside a task idempotent

PostgreSQL is the source of truth for job state and Redis only owns short lived delivery data

Retries use exponential backoff capped by `MAX_RETRY_DELAY_SECONDS`

Running jobs check for cancellation at task checkpoints

## Test it

Install the project with its dev dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

On Windows use `.venv\Scripts\activate` instead

Run the tests and linter

```bash
pytest
ruff check .
```

The test suite uses SQLite and a fake Redis instance so it does not need Docker

## API routes

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/jobs` | submit a job |
| `GET` | `/v1/jobs/{job_id}` | inspect job state |
| `GET` | `/v1/jobs/{job_id}/result` | retrieve a completed result |
| `DELETE` | `/v1/jobs/{job_id}` | request cancellation |
| `GET` | `/v1/workers` | list live workers |
| `GET` | `/v1/dead-letters` | inspect jobs that exhausted retries |
| `GET` | `/health/live` | process health |
| `GET` | `/health/ready` | database and Redis readiness |

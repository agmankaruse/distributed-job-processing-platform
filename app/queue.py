import json
import time
from datetime import UTC, datetime
from typing import Any

from redis import Redis

from app.config import get_settings

## redis keys

READY_QUEUE = "jobs:ready"
DELAYED_QUEUE = "jobs:delayed"
LEASES = "jobs:leases"
DEAD_LETTER_QUEUE = "jobs:dead"
WORKER_PREFIX = "workers:"

## claim and lease in the same redis operation

CLAIM_SCRIPT = """
local job_id = redis.call('LPOP', KEYS[1])
if job_id then
  redis.call('ZADD', KEYS[2], ARGV[1], job_id)
end
return job_id
"""


class JobQueue:
    def __init__(self, client: Redis, lease_seconds: int) -> None:
        self.client = client
        self.lease_seconds = lease_seconds

    def enqueue(self, job_id: str) -> None:
        self.client.rpush(READY_QUEUE, job_id)

    def claim(self) -> str | None:
        lease_until = time.time() + self.lease_seconds
        job_id = self.client.eval(CLAIM_SCRIPT, 2, READY_QUEUE, LEASES, lease_until)
        return str(job_id) if job_id else None

    def renew_lease(self, job_id: str) -> None:
        self.client.zadd(LEASES, {job_id: time.time() + self.lease_seconds}, xx=True)

    def acknowledge(self, job_id: str) -> None:
        self.client.zrem(LEASES, job_id)

    def remove(self, job_id: str) -> None:
        pipeline = self.client.pipeline()
        pipeline.lrem(READY_QUEUE, 0, job_id)
        pipeline.zrem(DELAYED_QUEUE, job_id)
        pipeline.zrem(LEASES, job_id)
        pipeline.execute()

    def retry_later(self, job_id: str, delay_seconds: int) -> None:
        pipeline = self.client.pipeline()
        pipeline.zrem(LEASES, job_id)
        pipeline.zadd(DELAYED_QUEUE, {job_id: time.time() + delay_seconds})
        pipeline.execute()

    def promote_due_jobs(self) -> int:
        job_ids = self.client.zrangebyscore(DELAYED_QUEUE, 0, time.time())
        if not job_ids:
            return 0
        pipeline = self.client.pipeline()
        for job_id in job_ids:
            pipeline.zrem(DELAYED_QUEUE, job_id)
            pipeline.rpush(READY_QUEUE, job_id)
        pipeline.execute()
        return len(job_ids)

    def recover_expired_leases(self) -> int:
        job_ids = self.client.zrangebyscore(LEASES, 0, time.time())
        if not job_ids:
            return 0
        pipeline = self.client.pipeline()
        for job_id in job_ids:
            pipeline.zrem(LEASES, job_id)
            pipeline.rpush(READY_QUEUE, job_id)
        pipeline.execute()
        return len(job_ids)

    def dead_letter(self, item: dict[str, Any]) -> None:
        self.client.lpush(DEAD_LETTER_QUEUE, json.dumps(item, default=str))

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        return [json.loads(item) for item in self.client.lrange(DEAD_LETTER_QUEUE, 0, limit - 1)]

    def heartbeat(
        self,
        worker_id: str,
        current_job_id: str | None,
        ttl_seconds: int,
    ) -> None:
        payload = json.dumps(
            {
                "worker_id": worker_id,
                "last_seen": datetime.now(UTC).isoformat(),
                "current_job_id": current_job_id,
            }
        )
        self.client.set(f"{WORKER_PREFIX}{worker_id}", payload, ex=ttl_seconds)

    def workers(self) -> list[dict[str, Any]]:
        workers = []
        for key in self.client.scan_iter(f"{WORKER_PREFIX}*"):
            payload = self.client.get(key)
            if payload:
                workers.append(json.loads(payload))
        return sorted(workers, key=lambda item: item["worker_id"])


settings = get_settings()
redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
job_queue = JobQueue(redis_client, settings.job_lease_seconds)


def get_queue() -> JobQueue:
    return job_queue

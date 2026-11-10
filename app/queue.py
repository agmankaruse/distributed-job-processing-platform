import json
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from redis import Redis

from app.config import get_settings

## redis keys

READY_QUEUE = "jobs:ready"
DELAYED_QUEUE = "jobs:delayed"
LEASES = "jobs:leases"
LEASE_OWNERS = "jobs:lease-owners"
DEAD_LETTER_QUEUE = "jobs:dead"
WORKER_PREFIX = "workers:"

## claim and lease in the same redis operation

CLAIM_SCRIPT = """
while true do
  local job_id = redis.call('LPOP', KEYS[1])
  if not job_id then return nil end
  if not redis.call('ZSCORE', KEYS[2], job_id) then
    redis.call('ZADD', KEYS[2], ARGV[1], job_id)
    redis.call('HSET', KEYS[3], job_id, ARGV[2])
    return job_id
  end
end
"""

OWNER_SCRIPT = """
local expiry = redis.call('ZSCORE', KEYS[1], ARGV[1])
if redis.call('HGET', KEYS[2], ARGV[1]) == ARGV[2]
   and expiry and tonumber(expiry) > tonumber(ARGV[3]) then
  return 1
end
return 0
"""

RENEW_SCRIPT = OWNER_SCRIPT.replace(
    "return 1", "redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1]); return 1"
)

ACK_SCRIPT = """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""

RETRY_SCRIPT = ACK_SCRIPT.replace(
    "return 1", "redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1]); return 1"
)

# Selection and movement happen in one Redis operation. Competing recovery
# workers cannot enqueue the same expired lease or delayed retry twice.
MOVE_DUE_SCRIPT = """
local job_ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, job_id in ipairs(job_ids) do
  redis.call('ZREM', KEYS[1], job_id)
  if #KEYS == 3 then redis.call('HDEL', KEYS[3], job_id) end
  redis.call('RPUSH', KEYS[2], job_id)
end
return #job_ids
"""


class JobQueue:
    def __init__(self, client: Redis, lease_seconds: int) -> None:
        self.client = client
        self.lease_seconds = lease_seconds

    def enqueue(self, job_id: str) -> None:
        self.client.rpush(READY_QUEUE, job_id)

    def claim(self, token: str | None = None) -> str | None:
        lease_until = time.time() + self.lease_seconds
        job_id = self.client.eval(
            CLAIM_SCRIPT, 3, READY_QUEUE, LEASES, LEASE_OWNERS, lease_until, token or uuid4().hex
        )
        return str(job_id) if job_id else None

    def owns_lease(self, job_id: str, token: str) -> bool:
        return bool(
            self.client.eval(OWNER_SCRIPT, 2, LEASES, LEASE_OWNERS, job_id, token, time.time())
        )

    def renew_lease(self, job_id: str, token: str) -> bool:
        now = time.time()
        return bool(
            self.client.eval(
                RENEW_SCRIPT, 2, LEASES, LEASE_OWNERS, job_id, token, now, now + self.lease_seconds
            )
        )

    def acknowledge(self, job_id: str, token: str) -> bool:
        return bool(self.client.eval(ACK_SCRIPT, 2, LEASES, LEASE_OWNERS, job_id, token))

    def remove(self, job_id: str) -> None:
        pipeline = self.client.pipeline()
        pipeline.lrem(READY_QUEUE, 0, job_id)
        pipeline.zrem(DELAYED_QUEUE, job_id)
        pipeline.zrem(LEASES, job_id)
        pipeline.hdel(LEASE_OWNERS, job_id)
        pipeline.execute()

    def retry_later(self, job_id: str, delay_seconds: int, token: str) -> bool:
        return bool(
            self.client.eval(
                RETRY_SCRIPT,
                3,
                LEASES,
                LEASE_OWNERS,
                DELAYED_QUEUE,
                job_id,
                token,
                time.time() + delay_seconds,
            )
        )

    def promote_due_jobs(self) -> int:
        return int(self.client.eval(MOVE_DUE_SCRIPT, 2, DELAYED_QUEUE, READY_QUEUE, time.time()))

    def recover_expired_leases(self) -> int:
        return int(
            self.client.eval(MOVE_DUE_SCRIPT, 3, LEASES, READY_QUEUE, LEASE_OWNERS, time.time())
        )

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

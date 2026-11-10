import time


def test_delayed_jobs_are_promoted(queue):
    queue.enqueue("job-1")
    assert queue.claim("owner-1") == "job-1"
    queue.retry_later("job-1", 0, "owner-1")

    assert queue.promote_due_jobs() == 1
    assert queue.claim() == "job-1"


def test_expired_lease_is_recovered(queue):
    queue.enqueue("job-1")
    assert queue.claim() == "job-1"
    queue.client.zadd("jobs:leases", {"job-1": time.time() - 1})

    assert queue.recover_expired_leases() == 1
    assert queue.claim() == "job-1"


def test_worker_heartbeat_is_listed(queue):
    queue.heartbeat("worker-1", "job-1", ttl_seconds=10)

    workers = queue.workers()

    assert workers[0]["worker_id"] == "worker-1"
    assert workers[0]["current_job_id"] == "job-1"


def test_old_lease_cannot_renew_acknowledge_or_retry_new_claim(queue):
    queue.enqueue("job-1")
    assert queue.claim("old") == "job-1"
    queue.client.zadd("jobs:leases", {"job-1": time.time() - 1})
    assert not queue.renew_lease("job-1", "old")
    assert queue.recover_expired_leases() == 1
    assert queue.claim("new") == "job-1"

    assert not queue.renew_lease("job-1", "old")
    assert not queue.acknowledge("job-1", "old")
    assert not queue.retry_later("job-1", 0, "old")
    assert queue.owns_lease("job-1", "new")
    assert queue.renew_lease("job-1", "new")
    assert queue.acknowledge("job-1", "new")


def test_duplicate_delivery_does_not_replace_active_lease(queue):
    queue.enqueue("job-1")
    queue.enqueue("job-1")
    assert queue.claim("first") == "job-1"
    assert queue.claim("duplicate") is None
    assert queue.owns_lease("job-1", "first")


def test_competing_recovery_workers_enqueue_once(queue):
    from concurrent.futures import ThreadPoolExecutor

    queue.enqueue("job-1")
    assert queue.claim("old") == "job-1"
    queue.client.zadd("jobs:leases", {"job-1": time.time() - 1})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: queue.recover_expired_leases(), range(2)))
    assert sum(results) == 1
    assert queue.client.lrange("jobs:ready", 0, -1) == ["job-1"]


def test_competing_retry_promoters_enqueue_once(queue):
    from concurrent.futures import ThreadPoolExecutor

    queue.enqueue("job-1")
    assert queue.claim("owner") == "job-1"
    assert queue.retry_later("job-1", 0, "owner")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: queue.promote_due_jobs(), range(2)))
    assert sum(results) == 1
    assert queue.client.lrange("jobs:ready", 0, -1) == ["job-1"]

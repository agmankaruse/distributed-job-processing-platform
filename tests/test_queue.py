import time


def test_delayed_jobs_are_promoted(queue):
    queue.retry_later("job-1", 0)

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

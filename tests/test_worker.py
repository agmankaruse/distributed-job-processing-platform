from app.config import Settings
from app.models import Job, JobStatus
from app.worker import Worker


def make_worker(queue, session_factory):
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        redis_url="redis://unused:6379/0",
        worker_poll_seconds=0,
        job_lease_seconds=5,
        max_retry_delay_seconds=0,
    )
    return Worker(queue, session_factory, settings, worker_id="test-worker")


def test_worker_completes_job(queue, session_factory):
    with session_factory() as db:
        job = Job(task_type="add_numbers", payload={"numbers": [4, 6]}, max_attempts=3)
        db.add(job)
        db.commit()
        job_id = job.id
    queue.enqueue(job_id)

    assert make_worker(queue, session_factory).run_once()

    with session_factory() as db:
        finished = db.get(Job, job_id)
        assert finished.status == JobStatus.succeeded
        assert finished.result == {"sum": 10.0}
        assert finished.attempts == 1


def test_worker_retries_then_dead_letters(queue, session_factory):
    with session_factory() as db:
        job = Job(task_type="fail", payload={"message": "boom"}, max_attempts=2)
        db.add(job)
        db.commit()
        job_id = job.id
    queue.enqueue(job_id)
    worker = make_worker(queue, session_factory)

    assert worker.run_once()
    assert worker.run_once()

    with session_factory() as db:
        failed = db.get(Job, job_id)
        assert failed.status == JobStatus.failed
        assert failed.attempts == 2
    assert queue.dead_letters()[0]["job_id"] == job_id


def test_worker_recovers_job_after_another_worker_dies(queue, session_factory):
    import time

    with session_factory() as db:
        job = Job(
            task_type="add_numbers",
            payload={"numbers": [4, 6]},
            max_attempts=3,
            status=JobStatus.running,
            attempts=1,
            claimed_by="dead-worker",
        )
        db.add(job)
        db.commit()
        job_id = job.id
    queue.enqueue(job_id)
    assert queue.claim("dead-lease") == job_id
    queue.client.zadd("jobs:leases", {job_id: time.time() - 1})

    assert make_worker(queue, session_factory).run_once()
    with session_factory() as db:
        finished = db.get(Job, job_id)
        assert finished.status == JobStatus.succeeded
        assert finished.result == {"sum": 10.0}
        assert finished.attempts == 2
    assert queue.client.zscore("jobs:leases", job_id) is None


def test_stale_worker_cannot_finish_or_fail_reassigned_job(queue, session_factory):
    import time

    old = make_worker(queue, session_factory)
    old.worker_id = "old-worker"
    old.claim_token = "old-lease"
    with session_factory() as db:
        job = Job(
            task_type="echo",
            payload={"value": "new"},
            max_attempts=3,
            status=JobStatus.running,
            attempts=2,
            claimed_by="new-worker",
        )
        db.add(job)
        db.commit()
        job_id = job.id
    queue.enqueue(job_id)
    assert queue.claim("old-lease") == job_id
    queue.client.zadd("jobs:leases", {job_id: time.time() - 1})
    queue.recover_expired_leases()
    assert queue.claim("new-lease") == job_id

    assert not old._mark_succeeded(job_id, {"value": "stale"})
    old._handle_failure(job_id, RuntimeError("stale failure"))
    assert queue.owns_lease(job_id, "new-lease")
    with session_factory() as db:
        active = db.get(Job, job_id)
        assert active.status == JobStatus.running
        assert active.claimed_by == "new-worker"
        assert active.result is None
        assert active.error is None


def test_recovery_after_committed_result_does_not_repeat_task(queue, session_factory):
    import time

    with session_factory() as db:
        job = Job(
            task_type="fail",
            payload={},
            max_attempts=3,
            attempts=1,
            status=JobStatus.succeeded,
            result={"saved": True},
        )
        db.add(job)
        db.commit()
        job_id = job.id
    queue.enqueue(job_id)
    assert queue.claim("crashed-before-ack") == job_id
    queue.client.zadd("jobs:leases", {job_id: time.time() - 1})
    assert make_worker(queue, session_factory).run_once()
    with session_factory() as db:
        job = db.get(Job, job_id)
        assert job.attempts == 1
        assert job.result == {"saved": True}
        assert job.status == JobStatus.succeeded


def test_cancelled_job_is_not_executed_after_recovery(queue, session_factory):
    import time

    with session_factory() as db:
        job = Job(
            task_type="fail",
            payload={},
            max_attempts=3,
            attempts=1,
            status=JobStatus.running,
            claimed_by="dead-worker",
            cancel_requested=True,
        )
        db.add(job)
        db.commit()
        job_id = job.id
    queue.enqueue(job_id)
    assert queue.claim("dead-lease") == job_id
    queue.client.zadd("jobs:leases", {job_id: time.time() - 1})
    assert make_worker(queue, session_factory).run_once()
    with session_factory() as db:
        job = db.get(Job, job_id)
        assert job.status == JobStatus.cancelled
        assert job.attempts == 1

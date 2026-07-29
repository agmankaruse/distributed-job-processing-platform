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

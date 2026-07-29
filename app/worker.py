import logging
import os
import socket
import threading
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.database import SessionLocal, create_tables
from app.models import Job, JobStatus, utcnow
from app.queue import JobQueue, job_queue
from app.tasks import JobCancelled, TaskContext, run_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("worker")

## lease renewal


class LeaseKeeper:
    def __init__(self, queue: JobQueue, job_id: str, lease_seconds: int) -> None:
        self.queue = queue
        self.job_id = job_id
        self.interval = max(1, lease_seconds // 3)
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "LeaseKeeper":
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stopped.set()
        self.thread.join(timeout=self.interval + 1)

    def _run(self) -> None:
        while not self.stopped.wait(self.interval):
            self.queue.renew_lease(self.job_id)


## worker lifecycle


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        session_factory: type[Session] | object,
        settings: Settings,
        worker_id: str | None = None,
    ) -> None:
        self.queue = queue
        self.session_factory = session_factory
        self.settings = settings
        self.worker_id = worker_id or self._make_worker_id()
        self.current_job_id: str | None = None
        self.stopped = threading.Event()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)

    @staticmethod
    def _make_worker_id() -> str:
        return f"{socket.gethostname()}-{os.getpid()}-{str(uuid4())[:8]}"

    def run_forever(self) -> None:
        create_tables()
        self.heartbeat_thread.start()
        logger.info("worker started id=%s", self.worker_id)
        try:
            while not self.stopped.is_set():
                self.run_once()
        finally:
            self.stopped.set()
            self.heartbeat_thread.join(timeout=self.settings.worker_heartbeat_seconds + 1)
            logger.info("worker stopped id=%s", self.worker_id)

    def run_once(self) -> bool:
        # recover work before taking the next job
        self.queue.promote_due_jobs()
        self.queue.recover_expired_leases()
        job_id = self.queue.claim()
        if not job_id:
            self.stopped.wait(self.settings.worker_poll_seconds)
            return False

        self.current_job_id = job_id
        try:
            self._execute(job_id)
        finally:
            self.current_job_id = None
        return True

    def stop(self) -> None:
        self.stopped.set()

    def _execute(self, job_id: str) -> None:
        with self.session_factory() as db:
            job = db.get(Job, job_id)
            if not job or job.status in {
                JobStatus.succeeded,
                JobStatus.failed,
                JobStatus.cancelled,
            }:
                self.queue.acknowledge(job_id)
                return
            if job.cancel_requested:
                self._mark_cancelled(db, job)
                self.queue.acknowledge(job_id)
                return

            job.status = JobStatus.running
            job.claimed_by = self.worker_id
            job.attempts += 1
            job.started_at = job.started_at or utcnow()
            job.error = None
            db.commit()
            payload = job.payload
            task_type = job.task_type

        context = TaskContext(is_cancelled=lambda: self._is_cancelled(job_id))
        try:
            with LeaseKeeper(self.queue, job_id, self.settings.job_lease_seconds):
                result = run_task(task_type, payload, context)
            self._mark_succeeded(job_id, result)
            self.queue.acknowledge(job_id)
            logger.info("job succeeded id=%s", job_id)
        except JobCancelled:
            with self.session_factory() as db:
                job = db.get(Job, job_id)
                if job and job.claimed_by == self.worker_id:
                    self._mark_cancelled(db, job)
            self.queue.acknowledge(job_id)
            logger.info("job cancelled id=%s", job_id)
        except Exception as exc:
            self._handle_failure(job_id, exc)

    def _mark_succeeded(self, job_id: str, result: object) -> None:
        with self.session_factory() as db:
            job = db.get(Job, job_id)
            if not job or job.claimed_by != self.worker_id:
                return
            job.status = JobStatus.succeeded
            job.result = result
            job.completed_at = utcnow()
            db.commit()

    def _handle_failure(self, job_id: str, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"[:2000]
        with self.session_factory() as db:
            job = db.get(Job, job_id)
            if not job or job.claimed_by != self.worker_id:
                self.queue.acknowledge(job_id)
                return

            job.error = message
            job.claimed_by = None
            if job.cancel_requested:
                self._mark_cancelled(db, job)
                self.queue.acknowledge(job_id)
                return

            if job.attempts < job.max_attempts:
                delay = min(2 ** (job.attempts - 1), self.settings.max_retry_delay_seconds)
                job.status = JobStatus.queued
                db.commit()
                self.queue.retry_later(job_id, delay)
                logger.warning(
                    "job retry scheduled id=%s attempt=%s delay=%s",
                    job_id,
                    job.attempts,
                    delay,
                )
                return

            job.status = JobStatus.failed
            job.completed_at = utcnow()
            db.commit()
            self.queue.acknowledge(job_id)
            self.queue.dead_letter(
                {
                    "job_id": job.id,
                    "task_type": job.task_type,
                    "attempts": job.attempts,
                    "error": message,
                    "failed_at": datetime.now(UTC).isoformat(),
                }
            )
            logger.error("job failed id=%s attempts=%s", job_id, job.attempts)

    def _is_cancelled(self, job_id: str) -> bool:
        with self.session_factory() as db:
            job = db.get(Job, job_id)
            return not job or job.cancel_requested or job.claimed_by != self.worker_id

    @staticmethod
    def _mark_cancelled(db: Session, job: Job) -> None:
        job.status = JobStatus.cancelled
        job.cancel_requested = True
        job.completed_at = utcnow()
        db.commit()

    def _heartbeat_loop(self) -> None:
        while not self.stopped.is_set():
            self.queue.heartbeat(
                self.worker_id,
                self.current_job_id,
                self.settings.worker_heartbeat_ttl_seconds,
            )
            self.stopped.wait(self.settings.worker_heartbeat_seconds)


def run() -> None:
    Worker(job_queue, SessionLocal, get_settings()).run_forever()


if __name__ == "__main__":
    run()

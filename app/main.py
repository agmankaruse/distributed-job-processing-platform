from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from redis.exceptions import RedisError
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import create_tables, get_db
from app.models import Job, JobStatus, utcnow
from app.queue import JobQueue, get_queue
from app.schemas import (
    CancelView,
    DeadLetterView,
    JobCreate,
    JobView,
    ResultView,
    WorkerView,
)
from app.tasks import TASKS


@asynccontextmanager
async def lifespan(_: FastAPI):
    create_tables()
    yield


settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="submit work and let a pool of workers handle it",
    lifespan=lifespan,
)

Database = Annotated[Session, Depends(get_db)]
Queue = Annotated[JobQueue, Depends(get_queue)]


@app.get("/health/live", tags=["health"])
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
def ready(db: Database, queue: Queue) -> dict[str, str]:
    try:
        db.execute(text("select 1"))
        queue.client.ping()
    except (RedisError, SQLAlchemyError, OSError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="dependencies are unavailable") from exc
    return {"status": "ready"}


@app.post("/v1/jobs", response_model=JobView, tags=["jobs"])
def create_job(
    request: JobCreate,
    response: Response,
    db: Database,
    queue: Queue,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> Job:
    if request.task_type not in TASKS:
        raise HTTPException(
            status_code=422,
            detail=f"unknown task type {request.task_type}",
        )

    normalized_key = idempotency_key.strip() if idempotency_key else None
    if normalized_key and len(normalized_key) > 200:
        raise HTTPException(status_code=422, detail="idempotency key is too long")

    if normalized_key:
        existing = db.scalar(select(Job).where(Job.idempotency_key == normalized_key))
        if existing:
            response.status_code = status.HTTP_200_OK
            return existing

    job = Job(
        task_type=request.task_type,
        payload=request.payload,
        max_attempts=request.max_attempts,
        idempotency_key=normalized_key,
    )
    db.add(job)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        if not normalized_key:
            raise
        existing = db.scalar(select(Job).where(Job.idempotency_key == normalized_key))
        if not existing:
            raise
        response.status_code = status.HTTP_200_OK
        return existing

    try:
        queue.enqueue(job.id)
    except RedisError as exc:
        db.delete(job)
        db.commit()
        raise HTTPException(status_code=503, detail="job queue is unavailable") from exc

    response.status_code = status.HTTP_201_CREATED
    return job


@app.get("/v1/jobs/{job_id}", response_model=JobView, tags=["jobs"])
def get_job(job_id: str, db: Database) -> Job:
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/v1/jobs/{job_id}/result", response_model=ResultView, tags=["jobs"])
def get_result(job_id: str, db: Database) -> ResultView:
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status != JobStatus.succeeded:
        raise HTTPException(status_code=409, detail=f"job is {job.status}")
    return ResultView(job_id=job.id, result=job.result)


@app.delete("/v1/jobs/{job_id}", response_model=CancelView, tags=["jobs"])
def cancel_job(job_id: str, db: Database, queue: Queue) -> CancelView:
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status in {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}:
        return CancelView(
            job_id=job.id,
            status=JobStatus(job.status),
            cancel_requested=job.cancel_requested,
        )

    job.cancel_requested = True
    if job.status == JobStatus.queued:
        job.status = JobStatus.cancelled
        job.completed_at = utcnow()
        queue.remove(job.id)
    db.commit()
    return CancelView(
        job_id=job.id,
        status=JobStatus(job.status),
        cancel_requested=job.cancel_requested,
    )


@app.get("/v1/workers", response_model=list[WorkerView], tags=["workers"])
def list_workers(queue: Queue) -> list[dict]:
    return queue.workers()


@app.get("/v1/dead-letters", response_model=list[DeadLetterView], tags=["jobs"])
def list_dead_letters(queue: Queue, limit: int = 100) -> list[dict]:
    if not 1 <= limit <= 500:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 500")
    return queue.dead_letters(limit)


def run() -> None:
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    run()

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.models import JobStatus


class JobCreate(BaseModel):
    task_type: str = Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_]*$")
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=10)


class JobView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_type: str
    payload: dict[str, Any]
    status: JobStatus
    result: Any | None
    error: str | None
    attempts: int
    max_attempts: int
    cancel_requested: bool
    claimed_by: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class ResultView(BaseModel):
    job_id: str
    result: Any


class CancelView(BaseModel):
    job_id: str
    status: JobStatus
    cancel_requested: bool


class WorkerView(BaseModel):
    worker_id: str
    last_seen: datetime
    current_job_id: str | None


class DeadLetterView(BaseModel):
    job_id: str
    task_type: str
    attempts: int
    error: str
    failed_at: datetime

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "distributed job processing platform"
    database_url: str = "postgresql+psycopg://jobs:jobs@localhost:5432/jobs"
    redis_url: str = "redis://localhost:6379/0"
    worker_poll_seconds: float = 1.0
    worker_heartbeat_seconds: float = 5.0
    worker_heartbeat_ttl_seconds: int = 15
    job_lease_seconds: int = 30
    max_retry_delay_seconds: int = 60


@lru_cache
def get_settings() -> Settings:
    return Settings()

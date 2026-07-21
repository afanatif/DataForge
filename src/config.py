from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://dataforge:dataforge@localhost:5432/dataforge"
    sync_database_url: str = "postgresql://dataforge:dataforge@localhost:5432/dataforge"
    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379"
    pipeline_schedule_interval_minutes: int = 15
    log_level: str = "INFO"
    checkpoint_dir: str = "/data/checkpoints"  # DF-09: persistent path, overridden via CHECKPOINT_DIR env var

    class Config:
        env_file = ".env"


settings = Settings()

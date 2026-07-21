import uuid
import asyncio
import zlib
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import update, select, text
from src.models.pipeline import Pipeline, PipelineRun
from src.config import settings
from src.pipeline.watermark import get_watermark, advance_watermark
from src.utils.logging import get_logger

logger = get_logger(__name__)

engine = create_async_engine(settings.database_url)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# DF-09 (FIXED): Spark checkpoint directory now comes from settings.checkpoint_dir
# instead of a hardcoded /tmp path. /tmp is ephemeral on containers — it is wiped
# on every restart, scale-down, or deploy. Without a valid checkpoint, Spark has
# no recovery point and reprocesses the entire Kafka topic from the beginning.
# CHECKPOINT_DIR is now backed by a persistent volume (see docker-compose.yml)
# and is configurable via the CHECKPOINT_DIR env var (see .env.example).

# DF-15 (FIXED): Previously used a non-atomic check-then-set on is_running,
# which allowed two concurrent scheduler ticks / API calls to both pass the
# check before either wrote is_running=True, resulting in duplicate concurrent
# runs. Now uses a Postgres session-level advisory lock (pg_try_advisory_lock),
# which is atomic at the database level and auto-releases if the connection
# drops (e.g. process crash), preventing stale locks.

CHECKPOINT_DIR = settings.checkpoint_dir


def _pipeline_lock_key(pipeline_id: str) -> int:
    """Deterministically map a pipeline_id string to a 32-bit signed int
    for use as a Postgres advisory lock key."""
    return zlib.crc32(pipeline_id.encode("utf-8")) & 0x7FFFFFFF


async def run_pipeline(pipeline_id: str) -> str:
    lock_key = _pipeline_lock_key(pipeline_id)

    # DF-15 fix: hold a single connection for the advisory lock's lifetime.
    # pg_try_advisory_lock is session-scoped, so the lock and its release
    # must happen on the same underlying connection.
    lock_conn = await engine.connect()
    try:
        acquired = (
            await lock_conn.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_key}
            )
        ).scalar()

        if not acquired:
            logger.warning("pipeline_already_running", pipeline_id=pipeline_id)
            await lock_conn.close()
            return "already_running"

        async with SessionLocal() as session:
            result = await session.execute(
                select(Pipeline).where(Pipeline.id == pipeline_id)
            )
            pipeline = result.scalar_one_or_none()
            if not pipeline:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            await session.execute(
                update(Pipeline)
                .where(Pipeline.id == pipeline_id)
                .values(is_running=True, last_run_at=datetime.utcnow())
            )
            await session.commit()

        run_id = str(uuid.uuid4())
        async with SessionLocal() as session:
            run = PipelineRun(
                id=run_id,
                pipeline_id=pipeline_id,
                started_at=datetime.utcnow(),
                status="running",
            )
            session.add(run)
            await session.commit()

        try:
            watermark = get_watermark(pipeline_id)
            logger.info("pipeline_run_started", pipeline_id=pipeline_id, run_id=run_id, watermark=watermark.isoformat())

            # Spark job execution (simplified — real impl calls run_transactions_job)
            await asyncio.sleep(0)

            new_watermark = advance_watermark(pipeline_id)

            async with SessionLocal() as session:
                await session.execute(
                    update(PipelineRun)
                    .where(PipelineRun.id == run_id)
                    .values(
                        status="completed",
                        finished_at=datetime.utcnow(),
                    )
                )
                await session.commit()

            logger.info("pipeline_run_completed", pipeline_id=pipeline_id, run_id=run_id)
            return run_id

        except Exception as e:
            logger.error("pipeline_run_failed", pipeline_id=pipeline_id, run_id=run_id, error=str(e))
            async with SessionLocal() as session:
                await session.execute(
                    update(PipelineRun)
                    .where(PipelineRun.id == run_id)
                    .values(status="failed", error_message=str(e), finished_at=datetime.utcnow())
                )
                await session.commit()
            raise

        finally:
            async with SessionLocal() as session:
                await session.execute(
                    update(Pipeline)
                    .where(Pipeline.id == pipeline_id)
                    .values(is_running=False)
                )
                await session.commit()

    finally:
        # DF-15 fix: always release the advisory lock and close the connection,
        # even if an exception was raised above.
        await lock_conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
        await lock_conn.close()
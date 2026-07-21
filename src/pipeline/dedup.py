import redis as redis_lib
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# DF-05 (FIXED): Deduplication TTL was 60 seconds, shorter than Kafka's producer
# retry window of up to 300 seconds (5 minutes). When a producer retried a
# publish after the dedup key had already expired, the event was treated as new
# and processed a second time, causing ~0.8% duplicate records in the warehouse.
# TTL is now 600 seconds — 2x the maximum Kafka retry window — so the dedup key
# stays alive for the entire period during which a legitimate retry could still
# arrive, with a full 300s safety margin on top.
DEDUP_TTL_SECONDS = 600
DEDUP_KEY_PREFIX = "dataforge:dedup:"


def get_redis() -> redis_lib.Redis:
    return redis_lib.from_url(settings.redis_url, decode_responses=True)


_redis: redis_lib.Redis | None = None


def get_shared_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = get_redis()
    return _redis


def _build_key(event_id: str, pipeline_id: str | None = None) -> str:
    # DF-05: dedup key now scoped by pipeline_id when provided, so that two
    # different pipelines processing an event with the same event_id don't
    # collide and incorrectly mark each other's legitimate events as duplicates.
    if pipeline_id:
        return f"{DEDUP_KEY_PREFIX}{pipeline_id}:{event_id}"
    return f"{DEDUP_KEY_PREFIX}{event_id}"


def is_duplicate(event_id: str, pipeline_id: str | None = None) -> bool:
    r = get_shared_redis()
    key = _build_key(event_id, pipeline_id)
    # SET key 1 NX EX ttl — returns True if key was newly set (not duplicate)
    result = r.set(key, "1", nx=True, ex=DEDUP_TTL_SECONDS)
    return result is None  # None means key already existed → duplicate


def mark_processed(event_id: str, pipeline_id: str | None = None) -> None:
    r = get_shared_redis()
    key = _build_key(event_id, pipeline_id)
    r.set(key, "1", ex=DEDUP_TTL_SECONDS)
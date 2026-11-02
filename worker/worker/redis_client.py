"""Redis connection settings for long-running processes.

A pooled connection goes stale when Redis restarts (e.g. `docker compose up` recreates it).
redis-py's defaults then fail the next command with "Connection reset" / "Connection closed
by server"; with retries on connection errors the client reconnects instead. The same
settings are used by the API (api/api/app.py).
"""

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError


def connect(url: str) -> Redis:
    return Redis.from_url(
        url,
        decode_responses=True,
        retry=Retry(ExponentialBackoff(cap=2.0, base=0.1), retries=5),
        retry_on_error=[ConnectionError, TimeoutError],
        health_check_interval=30,
    )

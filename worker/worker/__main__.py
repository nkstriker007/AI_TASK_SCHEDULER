"""python -m worker: run one worker process until SIGTERM / Ctrl-C.

Configuration comes from the environment (see WorkerConfig.from_env):
REDIS_URL, WORKER_ID, WORKER_CAPABILITIES (default "sim"), WORKER_CONCURRENCY,
SIM_TIME_SCALE, SIM_DEFAULT_SECONDS.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from redis.asyncio import Redis

from .main import Worker, WorkerConfig


async def _main() -> None:
    config = WorkerConfig.from_env()
    redis = Redis.from_url(config.redis_url, decode_responses=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await Worker(redis, config).run(stop)
    finally:
        await redis.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(_main())


if __name__ == "__main__":
    main()

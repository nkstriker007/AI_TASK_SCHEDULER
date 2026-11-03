"""python -m worker: run one worker process until SIGTERM / Ctrl-C.

Configuration comes from the environment (see WorkerConfig.from_env):
REDIS_URL, WORKER_ID, WORKER_CAPABILITIES (default "sim"), WORKER_CONCURRENCY,
SIM_TIME_SCALE, SIM_DEFAULT_SECONDS, and the SIM_FAULT* settings (worker/faults.py).

SIGTERM / Ctrl-C once: finish the attempts in progress, then exit. Twice: exit now.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from .main import Worker, WorkerConfig

log = logging.getLogger("worker")
from .redis_client import connect


async def _main() -> None:
    config = WorkerConfig.from_env()
    redis = connect(config.redis_url)
    stop = asyncio.Event()
    worker = Worker(redis, config)
    loop = asyncio.get_running_loop()

    def on_signal() -> None:
        # First signal: graceful, take no new work and finish the attempts in progress.
        # Second signal: stop now; unfinished attempts are recovered by atsd's deadline.
        if stop.is_set():
            log.warning("second stop signal: abandoning %d attempt(s) in progress", len(worker._inflight))
            worker.abort()
        else:
            log.info("stop signal: finishing %d attempt(s) in progress (signal again to stop now)",
                     len(worker._inflight))
            stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, on_signal)
    if config.faults.enabled:
        log.warning("fault injection is ON for sim tasks: %s", config.faults)
    try:
        await worker.run(stop)
    finally:
        await redis.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(_main())


if __name__ == "__main__":
    main()

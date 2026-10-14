"""Shared fixtures. `redis_client` gives each test an empty Redis database.

Uses $ATS_TEST_REDIS_URL when set (CI runs a Redis service), otherwise starts a
throwaway redis-server on a free port, otherwise skips the Redis tests.
"""

import os
import shutil
import socket
import subprocess
import time

import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_url():
    url = os.environ.get("ATS_TEST_REDIS_URL")
    if url:
        yield url
        return
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("no Redis: set ATS_TEST_REDIS_URL or install redis-server")
    port = _free_port()
    proc = subprocess.Popen([binary, "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}/15"
    import redis

    for _ in range(100):
        try:
            redis.Redis.from_url(url).ping()
            break
        except redis.ConnectionError:
            time.sleep(0.05)
    try:
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture
def redis_client(redis_url):
    import redis

    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()

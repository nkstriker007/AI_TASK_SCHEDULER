"""Tests run against a real Redis.

Set ATS_TEST_REDIS_URL to use an existing server (CI does). Otherwise a throwaway
redis-server is started on a free port. The test database is flushed before every
test, so ATS_TEST_REDIS_URL must never point at a Redis holding data you need.
"""

import os
import shutil
import socket
import subprocess
import time

import pytest
import redis


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_server_url():
    url = os.environ.get("ATS_TEST_REDIS_URL")
    if url:
        yield url
        return
    exe = shutil.which("redis-server")
    if not exe:
        pytest.skip("needs redis-server on PATH or ATS_TEST_REDIS_URL")
    port = _free_port()
    proc = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}/0"
    client = redis.Redis.from_url(url)
    for _ in range(100):
        try:
            client.ping()
            break
        except redis.ConnectionError:
            time.sleep(0.05)
    else:
        proc.kill()
        pytest.fail("redis-server did not start")
    yield url
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def redis_url(redis_server_url):
    redis.Redis.from_url(redis_server_url).flushdb()
    return redis_server_url

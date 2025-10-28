import aiohttp
import asyncio
import json
import logging
import os
import time

from typing import Dict, Optional, Union

from .credentials import Credentials
from .exceptions import (
    HTTPError,
    ContentTooLarge,
    InternalServerError,
    InvalidCredentials,
)
from .services.base import APIBase

log = logging.getLogger(__name__)

BACKOFF_FACTOR = 0.5
RATE_LIMIT_DELAY = 0.5
STATUS_FORCELIST = (429, 500, 502, 503, 504)


class AsyncSession:
    def __init__(self, user, secret, credentials_file, max_retries):
        self._credentials = Credentials(user, secret, credentials_file)
        self.session = aiohttp.ClientSession()
        app_env = os.getenv("APP_ENV", "app")
        self.app_env = APIBase._get_app_env(app_env)
        self.max_retries = max_retries
        self._rate_limiter = AsyncRateLimiter(RATE_LIMIT_DELAY) if RATE_LIMIT_DELAY else None

    async def get(self, url: str, params: Optional[Dict] = None):
        full_url = f"{self.app_env}/{url}"
        params = params or {}
        params.update(self._credentials.api_format)

        attempt = 0
        while True:
            response = None
            t0 = time.perf_counter()
            task = asyncio.current_task()
            task_name = task.get_name() if task else "task-unknown"
            log.debug("http_start name=%s attempt=%d url=%s start=%s end=%s",
                    task_name, attempt + 1, full_url, params.get("start"), params.get("end"))
            try:
                if self._rate_limiter:
                    await self._rate_limiter()
                async with self.session.get(full_url, params=params) as response:
                    status = response.status
                    content_type = response.headers.get("Content-Type", "")
                    if status == 413:
                        raise ContentTooLarge
                    text = await response.text()
                    elapsed = time.perf_counter() - t0
                    log.debug("http_done name=%s attempt=%d status=%d elapsed=%.3fs",
                            task_name, attempt + 1, status, elapsed)
                    if status == 200:
                        return self._decode(content_type, text)
                    if status == 401:
                        raise InvalidCredentials
                    if status in STATUS_FORCELIST:
                        raise HTTPError(f"Retryable HTTP {status}: {text}")
                    raise HTTPError(f"Unexpected HTTP {status}: {text}")

            except (aiohttp.ClientError, asyncio.TimeoutError, HTTPError, InternalServerError) as e:
                err = HTTPError(e)
            except (HTTPError, InternalServerError) as e:
                err = e

            if not self.max_retries or attempt >= self.max_retries:
                raise err

            delay = self._compute_backoff(attempt)
            log.debug("http_retry name=%s exc=%r attempt=%d delay=%.3fs", task_name, err, attempt + 1, delay)
            await asyncio.sleep(delay)
            attempt += 1

    def _decode(self, content_type: str, body: str):
        if "application/json" in content_type:
            try:
                return json.loads(body)
            except json.decoder.JSONDecodeError as e:
                raise HTTPError(f"Failed to parse JSON response: {e}")
        return body

    @staticmethod
    def _compute_backoff(attempt: int) -> float:
        retry_after = BACKOFF_FACTOR * (2 ** attempt)
        return max(retry_after, RATE_LIMIT_DELAY)

    async def close(self):
        await self.session.close()


class AsyncRateLimiter:
    """Async fixed-delay limiter applied before each request."""
    def __init__(self, delay: float | None = None):
        self.delay = delay
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def __call__(self) -> None:
        if not self.delay:
            return
        async with self._lock:
            now = time.monotonic()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next = now + self.delay

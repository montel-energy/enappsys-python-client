import json
import os
import requests
import threading
import time

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .credentials import Credentials
from .exceptions import (
    HTTPError,
    ContentTooLarge,
    InternalServerError,
    InvalidCredentials,
)
from .services.base import APIBase

BACKOFF_FACTOR = 0.5
RATE_LIMIT_DELAY = 0.5
STATUS_FORCELIST = (429, 500, 502, 503, 504)


class Session:
    def __init__(self, user, secret, credentials_file, max_retries):
        self._credentials = Credentials(user, secret, credentials_file)
        self.session = requests.Session()
        app_env = os.getenv("APP_ENV", "app")
        self.app_env = APIBase._get_app_env(app_env)
        self._rate_limiter = RateLimiter(RATE_LIMIT_DELAY) if RATE_LIMIT_DELAY else None
        if max_retries > 0:
            self._mount_retry_adapter(max_retries)

    def get(self, url: str, params: dict | None = None):
        """Return objects returned from a GET request to ``url`` with ``params``.

        Parameters
        ----------
        url : str
            The url to fetch.
        params : dict | None, optional
            The query parameters to add to the request.

        Returns
        -------
        response: object
            Object(s) returned from a GET request
        """
        full_url = f"{self.app_env}/{url}"

        params = params or {}
        params.update(self._credentials.api_format)
        try:
            if self._rate_limiter:
                self._rate_limiter()
            response = self.session.get(full_url, params=params)
        except requests.exceptions.RequestException as e:
            raise HTTPError(e)

        if response.status_code == 200:
            return self._get_response_content(response)
        elif response.status_code == 413:
            raise ContentTooLarge
        elif response.status_code == 401:
            raise InvalidCredentials
        else:
            response_error = self._get_response_content(response)
            raise HTTPError(f"Unexpected HTTP {response.status_code}: {response_error}")

    def _get_response_content(self, response):
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                return response.json()
            except json.decoder.JSONDecodeError as e:
                raise HTTPError(f"Failed to parse JSON response: {e}")
        else:
            return response.text

    def _mount_retry_adapter(self, max_retries: int):
        backoff_factor = max(BACKOFF_FACTOR, getattr(self._rate_limiter, "delay", 0.0))
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=STATUS_FORCELIST,
            allowed_methods=("GET", "HEAD", "OPTIONS"),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)


class RateLimiter:
    """Minimal thread-safe fixed-delay limiter applied before each request."""
    def __init__(self, delay: float | None = None):
        self.delay = delay
        self._next = 0.0
        self._lock = threading.RLock()

    def __call__(self) -> None:
        if not self.delay:
            return
        with self._lock:
            now = time.monotonic()
            wait = self._next - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next = now + self.delay

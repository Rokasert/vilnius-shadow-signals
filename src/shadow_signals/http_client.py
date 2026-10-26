import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from tenacity.wait import wait_base

log = logging.getLogger(__name__)

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class RetryableStatusError(Exception):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(f"HTTP {response.status_code} for {response.request.url}")
        self.response = response


@dataclass
class Fetched:
    response: httpx.Response
    attempts: int


@dataclass
class Streamed[T]:
    value: T
    status: int
    headers: dict[str, str]
    attempts: int


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        timeout_s: float,
        max_attempts: int,
        min_interval_s: float = 0.0,
        transport: httpx.BaseTransport | None = None,
        wait: wait_base | None = None,
    ) -> None:
        self._client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=httpx.Timeout(timeout_s, connect=min(timeout_s, 20.0)),
            follow_redirects=True,
            transport=transport,
        )
        self._max_attempts = max_attempts
        self._min_interval_s = min_interval_s
        self._wait = wait if wait is not None else wait_exponential_jitter(initial=2, max=60)
        self._last_request_at = 0.0
        self._lock = threading.Lock()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def get(self, url: str, **kwargs: Any) -> Fetched:
        return self.request("GET", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> Fetched:
        retrying = self._retrying()
        for attempt in retrying:
            with attempt:
                self._throttle()
                response = self._check(self._client.request(method, url, **kwargs))
        return Fetched(response, retrying.statistics["attempt_number"])

    def stream[T](
        self, url: str, consume: Callable[[httpx.Response], T], method: str = "GET", **kwargs: Any
    ) -> Streamed[T]:
        """Stream a response body into `consume`. A failure part-way through the body retries the
        whole request, so `consume` must be safe to call again from the start."""
        retrying = self._retrying()
        for attempt in retrying:
            with attempt:
                self._throttle()
                with self._client.stream(method, url, **kwargs) as response:
                    value = consume(self._check(response))
        return Streamed(
            value,
            response.status_code,
            dict(response.headers),
            retrying.statistics["attempt_number"],
        )

    def _retrying(self) -> Retrying:
        return Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=self._wait,
            retry=retry_if_exception_type((httpx.TransportError, RetryableStatusError)),
            before_sleep=_log_retry,
            reraise=True,
        )

    @staticmethod
    def _check(response: httpx.Response) -> httpx.Response:
        if response.status_code in RETRY_STATUSES:
            raise RetryableStatusError(response)
        response.raise_for_status()
        return response

    def _throttle(self) -> None:
        with self._lock:
            wait = self._last_request_at + self._min_interval_s - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()


def _log_retry(state: RetryCallState) -> None:
    error = state.outcome.exception() if state.outcome else None
    log.warning("attempt %d failed (%s); retrying", state.attempt_number, error)

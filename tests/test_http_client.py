import httpx
import pytest

from shadow_signals.http_client import RetryableStatusError


def test_retries_retryable_status_then_succeeds(make_http):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) < 3 else 200, text="ok")

    with make_http(handler, max_attempts=4) as client:
        fetched = client.get("https://example.test/data")

    assert fetched.response.text == "ok"
    assert fetched.attempts == 3


def test_retries_timeouts(make_http):
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(200)

    with make_http(handler) as client:
        assert client.get("https://example.test/data").attempts == 2


def test_does_not_retry_client_errors(make_http):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(404)

    with make_http(handler) as client, pytest.raises(httpx.HTTPStatusError):
        client.get("https://example.test/missing")

    assert len(calls) == 1


def test_gives_up_after_max_attempts(make_http):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503)

    with make_http(handler, max_attempts=3) as client, pytest.raises(RetryableStatusError):
        client.get("https://example.test/data")

    assert len(calls) == 3


def test_sends_user_agent(make_http):
    seen = {}

    def handler(request):
        seen["ua"] = request.headers["user-agent"]
        return httpx.Response(200)

    with make_http(handler) as client:
        client.get("https://example.test/data")

    assert seen["ua"] == "shadow-signals-test"


def test_stream_restarts_when_body_breaks_midway(make_http):
    calls = []

    class BreaksOnce(httpx.SyncByteStream):
        def __iter__(self):
            yield b"partial "
            if len(calls) == 1:
                raise httpx.RemoteProtocolError("peer closed connection")
            yield b"complete"

    def handler(request):
        calls.append(request)
        return httpx.Response(200, stream=BreaksOnce())

    with make_http(handler) as client:
        streamed = client.stream("https://example.test/big", lambda r: b"".join(r.iter_bytes()))

    assert streamed.value == b"partial complete"
    assert streamed.attempts == 2
    assert streamed.status == 200

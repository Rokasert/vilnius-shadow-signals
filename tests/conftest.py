import httpx
import pytest
from tenacity import wait_none

from shadow_signals.context import RunContext
from shadow_signals.http_client import HttpClient
from shadow_signals.lake import Lake
from shadow_signals.settings import Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(
        _env_file=None,
        lake_uri=str(tmp_path / "lake"),
        sample_uri=str(tmp_path / "sample"),
        warehouse_path=str(tmp_path / "warehouse.duckdb"),
        spinta_base_url="https://spinta.test",
        sodra_base_url="https://sodra.test",
    )


@pytest.fixture
def ctx(settings):
    return RunContext(
        snapshot_month="2026-10", mode="live", settings=settings, lake=Lake(settings.lake_uri)
    )


@pytest.fixture
def make_http():
    def make(handler, max_attempts=3):
        return HttpClient(
            user_agent="shadow-signals-test",
            timeout_s=5,
            max_attempts=max_attempts,
            transport=httpx.MockTransport(handler),
            wait=wait_none(),
        )

    return make

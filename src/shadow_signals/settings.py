from functools import lru_cache
from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

RunMode = Literal["replay", "live"]
GoogleMode = Literal["reuse", "live"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    lake_uri: str = "data/lake"
    sample_uri: str = "data/sample"
    warehouse_path: str = "data/warehouse.duckdb"
    dbt_project_dir: str = "transform"
    run_mode: RunMode = "replay"

    http_user_agent: str = "shadow-signals/0.1 (research prototype)"
    http_timeout_s: float = 300.0
    http_max_attempts: int = 5
    http_min_interval_s: float = 1.0
    ingest_workers: int = 3

    spinta_base_url: str = "https://get.data.gov.lt"
    sodra_base_url: str = "https://atvira.sodra.lt"

    # Google is only called with --google live; otherwise the newest saved pull is reused.
    google_mode: GoogleMode = "reuse"
    google_maps_api_key: SecretStr | None = None
    places_base_url: str = "https://places.googleapis.com"
    places_max_calls: int = 900
    places_min_interval_s: float = 0.2
    # south-west and north-east corners of the area searched (Vilnius city municipality)
    places_area: tuple[float, float, float, float] = (54.567, 25.024, 54.833, 25.482)
    places_grid: int = 3
    places_max_depth: int = 6

    crawl_workers: int = 8
    crawl_max_pages: int = 4
    crawl_timeout_s: float = 15.0
    crawl_delay_s: float = 1.0

    @field_validator("google_maps_api_key", mode="before")
    @classmethod
    def _blank_is_missing(cls, value: object) -> object:
        return None if value == "" else value


@lru_cache
def get_settings() -> Settings:
    return Settings()

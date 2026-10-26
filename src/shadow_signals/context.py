from dataclasses import dataclass

from shadow_signals.lake import Lake
from shadow_signals.settings import GoogleMode, RunMode, Settings


@dataclass(frozen=True)
class RunContext:
    snapshot_month: str
    mode: RunMode
    settings: Settings
    lake: Lake
    google_mode: GoogleMode = "reuse"

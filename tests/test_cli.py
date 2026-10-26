import re

import pytest
import typer

from shadow_signals.cli import parse_month


def test_parse_month_accepts_valid_month():
    assert parse_month("2026-10") == "2026-10"


def test_parse_month_defaults_to_current_month():
    assert re.fullmatch(r"\d{4}-\d{2}", parse_month(None))


@pytest.mark.parametrize("value", ["2026-13", "2026-1", "26-10", "2026/10", ""])
def test_parse_month_rejects_bad_values(value):
    with pytest.raises(typer.BadParameter):
        parse_month(value)

"""Tests for GPU profile resolution (no CUDA required)."""

import pytest

from worker_runtime import PROFILE_ALIASES, resolve_profile_name


@pytest.mark.unit
def test_resolve_profile_full():
    assert resolve_profile_name("full") == "20gb_nats"
    assert resolve_profile_name("a") == "20gb_nats"
    assert resolve_profile_name("nats") == "20gb_nats"


@pytest.mark.unit
def test_resolve_profile_capped():
    assert resolve_profile_name("capped_5gb") == "capped_5gb"
    assert resolve_profile_name("b") == "capped_5gb"
    assert resolve_profile_name("5gb") == "capped_5gb"


@pytest.mark.unit
def test_profile_aliases_complete():
    assert PROFILE_ALIASES["full"] == "20gb_nats"

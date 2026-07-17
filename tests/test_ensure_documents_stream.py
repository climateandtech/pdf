"""Unit: DOCUMENTS stream ensure must be additive by default (NATS migration safety)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ensure_documents_stream.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("ensure_documents_stream", _SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # Avoid executing nats connect on import — module only defines helpers + main.
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def ens():
    return _load_module()


def test_merge_additive_keeps_live_only_subjects(ens) -> None:
    desired = ["docs.chunk.*", "docs.result.*"]
    live = ["docs.chunk.*", "docs.embed.*", "docs.result.*"]
    assert ens.merge_stream_subjects(desired, live, prune=False) == [
        "docs.chunk.*",
        "docs.result.*",
        "docs.embed.*",
    ]


def test_merge_prune_drops_live_only(ens) -> None:
    desired = ["docs.chunk.*", "docs.result.*"]
    live = ["docs.chunk.*", "docs.embed.*", "docs.result.*"]
    assert ens.merge_stream_subjects(desired, live, prune=True) == [
        "docs.chunk.*",
        "docs.result.*",
    ]


def test_merge_additive_with_empty_live_is_desired(ens) -> None:
    desired = ["docs.upload.*", "docs.embed.*"]
    assert ens.merge_stream_subjects(desired, [], prune=False) == desired


def test_load_documents_subjects_includes_embed(ens) -> None:
    subjects = ens.load_documents_subjects()
    assert "docs.embed.*" in subjects
    assert "docs.embed.start.*" in subjects
    assert "docs.chunk.*" in subjects


def test_yaml_and_fallback_agree_on_embed(ens) -> None:
    assert "docs.embed.*" in ens._FALLBACK_DOCUMENTS_SUBJECTS
    assert "docs.embed.start.*" in ens._FALLBACK_DOCUMENTS_SUBJECTS

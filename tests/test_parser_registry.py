"""Tests for durable parser benchmark registry."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from parser_registry import (
    REGISTRY_SCHEMA,
    append_manifest,
    collect_environment,
    load_manifest,
    manifest_record,
    register_benchmark_run,
    sha256_file,
    table_stats,
)


def test_sha256_file_is_stable(tmp_path: Path):
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4 minimal")
    assert sha256_file(pdf) == sha256_file(pdf)
    assert len(sha256_file(pdf)) == 64


def test_table_stats_counts_pipe_ready_tables():
    structured = {
        "tables": [
            {"prov": [{"page_no": 2}], "data": {"table_cells": [{"text": "a"}]}},
            {
                "prov": [{"page_no": 3}],
                "data": {"table_cells": [{"text": "a"}, {"text": "b"}]},
            },
        ]
    }
    stats = table_stats(structured)
    assert stats["table_count"] == 2
    assert stats["pipe_ready_tables"] == 1
    assert stats["max_cells_in_table"] == 2


def test_register_benchmark_run_appends_manifest_jsonl(tmp_path: Path):
    registry_root = tmp_path / "registry"
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-test")
    run_dir = registry_root / "runs" / "run-1"
    run_dir.mkdir(parents=True)
    environment = {
        "collected_at": "2026-05-31T00:00:00+00:00",
        "docling_version": "2.96.0",
        "gpu": None,
    }
    results = [
        {
            "mode": "fast_text_tables",
            "mode_description": "pass-1",
            "elapsed_s": 1.5,
            "pages_per_min": 600.0,
            "page_count": 10,
            "markdown_chars": 12000,
            "picture_stats": {"picture_count": 2, "described_picture_count": 0},
            "table_stats": {"table_count": 1, "pipe_ready_tables": 1, "max_cells_in_table": 12},
            "options": {"do_table_structure": True},
        }
    ]
    manifest = register_benchmark_run(
        run_id="run-1",
        pdf_path=pdf,
        modes=["fast_text_tables"],
        results=results,
        run_dir=run_dir,
        environment=environment,
        root=registry_root,
    )
    assert manifest.is_file()
    rows = load_manifest(root=registry_root)
    assert len(rows) == 1
    assert rows[0]["schema_version"] == REGISTRY_SCHEMA
    assert rows[0]["record_id"] == "run-1:fast_text_tables"
    assert rows[0]["pdf_sha256"] == sha256_file(pdf)
    meta = json.loads((run_dir / "dataset_meta.json").read_text(encoding="utf-8"))
    assert meta["pdf"]["pages"] == 10


def test_manifest_record_is_one_flat_row_per_mode(tmp_path: Path):
    row = manifest_record(
        run_id="r1",
        run_dir=tmp_path / "runs" / "r1",
        registry_root=tmp_path / "registry",
        pdf_path=tmp_path / "x.pdf",
        pdf_sha256="abc",
        page_count=4,
        environment={"collected_at": "t"},
        result={
            "mode": "fast_text",
            "elapsed_s": 1.0,
            "pages_per_min": 240,
            "picture_stats": {},
            "table_stats": {},
        },
    )
    assert row["mode"] == "fast_text"
    assert row["record_id"] == "r1:fast_text"


def test_append_manifest_is_append_only(tmp_path: Path):
    root = tmp_path / "registry"
    append_manifest([{"record_id": "a:1", "schema_version": REGISTRY_SCHEMA}], root=root)
    append_manifest([{"record_id": "b:1", "schema_version": REGISTRY_SCHEMA}], root=root)
    rows = load_manifest(root=root)
    assert [row["record_id"] for row in rows] == ["a:1", "b:1"]


def test_load_manifest_returns_empty_when_missing(tmp_path: Path):
    assert load_manifest(root=tmp_path / "registry") == []


def test_collect_environment_records_docling_and_gpu():
    nvidia_smi = "/usr/bin/nvidia-smi"

    with patch("parser_registry.platform.platform", return_value="test-platform"):
        with patch("parser_registry._NVIDIA_SMI", nvidia_smi):
            with patch(
                "parser_registry.subprocess.check_output",
                return_value="RTX 4090, 550.54, 24564 MiB\n",
            ):
                env = collect_environment(gpu_profile="prod")
    assert env["gpu_profile"] == "prod"
    assert env["gpu"] == {
        "name": "RTX 4090",
        "driver": "550.54",
        "memory_total": "24564 MiB",
    }
    assert "docling_version" in env


def test_collect_environment_without_gpu_or_docling():
    with patch("parser_registry._NVIDIA_SMI", None):
        env = collect_environment()
    assert env["gpu"] is None


def test_table_stats_handles_dict_tables_and_invalid_entries():
    structured = {
        "tables": {
            "t1": {"prov": [{"page_no": 1}], "data": {"cells": ["a", "b"]}},
            "t2": "not-a-table",
        }
    }
    stats = table_stats(structured)
    assert stats["table_count"] == 2
    assert stats["pipe_ready_tables"] == 1
    assert stats["table_pages"] == [1]

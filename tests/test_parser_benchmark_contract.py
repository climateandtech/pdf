"""Functional: parser benchmark artifact + registry contract (no GPU)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parser_registry import REGISTRY_SCHEMA, load_manifest, register_benchmark_run

_spec = importlib.util.spec_from_file_location(
    "parser_benchmark",
    ROOT / "scripts" / "parser_benchmark.py",
)
_parser_benchmark = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_parser_benchmark)
SCHEMA_VERSION = _parser_benchmark.SCHEMA_VERSION
write_run = _parser_benchmark.write_run


def test_write_run_emits_benchmark_artifacts(tmp_path: Path):
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    results = [
        {
            "mode": "fast_text_tables",
            "mode_description": "pass-1 default",
            "elapsed_s": 1.0,
            "page_count": 2,
            "pages_per_min": 120.0,
            "markdown_chars": 1000,
            "picture_stats": {"picture_count": 1, "described_picture_count": 0, "sample_descriptions": []},
            "table_stats": {"table_count": 1, "pipe_ready_tables": 1, "max_cells_in_table": 4},
            "options": {"do_table_structure": True, "force_backend_text": True},
        }
    ]
    run_dir = write_run(
        output_root=tmp_path / "runs",
        run_id="contract-run",
        pdf_path=pdf,
        modes=["fast_text_tables"],
        results=results,
    )
    config = json.loads((run_dir / "run_config.json").read_text(encoding="utf-8"))
    metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    assert config["schema_version"] == SCHEMA_VERSION
    assert config["pdf_sha256"]
    assert metrics["results"][0]["mode"] == "fast_text_tables"
    assert (run_dir / "summary.md").is_file()


def test_register_benchmark_run_links_run_dir_to_manifest(tmp_path: Path):
    registry_root = tmp_path / "registry"
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-test")
    run_dir = registry_root / "runs" / "contract-reg"
    run_dir.mkdir(parents=True)
    register_benchmark_run(
        run_id="contract-reg",
        pdf_path=pdf,
        modes=["fast_text"],
        results=[
            {
                "mode": "fast_text",
                "elapsed_s": 0.5,
                "pages_per_min": 200.0,
                "page_count": 1,
                "markdown_chars": 10,
                "picture_stats": {},
                "table_stats": {},
            }
        ],
        run_dir=run_dir,
        environment={"collected_at": "2026-05-31T00:00:00+00:00"},
        root=registry_root,
    )
    rows = load_manifest(root=registry_root)
    assert len(rows) == 1
    assert rows[0]["schema_version"] == REGISTRY_SCHEMA

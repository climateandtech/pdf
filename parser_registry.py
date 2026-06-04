"""Durable parser benchmark registry for reproducible research datasets."""

from __future__ import annotations

import hashlib
import json
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REGISTRY_SCHEMA = "ct-parser-benchmark/v1"
DEFAULT_REGISTRY_ROOT = Path(__file__).resolve().parent / "benchmarks" / "parser" / "registry"
MANIFEST_FILENAME = "manifest.jsonl"
DATASET_README = "DATASET.md"
_NVIDIA_SMI = shutil.which("nvidia-smi")


def registry_paths(root: Path | None = None) -> dict[str, Path]:
    base = (root or DEFAULT_REGISTRY_ROOT).resolve()
    return {
        "root": base,
        "manifest": base / MANIFEST_FILENAME,
        "runs": base / "runs",
        "schema": base / "schema" / f"{REGISTRY_SCHEMA}.json",
    }


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def collect_environment(*, gpu_profile: str | None = None) -> dict[str, Any]:
    env: dict[str, Any] = {
        "schema_version": REGISTRY_SCHEMA,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "gpu_profile": gpu_profile,
    }
    try:
        import docling

        env["docling_version"] = getattr(docling, "__version__", None) or _pkg_version("docling")
    except ImportError:
        env["docling_version"] = None
    if _NVIDIA_SMI:
        try:
            out = subprocess.check_output(  # noqa: S603
                [
                    _NVIDIA_SMI,
                    "--query-gpu=name,driver_version,memory.total",
                    "--format=csv,noheader",
                ],
                text=True,
                timeout=5,
            ).strip()
            if out:
                name, driver, memory = [part.strip() for part in out.split(",")]
                env["gpu"] = {"name": name, "driver": driver, "memory_total": memory}
        except (subprocess.SubprocessError, ValueError):
            env["gpu"] = None
    else:
        env["gpu"] = None
    return env


def _pkg_version(name: str) -> str | None:
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version(name)
    except PackageNotFoundError:
        return None


def table_stats(structured: dict[str, Any]) -> dict[str, Any]:
    tables = structured.get("tables") or []
    if isinstance(tables, dict):
        tables = list(tables.values())
    cell_counts: list[int] = []
    pages: set[int] = set()
    for table in tables:
        if not isinstance(table, dict):
            continue
        data = table.get("data") or {}
        cells = data.get("table_cells") or data.get("cells") or []
        cell_counts.append(len(cells) if isinstance(cells, list) else 0)
        for prov in table.get("prov") or []:
            if isinstance(prov, dict) and prov.get("page_no") is not None:
                pages.add(int(prov["page_no"]))
    return {
        "table_count": len(tables),
        "table_pages": sorted(pages),
        "cells_per_table": cell_counts,
        "max_cells_in_table": max(cell_counts) if cell_counts else 0,
        "pipe_ready_tables": sum(1 for count in cell_counts if count > 1),
    }


def _relative_to_registry(path: Path, registry_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(registry_root.resolve()))
    except ValueError:
        return str(path.resolve())


def manifest_record(
    *,
    run_id: str,
    run_dir: Path,
    registry_root: Path,
    pdf_path: Path,
    pdf_sha256: str,
    page_count: int,
    environment: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any]:
    """One flat JSONL row per mode result — suitable for pandas/polars load."""
    pics = result.get("picture_stats") or {}
    tbl = result.get("table_stats") or {}
    return {
        "schema_version": REGISTRY_SCHEMA,
        "run_id": run_id,
        "record_id": f"{run_id}:{result.get('mode')}",
        "created_at": environment.get("collected_at"),
        "run_dir": _relative_to_registry(run_dir, registry_root),
        "pdf_name": pdf_path.name,
        "pdf_sha256": pdf_sha256,
        "pdf_pages": page_count,
        "mode": result.get("mode"),
        "mode_description": result.get("mode_description"),
        "elapsed_s": result.get("elapsed_s"),
        "pages_per_min": result.get("pages_per_min"),
        "markdown_chars": result.get("markdown_chars"),
        "picture_count": pics.get("picture_count", 0),
        "described_picture_count": pics.get("described_picture_count", 0),
        "table_count": tbl.get("table_count", 0),
        "pipe_ready_tables": tbl.get("pipe_ready_tables", 0),
        "max_cells_in_table": tbl.get("max_cells_in_table", 0),
        "docling_version": environment.get("docling_version"),
        "gpu_name": (environment.get("gpu") or {}).get("name"),
        "gpu_profile": environment.get("gpu_profile"),
        "options": result.get("options"),
    }


def append_manifest(records: list[dict[str, Any]], *, root: Path | None = None) -> Path:
    paths = registry_paths(root)
    paths["root"].mkdir(parents=True, exist_ok=True)
    manifest = paths["manifest"]
    with manifest.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    return manifest


def register_benchmark_run(
    *,
    run_id: str,
    pdf_path: Path,
    modes: list[str],
    results: list[dict[str, Any]],
    run_dir: Path,
    environment: dict[str, Any],
    root: Path | None = None,
) -> Path:
    """Append manifest rows and write dataset metadata alongside the run."""
    paths = registry_paths(root)
    pdf_sha256 = sha256_file(pdf_path)
    page_count = max((int(r.get("page_count") or 0) for r in results), default=0)
    records = [
        manifest_record(
            run_id=run_id,
            run_dir=run_dir,
            registry_root=paths["root"],
            pdf_path=pdf_path,
            pdf_sha256=pdf_sha256,
            page_count=page_count,
            environment=environment,
            result=result,
        )
        for result in results
    ]
    manifest = append_manifest(records, root=root)
    meta = {
        "schema_version": REGISTRY_SCHEMA,
        "run_id": run_id,
        "pdf": {
            "path": str(pdf_path.resolve()),
            "name": pdf_path.name,
            "sha256": pdf_sha256,
            "pages": page_count,
        },
        "modes": modes,
        "environment": environment,
        "manifest_records": len(records),
    }
    (run_dir / "dataset_meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return manifest


def load_manifest(*, root: Path | None = None) -> list[dict[str, Any]]:
    manifest = registry_paths(root)["manifest"]
    if not manifest.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows

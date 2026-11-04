"""Fetch the climate-health tracker CSV, or build a schema stand-in."""

from __future__ import annotations

import csv
import math
import os
from datetime import date, timedelta
from pathlib import Path

DATASET = "sohumgokhale/global-climate-health-impact-tracker-2015-2025"
CSV_NAME = "global_climate_health_impact_tracker_2015_2025.csv"
WEEKS = 564
COUNTRIES = (
    "USA",
    "IND",
    "CHN",
    "BRA",
    "NGA",
    "DEU",
    "JPN",
    "GBR",
    "FRA",
    "AUS",
    "KEN",
    "MEX",
    "IDN",
    "PAK",
    "BGD",
    "EGY",
    "ZAF",
    "CAN",
    "ESP",
    "ITA",
    "THA",
    "PHL",
    "VNM",
    "ARG",
    "COL",
)
COLUMNS = (
    "date",
    "country_code",
    "respiratory_disease_rate",
    "heat_related_admissions",
    "temperature_celsius",
    "temp_anomaly_celsius",
    "precipitation_mm",
    "heat_wave_days",
    "pm25_ugm3",
    "air_quality_index",
    "healthcare_access_index",
    "gdp_per_capita_usd",
)


def _allow_synthetic() -> bool:
    return os.getenv("FORECAST_BENCH_ALLOW_SYNTHETIC", "").lower() in {"1", "true", "yes"}


def _kaggle_credentials() -> tuple[str, str] | None:
    username = os.getenv("KAGGLE_USERNAME", "").strip()
    key = os.getenv("KAGGLE_KEY", "").strip()
    if username and key:
        return username, key
    path = Path.home() / ".kaggle" / "kaggle.json"
    if not path.is_file():
        return None
    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    username = str(data.get("username") or "").strip()
    key = str(data.get("key") or "").strip()
    if username and key:
        return username, key
    return None


def _download_kaggle(dest: Path) -> None:
    import io
    import urllib.error
    import urllib.request
    import zipfile

    creds = _kaggle_credentials()
    if creds is None:
        raise FileNotFoundError(
            "Kaggle credentials are missing. Set KAGGLE_USERNAME and KAGGLE_KEY, "
            "or FORECAST_BENCH_ALLOW_SYNTHETIC=1 for a schema stand-in."
        )
    url = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET}"
    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, url, creds[0], creds[1])
    opener = urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(password_mgr))
    try:
        with opener.open(url, timeout=120) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        raise FileNotFoundError(f"Kaggle download failed ({exc.code})") from exc
    dest.parent.mkdir(parents=True, exist_ok=True)
    if payload[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            name = next(item for item in archive.namelist() if item.endswith(".csv"))
            dest.write_bytes(archive.read(name))
        return
    if payload[:1] in {b"{", b"["}:
        detail = payload.decode("utf-8", errors="replace")[:300]
        raise FileNotFoundError(f"Kaggle download was not a CSV: {detail}")
    dest.write_bytes(payload)


def _wave(index: int, country_index: int, period: float, phase: float) -> float:
    return math.sin((index / period) * math.tau + phase + country_index)


def write_synthetic(dest: Path) -> None:
    """Weekly panel with the tracker columns. Not the Kaggle file."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    start = date(2015, 1, 5)
    with dest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for country_index, code in enumerate(COUNTRIES):
            for week in range(WEEKS):
                cold = _wave(week, country_index, 52, 0.4)
                heat = _wave(week, country_index, 52, math.pi)
                temperature = 15 + 8 * country_index / 25 + 12 * heat
                pm25 = 18 + 10 * (country_index % 5) + 6 * max(cold, 0)
                writer.writerow(
                    {
                        "date": (start + timedelta(days=7 * week)).isoformat(),
                        "country_code": code,
                        "respiratory_disease_rate": round(40 + 12 * cold + 0.3 * pm25, 4),
                        "heat_related_admissions": round(
                            5 + 4 * max(heat, 0) + 0.05 * temperature, 4
                        ),
                        "temperature_celsius": round(temperature, 4),
                        "temp_anomaly_celsius": round(0.4 + 0.2 * heat, 4),
                        "precipitation_mm": round(20 + 15 * max(-heat, 0), 4),
                        "heat_wave_days": int(max(heat, 0) > 0.85),
                        "pm25_ugm3": round(pm25, 4),
                        "air_quality_index": round(pm25 * 2.2, 4),
                        "healthcare_access_index": round(50 + country_index, 4),
                        "gdp_per_capita_usd": round(8000 + 2000 * country_index, 4),
                    }
                )


def ensure_csv(dest: Path) -> str:
    """Return ``kaggle`` or ``synthetic`` after the file exists."""
    override = os.getenv("FORECAST_BENCH_CSV", "").strip()
    if override:
        source = Path(override)
        if not source.is_file():
            raise FileNotFoundError(override)
        return "kaggle"
    if dest.is_file() and not _allow_synthetic():
        return "kaggle"
    marked = os.getenv("FORECAST_BENCH_SOURCE") == "synthetic"
    if dest.is_file() and dest.stat().st_size > 0 and marked:
        return "synthetic"
    try:
        _download_kaggle(dest)
    except FileNotFoundError:
        if not _allow_synthetic():
            raise
        write_synthetic(dest)
        return "synthetic"
    return "kaggle"

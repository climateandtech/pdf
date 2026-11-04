"""Observed series for the forecast bench. No synthetic tracker."""

from __future__ import annotations

import csv
import math
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
from huggingface_hub import hf_hub_download

from forecasting.panel import prepare_country

ECDC_URL = (
    "https://raw.githubusercontent.com/EU-ECDC/Respiratory_viruses_weekly_data/"
    "refs/heads/main/data/snapshots/2025-08-08_ILIARIRates.csv"
)
GIFT = "Salesforce/GiftEval"
_NONE: tuple[str, ...] = ()


def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 0:
        return dest
    if not url.startswith("https://"):
        raise ValueError("download URL must be https")
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310
        dest.write_bytes(response.read())
    return dest


def _arrow(name: str) -> pa.Table:
    path = hf_hub_download(GIFT, name, repo_type="dataset")
    with pa.memory_map(path, "r") as source:
        try:
            reader = ipc.open_stream(source)
        except pa.ArrowInvalid:
            source.seek(0)
            reader = ipc.open_file(source)
        return reader.read_all()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _strip(values: list[float | None]) -> list[float] | None:
    start = 0
    end = len(values)
    while start < end and values[start] is None:
        start += 1
    while end > start and values[end - 1] is None:
        end -= 1
    body = values[start:end]
    if not body or any(item is None for item in body):
        return None
    return [float(item) for item in body if item is not None]


def _dates(start: Any, freq: str, count: int) -> list[str]:
    if freq == "M":
        freq = "ME"
    stamps = pd.date_range(start=pd.Timestamp(start), periods=count, freq=freq)
    return [stamp.date().isoformat() for stamp in stamps]


def _from_rows(
    rows: list[dict[str, Any]],
    *,
    target: str,
    holdout: int,
    known: tuple[str, ...] = _NONE,
    past: tuple[str, ...] = _NONE,
) -> dict[str, Any] | None:
    try:
        return prepare_country(
            rows,
            target=target,
            holdout=holdout,
            known_covariates=known,
            past_covariates=past,
            id_field="series_id",
        )
    except ValueError:
        return None


def ecdc_ili(data_dir: Path, *, holdout: int, limit: int) -> list[dict[str, Any]]:
    """Weekly ILI consultation rates, ECDC snapshot 2025-08-08, age total."""
    path = _download(ECDC_URL, data_dir / "2025-08-08_ILIARIRates.csv")
    grouped: dict[str, list[dict[str, Any]]] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("indicator") != "ILIconsultationrate" or row.get("age") != "total":
                continue
            value = _finite(row.get("value"))
            token = str(row.get("yearweek") or "")
            if value is None or "-W" not in token:
                continue
            year, week = token.split("-W", 1)
            stamp = datetime.strptime(f"{year}-W{int(week):02d}-1", "%G-W%V-%u")
            country = str(row.get("countryname") or "")
            grouped.setdefault(country, []).append(
                {"date": stamp.date().isoformat(), "series_id": country, "ili": value}
            )
    tasks = []
    for country in sorted(grouped):
        rows = sorted(grouped[country], key=lambda item: item["date"])
        task = _from_rows(rows, target="ili", holdout=holdout)
        if task is None or len(task["target"]) <= 52:
            continue
        task["season"] = 52
        tasks.append(task)
        if len(tasks) >= limit:
            break
    return tasks


def _univariate_arrow(
    filename: str,
    *,
    holdout: int,
    limit: int,
    season: int,
) -> list[dict[str, Any]]:
    table = _arrow(filename)
    frame = table.to_pydict()
    tasks = []
    for item_id, start, freq, target in zip(
        frame["item_id"], frame["start"], frame["freq"], frame["target"], strict=True
    ):
        values = [_finite(item) for item in target]
        cleaned = _strip(values)
        if cleaned is None:
            continue
        offset = 0
        while offset < len(values) and values[offset] is None:
            offset += 1
        stamps = _dates(start, str(freq), len(values))[offset : offset + len(cleaned)]
        rows = [
            {"date": stamp, "series_id": str(item_id), "y": value}
            for stamp, value in zip(stamps, cleaned, strict=True)
        ]
        task = _from_rows(rows, target="y", holdout=holdout)
        if task is None or len(task["target"]) <= season:
            continue
        task["season"] = season
        tasks.append(task)
        if len(tasks) >= limit:
            break
    return tasks


def gift_electricity_weekly(holdout: int, limit: int) -> list[dict[str, Any]]:
    """UCI household electricity demand, GIFT-Eval weekly aggregation."""
    return _univariate_arrow(
        "electricity/W/data-00000-of-00001.arrow",
        holdout=holdout,
        limit=limit,
        season=52,
    )


def gift_covid_deaths(holdout: int, limit: int) -> list[dict[str, Any]]:
    """Daily COVID-19 deaths from the Monash archive via GIFT-Eval."""
    return _univariate_arrow(
        "covid_deaths/data-00000-of-00001.arrow",
        holdout=holdout,
        limit=limit,
        season=7,
    )


def gift_hospital(holdout: int, limit: int) -> list[dict[str, Any]]:
    """Monthly hospital patient counts from the Monash archive via GIFT-Eval."""
    return _univariate_arrow(
        "hospital/data-00000-of-00001.arrow",
        holdout=holdout,
        limit=limit,
        season=12,
    )


def _temperature_index(channels: list[list[Any]]) -> int:
    """Pick the Jena channel whose typical values are air temperature in Celsius."""
    best = 1
    for index, channel in enumerate(channels):
        finite = [value for item in channel if (value := _finite(item)) is not None]
        if len(finite) < 30:
            continue
        ordered = sorted(finite)
        median = ordered[len(ordered) // 2]
        spread = ordered[-1] - ordered[0]
        if -15 <= median <= 30 and spread > 8:
            return index
    return best


def gift_jena_daily(holdout: int) -> list[dict[str, Any]]:
    """Jena weather station, daily. Temperature is the target; other channels are known-future."""
    table = _arrow("jena_weather/D/data-00000-of-00001.arrow")
    frame = table.to_pydict()
    channels = frame["target"][0]
    target_index = _temperature_index(channels)
    temperature = [_finite(item) for item in channels[target_index]]
    cleaned = _strip(temperature)
    if cleaned is None:
        return []
    stamps = _dates(frame["start"][0], str(frame["freq"][0]), len(temperature))
    offset = next(index for index, value in enumerate(temperature) if value is not None)
    stamps = stamps[offset : offset + len(cleaned)]
    others = []
    for index, channel in enumerate(channels):
        if index == target_index:
            continue
        values = [_finite(item) for item in channel][offset : offset + len(cleaned)]
        if any(item is None for item in values):
            continue
        others.append((f"v{index}", [float(item) for item in values if item is not None]))
    rows = []
    for step, stamp in enumerate(stamps):
        row = {"date": stamp, "series_id": "jena", "T_degC": cleaned[step]}
        for name, values in others:
            row[name] = values[step]
        rows.append(row)
    task = _from_rows(
        rows,
        target="T_degC",
        holdout=holdout,
        known=tuple(name for name, _values in others),
    )
    if task is None or len(task["target"]) <= 7:
        return []
    task["season"] = 7
    return [task]

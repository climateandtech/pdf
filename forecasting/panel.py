"""Hold out the last weeks of a climate-health country and cap the context."""

from __future__ import annotations

from typing import Any

from forecasting.payload import CONTEXT_CAP

HOLDOUT_WEEKS = 12
SEASON_WEEKS = 52
PRIMARY_TARGET = "respiratory_disease_rate"
SECOND_TARGET = "heat_related_admissions"
KNOWN_COVARIATES = (
    "temperature_celsius",
    "temp_anomaly_celsius",
    "precipitation_mm",
    "heat_wave_days",
    "pm25_ugm3",
    "air_quality_index",
)
PAST_COVARIATES = (
    "healthcare_access_index",
    "gdp_per_capita_usd",
)


def _column(rows: list[dict[str, Any]], name: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        try:
            values.append(float(row[name]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"column {name} is missing or not numeric") from exc
    return values


def prepare_country(
    rows: list[dict[str, Any]],
    *,
    target: str,
    holdout: int = HOLDOUT_WEEKS,
    context_cap: int = CONTEXT_CAP,
    known_covariates: tuple[str, ...] | None = None,
    past_covariates: tuple[str, ...] | None = None,
    id_field: str = "country_code",
) -> dict[str, Any]:
    """Train on the last ``context_cap`` weeks before a ``holdout`` tail.

    The full country series is longer than the catalogue row cap. The bench
    keeps the most recent context instead of sending the whole history.
    """
    if holdout < 1:
        raise ValueError("holdout must be positive")
    if len(rows) <= holdout + 1:
        raise ValueError("series is shorter than the holdout")
    train = rows[:-holdout]
    future = rows[-holdout:]
    if len(train) > context_cap:
        train = train[-context_cap:]
    known_names = KNOWN_COVARIATES if known_covariates is None else known_covariates
    past_names = PAST_COVARIATES if past_covariates is None else past_covariates
    known: dict[str, list[float]] = {}
    for name in known_names:
        known[name] = _column(train, name) + _column(future, name)
    past = {name: _column(train, name) for name in past_names}
    return {
        "id": str(train[0].get(id_field) or ""),
        "target_name": target,
        "target": _column(train, target),
        "actual": _column(future, target),
        "time_context": [str(row.get("date") or "") for row in train],
        "time_horizon": [str(row.get("date") or "") for row in future],
        "known_covariates": known,
        "past_covariates": past,
    }


def group_countries(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group CSV rows by country, ordered by date."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = str(row.get("country_code") or "")
        grouped.setdefault(code, []).append(row)
    for code, series in grouped.items():
        series.sort(key=lambda item: str(item.get("date") or ""))
        grouped[code] = series
    return grouped

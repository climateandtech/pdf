"""Validate a forecast.infer body and shape quantile rows. No torch."""

from __future__ import annotations

from typing import Any

CONTEXT_CAP = 500
PRODUCTION_MODEL = "timestral-28m"
QUANTILE_COLUMNS = ("q10", "q50", "q90")


def _floats(values: Any, *, name: str) -> list[float]:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{name} must be a non-empty list")
    out: list[float] = []
    for item in values:
        if item is None:
            raise ValueError(f"{name} contains a null")
        try:
            number = float(item)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be numeric") from exc
        if number != number:  # NaN
            raise ValueError(f"{name} contains a null")
        out.append(number)
    return out


def _optional_block(raw: Any, *, name: str, length: int) -> dict[str, list[float]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{name} must be an object of numeric columns")
    out: dict[str, list[float]] = {}
    for key, values in raw.items():
        column = _floats(values, name=f"{name}.{key}")
        if len(column) != length:
            raise ValueError(f"{name}.{key} must have length {length}")
        out[str(key)] = column
    return out


def validate_series(item: Any, horizon: int) -> dict[str, Any]:
    """Check one series. Context longer than ``CONTEXT_CAP`` is rejected."""
    if not isinstance(item, dict):
        raise ValueError("series item must be an object")
    target = _floats(item.get("target"), name="target")
    if len(target) > CONTEXT_CAP:
        raise ValueError(f"context has {len(target)} rows; cap is {CONTEXT_CAP}")
    known = _optional_block(
        item.get("known_covariates"),
        name="known_covariates",
        length=len(target) + horizon,
    )
    past = _optional_block(
        item.get("past_covariates"),
        name="past_covariates",
        length=len(target),
    )
    times = item.get("time_horizon") or []
    if times and (not isinstance(times, list) or len(times) != horizon):
        raise ValueError("time_horizon must have one entry per horizon step")
    return {
        "id": str(item.get("id") or ""),
        "target_name": str(item.get("target_name") or "target"),
        "target": target,
        "known_covariates": known,
        "past_covariates": past,
        "time_horizon": [str(step) for step in times],
    }


def validate_request(body: Any) -> dict[str, Any]:
    """Check a worker request. Only the production Timestral checkpoint is served."""
    if not isinstance(body, dict):
        raise ValueError("request must be an object")
    model = str(body.get("model") or PRODUCTION_MODEL)
    if model != PRODUCTION_MODEL:
        raise ValueError(f"production worker serves {PRODUCTION_MODEL}")
    try:
        horizon = int(body.get("horizon") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("horizon must be an integer") from exc
    if horizon < 1 or horizon > 1024:
        raise ValueError("horizon must be from 1 to 1024")
    raw_series = body.get("series")
    if not isinstance(raw_series, list) or not raw_series:
        raise ValueError("series must be a non-empty list")
    series = [validate_series(item, horizon) for item in raw_series]
    return {"model": model, "horizon": horizon, "series": series}


def quantile_rows(
    series: list[dict[str, Any]],
    *,
    q10: list[list[float]],
    q50: list[list[float]],
    q90: list[list[float]],
) -> list[dict[str, Any]]:
    """One row per horizon step, with the target name and three quantiles."""
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(series):
        horizon = len(q50[index])
        times = item["time_horizon"] or [str(step + 1) for step in range(horizon)]
        for step in range(horizon):
            rows.append(
                {
                    "time": times[step],
                    "series_id": item["id"],
                    "target": item["target_name"],
                    "q10": q10[index][step],
                    "q50": q50[index][step],
                    "q90": q90[index][step],
                }
            )
    return rows

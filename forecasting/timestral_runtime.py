"""Load Timestral-28M and forecast one batch. Torch stays inside this module."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import numpy as np

from forecasting.device import resolve_device
from forecasting.payload import PRODUCTION_MODEL, quantile_rows, validate_request

_REPO = "mistralai/Timestral-28M"


@lru_cache(maxsize=1)
def _forecaster(device: str) -> Any:
    from timestral import TimestralForecaster

    return TimestralForecaster.from_pretrained(_REPO, device=device)


def _matrix(series: list[dict[str, Any]], key: str, names: list[str]) -> np.ndarray | None:
    if not names:
        return None
    blocks = []
    for item in series:
        columns = item[key]
        blocks.append([columns[name] for name in names])
    return np.asarray(blocks, dtype=np.float64)


def _squeeze_horizon(values: Any) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim == 3:
        array = array[:, 0, :]
    if array.ndim != 2:
        raise RuntimeError(f"unexpected forecast shape {array.shape}")
    return array


def forecast_payload(body: dict[str, Any]) -> dict[str, Any]:
    """Run the production checkpoint and return quantile rows."""
    request = validate_request(body)
    device = resolve_device()
    series = request["series"]
    horizon = request["horizon"]
    context = np.asarray([item["target"] for item in series], dtype=np.float64)
    known_names = sorted(series[0]["known_covariates"])
    past_names = sorted(series[0]["past_covariates"])
    for item in series[1:]:
        if sorted(item["known_covariates"]) != known_names:
            raise ValueError("known covariate columns must match across the batch")
        if sorted(item["past_covariates"]) != past_names:
            raise ValueError("past covariate columns must match across the batch")
    kwargs: dict[str, Any] = {}
    known = _matrix(series, "known_covariates", known_names)
    past = _matrix(series, "past_covariates", past_names)
    if known is not None:
        kwargs["known_covariates"] = known
    if past is not None:
        kwargs["past_covariates"] = past
    forecast = _forecaster(device).forecast(context, prediction_length=horizon, **kwargs)
    q10 = _squeeze_horizon(forecast.quantile(0.1))
    q50 = _squeeze_horizon(forecast.median)
    q90 = _squeeze_horizon(forecast.quantile(0.9))
    rows = quantile_rows(
        series,
        q10=q10.tolist(),
        q50=q50.tolist(),
        q90=q90.tolist(),
    )
    return {
        "ok": True,
        "model": PRODUCTION_MODEL,
        "device": device,
        "columns": ["time", "series_id", "target", "q10", "q50", "q90"],
        "rows": rows,
    }

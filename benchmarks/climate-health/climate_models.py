"""Zero-shot Timestral and TimesFM, plus a per-country Prophet fit."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np


def _triple(
    point: list[float], lower: list[float], upper: list[float]
) -> tuple[list[float], list[float], list[float]]:
    return point, lower, upper


def forecast_timestral(task: dict[str, Any]) -> tuple[list[float], list[float], list[float]]:
    """One country through the production Timestral runtime."""
    from forecasting.timestral_runtime import forecast_payload

    horizon = len(task["actual"])
    out = forecast_payload(
        {
            "model": "timestral-28m",
            "horizon": horizon,
            "series": [
                {
                    "id": task["id"],
                    "target_name": task["target_name"],
                    "target": task["target"],
                    "known_covariates": task["known_covariates"],
                    "past_covariates": task["past_covariates"],
                    "time_horizon": task["time_horizon"],
                }
            ],
        }
    )
    rows = out["rows"]
    return _triple(
        [float(row["q50"]) for row in rows],
        [float(row["q10"]) for row in rows],
        [float(row["q90"]) for row in rows],
    )


def _timesfm():
    import timesfm

    model = timesfm.TimesFM_2p5_200M_torch.from_pretrained("google/timesfm-2.5-200m-pytorch")
    model.compile(
        timesfm.ForecastConfig(
            max_context=512,
            max_horizon=128,
            normalize_inputs=True,
            use_continuous_quantile_head=True,
            force_flip_invariance=True,
            infer_is_positive=True,
            fix_quantile_crossing=True,
        )
    )
    return model


_TIMESFM = None


def forecast_timesfm(task: dict[str, Any]) -> tuple[list[float], list[float], list[float]]:
    """Univariate TimesFM 2.5. Covariates are not part of this checkpoint."""
    global _TIMESFM
    if _TIMESFM is None:
        _TIMESFM = _timesfm()
    horizon = len(task["actual"])
    series = np.asarray(task["target"], dtype=np.float64)
    point, quantiles = _TIMESFM.forecast(horizon=horizon, inputs=[series])
    point_row = np.asarray(point)[0]
    quantile_row = np.asarray(quantiles)[0]
    # Continuous head is 0.1, 0.2, …, 0.9.
    lower = quantile_row[:, 0]
    upper = quantile_row[:, -1]
    return _triple(point_row.tolist(), lower.tolist(), upper.tolist())


def forecast_prophet(task: dict[str, Any]) -> tuple[list[float], list[float], list[float]]:
    """Fit Prophet on the train window with the climate columns as regressors."""
    import pandas as pd
    from prophet import Prophet

    horizon = len(task["actual"])
    context_stamps = task.get("time_context") or []
    if context_stamps:
        history_dates = [date.fromisoformat(str(day)[:10]) for day in context_stamps]
    else:
        first = date.fromisoformat(str(task["time_horizon"][0])[:10])
        length = len(task["target"])
        history_dates = [
            first - timedelta(days=7 * (length - index)) for index in range(length)
        ]
    future_dates = [date.fromisoformat(str(day)[:10]) for day in task["time_horizon"]]
    frame = pd.DataFrame({"ds": history_dates, "y": task["target"]})
    future = pd.DataFrame({"ds": history_dates + future_dates})
    span_days = (history_dates[-1] - history_dates[0]).days
    model = Prophet(
        interval_width=0.8,
        weekly_seasonality=False,
        daily_seasonality=False,
        yearly_seasonality=span_days >= 730,
    )
    for name, values in task["known_covariates"].items():
        frame[name] = values[: len(task["target"])]
        future[name] = values
        model.add_regressor(name)
    model.fit(frame)
    predicted = model.predict(future).tail(horizon)
    return _triple(
        predicted["yhat"].astype(float).tolist(),
        predicted["yhat_lower"].astype(float).tolist(),
        predicted["yhat_upper"].astype(float).tolist(),
    )

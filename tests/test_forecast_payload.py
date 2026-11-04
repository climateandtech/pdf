"""Payload cap, MASE snapshot, and climate-health context trim."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from forecasting.metrics import mase, mean_interval_pinball
from forecasting.panel import prepare_country
from forecasting.payload import CONTEXT_CAP, validate_request


def test_worker_rejects_context_over_500() -> None:
    with pytest.raises(ValueError, match="cap"):
        validate_request(
            {
                "horizon": 1,
                "series": [{"target": [float(index) for index in range(CONTEXT_CAP + 1)]}],
            }
        )


def test_mase_snapshot_on_a_seasonal_step() -> None:
    """Season length 2, in-sample scale 1, forecast MAE 1 → MASE 1."""
    history = [1.0, 3.0, 2.0, 4.0, 3.0, 5.0]
    assert mase([4.0, 6.0], [3.0, 5.0], history, season=2) == 1.0
    pinball = mean_interval_pinball([10.0], [8.0], [12.0])
    assert pinball == pytest.approx(0.2)


def test_prepare_country_keeps_the_last_500_context_weeks() -> None:
    start = date(2015, 1, 5)
    rows = []
    for index in range(CONTEXT_CAP + 20):
        rows.append(
            {
                "date": (start + timedelta(days=7 * index)).isoformat(),
                "country_code": "USA",
                "respiratory_disease_rate": float(index),
                "heat_related_admissions": 1.0,
                "temperature_celsius": 10.0,
                "temp_anomaly_celsius": 0.1,
                "precipitation_mm": 2.0,
                "heat_wave_days": 0.0,
                "pm25_ugm3": 12.0,
                "air_quality_index": 30.0,
                "healthcare_access_index": 80.0,
                "gdp_per_capita_usd": 1000.0,
            }
        )
    task = prepare_country(rows, target="respiratory_disease_rate", holdout=12)
    assert len(task["target"]) == CONTEXT_CAP
    assert len(task["actual"]) == 12
    assert len(task["known_covariates"]["temperature_celsius"]) == CONTEXT_CAP + 12
    assert task["target"][-1] == float(CONTEXT_CAP + 20 - 12 - 1)

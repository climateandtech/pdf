#!/usr/bin/env python3
"""Compare Timestral-28M, TimesFM 2.5, and Prophet on observed series.

The gate is ECDC weekly ILI (snapshot 2025-08-08). Timestral stays the
production default unless TimesFM 2.5 has a lower MASE on that task.

Also scored, and not used for the gate:

- GIFT-Eval electricity/W: household electricity demand
- GIFT-Eval covid_deaths: daily deaths
- GIFT-Eval hospital: monthly patient counts
- GIFT-Eval jena_weather/D: temperature with the other weather channels as known-future covariates

Prophet fits on CPU. Foundation models use FORECAST_DEVICE. Pytest does not run this script.
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BENCH_DIR = Path(__file__).resolve().parent
if str(BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(BENCH_DIR))

from climate_models import forecast_prophet, forecast_timesfm, forecast_timestral  # noqa: E402
from real_tasks import (  # noqa: E402
    ecdc_ili,
    gift_covid_deaths,
    gift_electricity_weekly,
    gift_hospital,
    gift_jena_daily,
)

from forecasting.metrics import mase, mean_interval_pinball  # noqa: E402
from forecasting.panel import HOLDOUT_WEEKS  # noqa: E402

DATA = BENCH_DIR / "data"
RESULT = BENCH_DIR / "last-run.json"
GATE_TASK = "ecdc_ili"


def _score(task: dict, point: list[float], lower: list[float], upper: list[float]) -> dict:
    return {
        "mase": mase(task["actual"], point, task["target"], season=int(task["season"])),
        "pinball": mean_interval_pinball(task["actual"], lower, upper),
    }


def _mean(rows: list[dict], key: str) -> float:
    return sum(item[key] for item in rows) / len(rows)


def _run_model(name: str, fn, tasks: list[dict]) -> dict:
    scored: list[dict] = []
    try:
        for task in tasks:
            point, lower, upper = fn(task)
            scored.append(_score(task, point, lower, upper))
    except Exception as exc:  # noqa: BLE001 — forecast libraries raise widely
        return {"model": name, "ok": False, "error": f"{exc}\n{traceback.format_exc()}"}
    return {
        "model": name,
        "ok": True,
        "mase": _mean(scored, "mase"),
        "pinball": _mean(scored, "pinball"),
        "series": len(scored),
    }


def _limit() -> int:
    raw = os.getenv("FORECAST_BENCH_MAX_SERIES", "25")
    try:
        return max(1, int(raw))
    except ValueError:
        return 25


def main() -> int:
    limit = _limit()
    holdout = HOLDOUT_WEEKS
    catalog = {
        "ecdc_ili": {
            "source": "ECDC ILI snapshot 2025-08-08",
            "tasks": ecdc_ili(DATA, holdout=holdout, limit=limit),
        },
        "electricity_weekly": {
            "source": "GIFT-Eval electricity/W (UCI household demand)",
            "tasks": gift_electricity_weekly(holdout, limit),
        },
        "covid_deaths": {
            "source": "GIFT-Eval covid_deaths",
            "tasks": gift_covid_deaths(holdout, limit),
        },
        "hospital": {
            "source": "GIFT-Eval hospital",
            "tasks": gift_hospital(holdout, limit),
        },
        "jena_weather_daily": {
            "source": "GIFT-Eval jena_weather/D",
            "tasks": gift_jena_daily(holdout),
        },
    }
    report: dict = {"holdout": holdout, "series_cap": limit, "tasks": {}}
    gate_loser = False
    for name, spec in catalog.items():
        tasks = spec["tasks"]
        print(f"{name}: {len(tasks)} series", flush=True)
        models = [
            _run_model("timestral-28m", forecast_timestral, tasks),
            _run_model("timesfm-2.5", forecast_timesfm, tasks),
            _run_model("prophet", forecast_prophet, tasks),
        ]
        report["tasks"][name] = {"source": spec["source"], "models": models}
        if name == GATE_TASK:
            by_name = {item["model"]: item for item in models if item.get("ok")}
            ours = by_name.get("timestral-28m")
            other = by_name.get("timesfm-2.5")
            if ours and other and other["mase"] < ours["mase"]:
                gate_loser = True
    report["production_default"] = "timestral-28m"
    report["gate"] = "revisit" if gate_loser else "keep"
    RESULT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    if gate_loser:
        print(
            "TimesFM 2.5 beat Timestral-28M on ECDC ILI MASE. Revisit the default.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""MASE and a two-quantile pinball score for the climate-health holdout."""

from __future__ import annotations


def _mean(values: list[float]) -> float:
    if not values:
        raise ValueError("empty series")
    return sum(values) / len(values)


def seasonal_scale(history: list[float], season: int) -> float:
    """In-sample seasonal-naive MAE. A zero scale becomes a tiny epsilon."""
    if season < 1:
        raise ValueError("season must be positive")
    if len(history) <= season:
        raise ValueError("history must be longer than one season")
    diffs = [abs(history[index] - history[index - season]) for index in range(season, len(history))]
    scale = _mean(diffs)
    if scale == 0.0:
        return 1e-8
    return scale


def mase(
    actual: list[float],
    predicted: list[float],
    history: list[float],
    *,
    season: int,
) -> float:
    """Mean absolute scaled error against a seasonal naive on ``history``."""
    if len(actual) != len(predicted) or not actual:
        raise ValueError("actual and predicted must be the same non-empty length")
    scale = seasonal_scale(history, season)
    error = _mean([abs(left - right) for left, right in zip(actual, predicted, strict=True)])
    return error / scale


def pinball(actual: float, quantile_forecast: float, level: float) -> float:
    """Pinball loss at ``level`` between 0 and 1."""
    if not 0.0 < level < 1.0:
        raise ValueError("level must be between 0 and 1")
    gap = actual - quantile_forecast
    if gap >= 0:
        return level * gap
    return (level - 1.0) * gap


def mean_interval_pinball(
    actual: list[float],
    lower: list[float],
    upper: list[float],
    *,
    lower_level: float = 0.1,
    upper_level: float = 0.9,
) -> float:
    """Mean pinball at the 0.1 and 0.9 forecasts."""
    if not (len(actual) == len(lower) == len(upper)) or not actual:
        raise ValueError("quantile columns must match the holdout")
    losses = []
    for value, low, high in zip(actual, lower, upper, strict=True):
        losses.append(pinball(value, low, lower_level))
        losses.append(pinball(value, high, upper_level))
    return _mean(losses)

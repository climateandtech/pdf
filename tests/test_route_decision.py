"""route.decide reply shape. The request does not choose the model."""

from __future__ import annotations

from route_decision.decide import decide


class _Model:
    def __init__(self, choice: str, confidence: float) -> None:
        self.choice = choice
        self.confidence = confidence

    def answer(self, *, state: str, questions: dict) -> dict:
        assert state
        assert "route" in questions
        return {
            "route": {
                "choice": self.choice,
                "probabilities": {self.choice: self.confidence},
            }
        }


def test_missing_state_is_rejected() -> None:
    body = decide(_Model("ping", 1.0), {"criteria": {"ping": "hello"}}, model_name="m")
    assert body == {"ok": False, "error": "missing state"}


def test_known_choice_names_the_worker_model() -> None:
    body = decide(
        _Model("ping", 0.42),
        {
            "state": "Latest user message: ping",
            "criteria": {"ping": "hello"},
            "model": "someone-elses-model",
        },
        model_name="tardellirs/crivo-400m",
    )
    assert body == {
        "ok": True,
        "choice": "ping",
        "confidence": 0.42,
        "model": "tardellirs/crivo-400m",
    }


def test_unknown_choice_is_rejected() -> None:
    body = decide(
        _Model("map_screen", 0.9),
        {"state": "show the map", "criteria": {"ping": "hello"}},
        model_name="m",
    )
    assert body["ok"] is False

"""Turn a decision-model answer into the route.decide reply."""

from __future__ import annotations

from typing import Any


def decide(model: Any, data: dict[str, Any], *, model_name: str) -> dict[str, Any]:
    """Ask the loaded model. A model name on the request is ignored."""
    state = str(data.get("state") or "").strip()
    criteria = data.get("criteria")
    if not state:
        return {"ok": False, "error": "missing state"}
    if not isinstance(criteria, dict) or not criteria:
        return {"ok": False, "error": "missing criteria"}
    labels = {str(key): str(value) for key, value in criteria.items()}
    answer = model.answer(
        state=state,
        questions={
            "route": {
                "type": "choice",
                "instructions": (
                    "Which single flow should handle the latest user message? "
                    "An emoji, a joke, or surprise or doubt about an earlier result "
                    "is reaction, not a search. A request to search or reuse an earlier "
                    "topic is a search. A search requires the message to name a subject."
                ),
                "criteria": labels,
            }
        },
    )
    route = answer["route"]
    choice = str(route.get("choice") or "").strip()
    probabilities = route.get("probabilities") or {}
    if choice not in labels:
        return {"ok": False, "error": f"unknown choice {choice}"}
    return {
        "ok": True,
        "choice": choice,
        "confidence": float(probabilities.get(choice) or 0.0),
        "model": model_name,
    }

"""Load the route-decision model configured on this worker."""

from __future__ import annotations

import os
import sys
from typing import Any

_MODEL: Any = None
_MODEL_ID = ""


def model_id() -> str:
    """Checkpoint this process serves. The bot does not send this."""
    return (os.getenv("ROUTE_DECISION_MODEL") or "tardellirs/crivo-400m").strip()


def get_model() -> Any:
    """Download on first use. A failed download leaves the cache empty."""
    global _MODEL, _MODEL_ID
    wanted = model_id()
    if _MODEL is not None and _MODEL_ID == wanted:
        return _MODEL
    from huggingface_hub import snapshot_download

    path = snapshot_download(wanted)
    if path not in sys.path:
        sys.path.insert(0, path)
    from crivo import Crivo

    _MODEL = Crivo(path)
    _MODEL_ID = wanted
    return _MODEL

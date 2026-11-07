"""Docling Whisper ASR for short agent utterances.

Uses the same preset names as ``docling_worker`` (``whisper_tiny`` and the
other ``asr_model_specs.WHISPER_*`` presets). The converter is cached per
model and language.
"""

from __future__ import annotations

import logging
import tempfile
from typing import Any

from docling.datamodel import asr_model_specs
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import AsrPipelineOptions
from docling.document_converter import AudioFormatOption, DocumentConverter
from docling.pipeline.asr_pipeline import AsrPipeline

from speech.contract import TranscribeRequest

logger = logging.getLogger(__name__)

_SPECS = {
    "whisper_tiny": asr_model_specs.WHISPER_TINY,
    "whisper_base": asr_model_specs.WHISPER_BASE,
    "whisper_small": asr_model_specs.WHISPER_SMALL,
    "whisper_medium": asr_model_specs.WHISPER_MEDIUM,
    "whisper_large": asr_model_specs.WHISPER_LARGE,
    "whisper_turbo": asr_model_specs.WHISPER_TURBO,
}
_CONVERTERS: dict[tuple[str, str], DocumentConverter] = {}


def _spec(model: str, language: str) -> Any:
    spec = _SPECS[model]
    if language in ("", "auto"):
        return spec
    try:
        return spec.model_copy(update={"language": language})
    except (AttributeError, TypeError, ValueError):
        logger.info("whisper language %s left at the preset default", language)
        return spec


def _converter(model: str, language: str) -> DocumentConverter:
    key = (model, language)
    cached = _CONVERTERS.get(key)
    if cached is not None:
        return cached
    options = AsrPipelineOptions(asr_options=_spec(model, language))
    converter = DocumentConverter(
        format_options={
            InputFormat.AUDIO: AudioFormatOption(
                pipeline_cls=AsrPipeline,
                pipeline_options=options,
            )
        }
    )
    _CONVERTERS[key] = converter
    return converter


def transcribe_request(request: TranscribeRequest) -> str:
    """Run Docling ASR on one in-memory audio clip."""
    converter = _converter(request.model, request.language)
    with tempfile.NamedTemporaryFile(suffix=request.suffix) as handle:
        handle.write(request.audio)
        handle.flush()
        result = converter.convert(handle.name)
    return result.document.export_to_markdown().strip()

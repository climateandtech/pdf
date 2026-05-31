"""Nemotron OCR v2 GPU wrapper for PDF page enrichment."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol


class NemotronUnavailableError(RuntimeError):
    """Raised when Nemotron OCR is not installed or cannot load on GPU."""


class PageRenderer(Protocol):
    def render_page(self, pdf_path: Path, page_index: int, output_path: Path, *, dpi: int) -> None:
        ...


@dataclass(frozen=True)
class NemotronPageResult:
    page_index: int
    text: str
    prediction_count: int
    image_path: str


@dataclass(frozen=True)
class NemotronConfig:
    lang: str = "en"
    merge_level: str = "paragraph"
    skip_relational: bool = False
    detector_only: bool = False
    model_dir: str | None = None
    dpi: int = 150

    @classmethod
    def from_env(cls) -> "NemotronConfig":
        return cls(
            lang=os.getenv("NEMOTRON_OCR_LANG", "en"),
            merge_level=os.getenv("NEMOTRON_MERGE_LEVEL", "paragraph"),
            skip_relational=os.getenv("NEMOTRON_SKIP_RELATIONAL", "false").lower()
            in ("1", "true", "yes"),
            detector_only=os.getenv("NEMOTRON_DETECTOR_ONLY", "false").lower() in ("1", "true", "yes"),
            model_dir=os.getenv("NEMOTRON_MODEL_DIR") or None,
            dpi=int(os.getenv("NEMOTRON_RENDER_DPI", "150")),
        )


def predictions_to_text(predictions: Iterable[dict[str, Any]]) -> str:
    parts = [str(item.get("text", "")).strip() for item in predictions]
    parts = [part for part in parts if part]
    return "\n\n".join(parts)


def _load_nemotron_pipeline(config: NemotronConfig):
    try:
        from nemotron_ocr.inference.pipeline_v2 import NemotronOCRV2
    except ImportError as exc:
        raise NemotronUnavailableError(
            "nemotron_ocr is not installed. Run pdf/scripts/setup_nemotron_gpu.sh on the GPU host."
        ) from exc

    kwargs: dict[str, Any] = {
        "skip_relational": config.skip_relational,
        "detector_only": config.detector_only,
    }
    if config.model_dir:
        kwargs["model_dir"] = config.model_dir
    else:
        kwargs["lang"] = config.lang
    return NemotronOCRV2(**kwargs)


class PyMuPDFPageRenderer:
    def render_page(self, pdf_path: Path, page_index: int, output_path: Path, *, dpi: int) -> None:
        try:
            import fitz
        except ImportError as exc:
            raise NemotronUnavailableError("PyMuPDF (fitz) is required to render PDF pages") from exc

        doc = fitz.open(pdf_path)
        try:
            if page_index < 0 or page_index >= doc.page_count:
                raise ValueError(f"Page index out of range: {page_index}")
            page = doc.load_page(page_index)
            pix = page.get_pixmap(dpi=dpi)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pix.save(str(output_path))
        finally:
            doc.close()


class NemotronOcrService:
    def __init__(
        self,
        config: NemotronConfig | None = None,
        renderer: PageRenderer | None = None,
        pipeline: Any | None = None,
    ) -> None:
        self.config = config or NemotronConfig.from_env()
        self.renderer = renderer or PyMuPDFPageRenderer()
        self._pipeline = pipeline

    @property
    def pipeline(self):
        if self._pipeline is None:
            self._pipeline = _load_nemotron_pipeline(self.config)
        return self._pipeline

    def ocr_image(self, image_path: Path, *, merge_level: str | None = None) -> str:
        predictions = self.pipeline(str(image_path), merge_level=merge_level or self.config.merge_level)
        return predictions_to_text(predictions)

    def ocr_pdf_page(self, pdf_path: Path, page_index: int, *, work_dir: Path | None = None) -> NemotronPageResult:
        pdf_path = pdf_path.resolve()
        if not pdf_path.is_file():
            raise FileNotFoundError(pdf_path)

        with tempfile.TemporaryDirectory(prefix="nemotron-page-", dir=work_dir) as tmp:
            image_path = Path(tmp) / f"page_{page_index:04d}.png"
            self.renderer.render_page(
                pdf_path,
                page_index,
                image_path,
                dpi=self.config.dpi,
            )
            text = self.ocr_image(image_path)
            return NemotronPageResult(
                page_index=page_index,
                text=text,
                prediction_count=text.count("\n\n") + (1 if text else 0),
                image_path=str(image_path),
            )

    def ocr_pdf_pages(
        self,
        pdf_path: Path,
        page_indices: Iterable[int],
        *,
        work_dir: Path | None = None,
    ) -> list[NemotronPageResult]:
        return [self.ocr_pdf_page(pdf_path, page_index, work_dir=work_dir) for page_index in page_indices]

    def merge_page_text_into_markdown(self, markdown: str, results: Iterable[NemotronPageResult]) -> str:
        blocks = [markdown.strip()] if markdown and markdown.strip() else []
        for result in results:
            if not result.text.strip():
                continue
            blocks.append(f"<!-- nemotron page {result.page_index + 1} -->\n{result.text.strip()}")
        return "\n\n".join(blocks)


def probe_nemotron_gpu() -> dict[str, Any]:
    """Quick capability probe for setup/smoke scripts."""
    info: dict[str, Any] = {"available": False}
    try:
        import torch

        info["torch_cuda"] = torch.cuda.is_available()
        if torch.cuda.is_available():
            info["gpu_name"] = torch.cuda.get_device_name(0)
    except ImportError:
        info["torch_cuda"] = False
    try:
        config = NemotronConfig.from_env()
        _load_nemotron_pipeline(config)
        info["available"] = True
        info["lang"] = config.lang
        info["merge_level"] = config.merge_level
    except NemotronUnavailableError as exc:
        info["error"] = str(exc)
    return info

# Parser benchmark `opus-docling-2.96`

- PDF: `opus_global_esg_2025_en.pdf` (56 pages)
- Docling: 2.96.x (`venv-benchmark`, `DOCLING_GPU_PROFILE=20gb_nats`)
- GPU: NVIDIA RTX 4000 SFF Ada

- `baseline`: 112.64 pages/min, 445789 chars, 29.83s, 0/77 figures described, 51/51 structured tables
- `fast_text_tables`: 132.58 pages/min, 445672 chars, 25.343s, 0/77 figures described, 51/51 structured tables
- `fast_text`: 358.59 pages/min, 337741 chars, 9.37s, 0/77 figures described, 0/51 structured tables

## vs legacy (Docling 2.42.2, production NATS, same PDF)

- ~76 s, ~44 pages/min (baseline / standard pipeline, May 2026)

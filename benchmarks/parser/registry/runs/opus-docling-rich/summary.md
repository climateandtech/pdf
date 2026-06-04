# Parser benchmark `opus-docling-rich`

- PDF: `opus_global_esg_2025_en.pdf`

- `rich`: 17.35 pages/min, 453640 chars, 193.696s, 39/77 figures described, 51/51 structured tables
  - Baseline plus VLM picture description: charts, photos, and diagrams get a text caption in the markdown (Granite Vision by default). Slowest and highest VRAM; best for image-heavy sustainability reports.
  - sample caption: opus global logo on a blue background with a white circle in the middle of it all, and an arrow pointing to left or righ…

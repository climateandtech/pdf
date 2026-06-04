# Multistage simulation `opus-multistage-sim`

- PDF: `opus_global_esg_2025_en.pdf`
- Pass-1 `fast_text`: 9.446s (355.69 pages/min)
- Plan: OCR pages 2, VLM pages 53, table pages 29
- Pass-2 table (`fast_text_tables`): 29.481s (11 ranges, span 29 pp)
- Pass-2 VLM (`rich`): 203.921s (3 ranges, span 53 pp)
- Pass-2 OCR (`baseline`): 3.853s
- **Total simulated**: 246.701s (13.62 effective pages/min)

Docling page_range is contiguous (min..max per segment), so sparse page lists may over-count work vs a true per-page merge.

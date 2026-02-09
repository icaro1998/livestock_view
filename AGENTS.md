# AGENTS

## Scope
This repository supports Google Earth Engine (GEE) analysis with a Colab-first execution model and VS Code local editing.

## Core Rules
- Treat `data/catalog/gee_catalog.csv` as the canonical, pinned GEE dataset inventory.
- Do not use dataset IDs that are not listed in `data/catalog/gee_catalog.csv`.
- Keep secrets out of git. Use `.env.example` as the only tracked environment template.
- Prefer server-side EE operations (`map`, reducers, filters, compositing) over client-side loops.
- Avoid `getInfo()` for large objects. Only use it for lightweight diagnostics (e.g., `size()`, first-image bands).
- Always print dataset diagnostics before analysis:
  - dataset ID
  - selected bands
  - filtered collection size
- Always log full run configuration (project, AOI, date window, scale, CRS, thresholds, export paths).
- Keep parameters reproducible and explicit; avoid hidden defaults in notebooks/scripts.

## Reproducibility Conventions
- AOI variables must be explicit and versioned in notebook/script constants.
- Date windows must be explicit (`YYYY-MM-DD`) and logged at runtime.
- Export naming should encode at least:
  - source family (`s1`, `s2`, `dw`, `s3`, `jrc`)
  - product name
  - temporal label (`YYYY-MM` or `YYYY-MM-DD`)
  - spatial context (`10km`, `30km`, etc.)
- Export summaries should be written to CSV or console with status per month/range.

## What Runs Where
- VS Code (local): code editing, notebook editing, lightweight validation, catalog maintenance.
- Colab (cloud): GEE heavy compute, exports, long-running workflows.
- GEE server-side: filtering, compositing, reducers, image calculations.
- Drive/GCS storage:
  - Drive for quick/manual artifacts and small-to-medium outputs.
  - GCS for large rasters/time series and pipeline-grade exports.
- Earth Studio: visualization/rendering only (consume prepared exports/tiles; do not run heavy analytics there).

## Workflow Guardrails
- Before running a new workflow, verify dataset IDs against `gee_catalog.csv`.
- For each dataset step, print bands and collection counts first.
- For exports, pick format by data shape:
  - GeoTIFF for static/single-period rasters
  - Zarr/NetCDF for dense multi-time arrays
- If auth/project/quota fails, stop and resolve infra first (see `docs/GOOGLE_GEO_STACK.md` debug checklist).

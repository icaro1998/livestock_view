# GeoTIFF QGIS Fix (xarray -> GeoTIFF)

## Problem summary
QGIS can show a raster as "empty" when exported GeoTIFF metadata is incomplete or inconsistent (CRS missing, bad transform, all nodata, wrong slice, or invalid nodata handling).

This repo now includes:
- `src/export_geotiff.py`: robust exporter with spatial normalization + nodata/CRS/transform safeguards.
- `tools/audit_geotiff.py`: reproducible audit CLI with failure-mode detection and post-write validation.
- `logs/geotiff_audit_<timestamp>.log`: full run trace for every audit execution.

## Root causes covered
The audit explicitly checks and reports these hypotheses:
1. No CRS written.
2. Spatial coordinates not recognized (`lat/lon` not mapped to `x/y`).
3. Latitude orientation / transform mismatch risk.
4. Data all NaN/nodata.
5. Wrong dimension exported (time-like dims not sliced).
6. Nodata missing/invalid (including `NaN` nodata issues).
7. Bounds out of range for CRS (units mismatch).

## Exporter behavior (`src/export_geotiff.py`)
`normalize_spatial_da(da, crs=None)`:
- Detects spatial dims from `x/y`, `lon/lat`, `longitude/latitude`.
- Renames dims to exactly `x` and `y`.
- Requires coordinate vectors for both dims.
- Enforces monotonic coordinates (descending -> sorted ascending).
- Sets spatial dims via `rio.set_spatial_dims(x_dim='x', y_dim='y')`.
- Resolves CRS from `rio.crs` or attrs (`crs`, `spatial_ref`, `grid_mapping`, etc.).
- If CRS is missing, defaults to `EPSG:4326` only when coordinate ranges look like degrees.
- Otherwise fails fast and asks for explicit CRS (example: `EPSG:32720`).
- Writes an affine transform inferred from regular-grid coordinate spacing.

`export_geotiff(da, path, nodata=None, dtype=None, compress='deflate')`:
- Normalizes spatial metadata.
- Handles dimensions so output is either:
  - single band `(y, x)`
  - multiband `(band, y, x)`
- For time-like dims (`time`, `month`, `date`, `datetime`, `bnds`, `bounds`), slices last frame by default.
- Picks nodata in this order:
  1. explicit `nodata` argument
  2. `rio.nodata`
  3. attrs (`_FillValue`, `missing_value`, etc.)
  4. fallback by dtype: `-9999.0` (float), `0` (uint), `-9999` (int)
- Converts `NaN` nodata to numeric nodata when needed.
- Writes CRS and nodata explicitly.
- Writes tiled/compressed GeoTIFF with BigTIFF safety.

## Audit CLI
### Basic
```bash
python tools/audit_geotiff.py --input <path> --var <name> --out output/audited.tif
```

### Demo mode (synthetic, reproducible)
```bash
python tools/audit_geotiff.py --demo --out output/geotiff_audit_demo.tif
```

### With explicit CRS
```bash
python tools/audit_geotiff.py --input data.nc --var flood --crs EPSG:32720 --out output/flood_fixed.tif
```

Audit output includes sections:
- `A) Data Validity`
- `B) Spatial Reference / Transform`
- `C) Nodata Handling`
- `D) Band/Time Dimension Correctness`
- `E) GeoTIFF Integrity Post-Write`
- `Root-Cause Hypotheses`

Every run writes a full log file to `logs/geotiff_audit_<timestamp>.log`.

## QGIS validation checklist
1. Add raster layer (`Layer > Add Layer > Add Raster Layer`).
2. Open layer properties -> `Information`:
   - CRS is present.
   - Extent is non-zero and plausible.
3. If right-click actions are limited, use:
   - `View > Panels > Layer Styling`
   - confirm renderer is active.
4. In `Layer Properties > Symbology`:
   - click `Min/Max` -> `Load`.
5. If still "empty":
   - set project CRS to `EPSG:4326` (or layer CRS if different).
   - check pixel values are not all nodata.
6. Compare with audit log:
   - confirm `E) GeoTIFF Integrity` shows valid CRS, bounds, and non-empty band stats.

## Acceptance expectation
For a valid regular EPSG:4326 grid, the exported GeoTIFF should:
- Open in QGIS with correct extent.
- Show non-nodata pixels.
- Report CRS and bounds correctly in layer information.

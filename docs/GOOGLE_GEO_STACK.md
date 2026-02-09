# Google Geo Stack

## Operating Model
- Edit code/notebooks locally in VS Code.
- Execute heavy GEE workloads in Google Colab.
- Keep heavy artifacts in Drive or GCS; keep secrets outside git.
- Validate dataset IDs against `data/catalog/gee_catalog.csv` before execution.

## Auth Patterns
### Colab (recommended)
```python
import ee

ee.Authenticate()
ee.Initialize(project="YOUR_GCP_PROJECT_ID")
```

### Local fallback
Use local auth only for lightweight checks. Keep long GEE jobs in Colab.

## Dataset Discovery Rules
Before any analysis/export:
1. Print dataset ID.
2. Print selected bands.
3. Print collection size after AOI/date filters.
4. Keep these diagnostics in notebook output or logs.

Example pattern:
```python
col = ee.ImageCollection("COPERNICUS/S1_GRD").filterBounds(aoi).filterDate(start, end)
print("count:", col.size().getInfo())
first = ee.Image(col.first())
print("bands:", first.bandNames().getInfo())
```

## Export Rules
### Use GeoTIFF when
- single timestamp / static raster
- quick inspection in GIS
- compact exchange with QGIS/ArcGIS

### Use Zarr (or NetCDF) when
- long temporal stacks
- model-ready multi-dimensional arrays
- chunked/cloud-friendly processing

### Drive vs GCS
- Drive: ad-hoc/manual outputs, review artifacts.
- GCS: large rasters/time-series, pipeline integration, durable automation.

## Naming Conventions
Use deterministic names:
`<source>_<product>_<YYYY-MM or YYYY-MM-DD>_<buffer or resolution>.<ext>`

Examples:
- `s1_flood_diff_2025-08-31_30km.tif`
- `dw_water_prob_2025-08_30km.tif`
- `s1_series_2024-01_2026-01_30km.zarr`

## Debug Checklist
If something fails, check in this order:
1. **Auth**: was `ee.Authenticate()` run for this session?
2. **Project**: does `ee.Initialize(project=...)` use the correct GCP project?
3. **Permissions**:
   - EE access granted to account
   - Drive/GCS write permissions
4. **Quota**:
   - task quota reached
   - bandwidth/timeouts
5. **Dataset ID**:
   - exists in `data/catalog/gee_catalog.csv`
   - correct dataset type (Image vs ImageCollection)
6. **Filters**:
   - AOI too small/large
   - date window has no images
   - cloud/orbit filters too strict
7. **Exports**:
   - CRS/scale sane
   - folder/bucket exists
   - file naming collision handled

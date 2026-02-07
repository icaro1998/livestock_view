# Beni Flood Plan (Operational)

Location: -13.700278, -63.927778 (Huacaraje, Beni)
Focus: flood history + flood risk

## Recommended defaults (already set in pipeline)
- Sentinel-1 scale: 30 m
- Temporal: monthly (ME)
- Aggregation: median
- AOI: rectangular corridor (longer than wide)

## AOI guidance
- Use a long rectangle aligned with the river/valley.
- Example: width 20 km, height 80 km.

## Date windows (2018+ only to keep files smaller)

Base climatology (risk)
- 2018-01-01 to 2026-02-05
- Monthly, 30-100 m
- Output: recurrence/percentiles

Sentinel-1 operational
- 2018-01-01 to 2026-02-05
- Monthly (ME)
- Output: backscatter + flood_diff series

Extreme events (validation)
- 2018-01-01 to 2019-12-31
- 2019-01-01 to 2020-12-31
- 2023-01-01 to 2024-12-31
- Higher resolution where needed

Wet season focus (DJFMA)
- 2018-12-01 to 2019-04-30
- 2019-12-01 to 2020-04-30
- 2020-12-01 to 2021-04-30
- 2021-12-01 to 2022-04-30
- 2022-12-01 to 2023-04-30
- 2023-12-01 to 2024-04-30
- 2024-12-01 to 2025-04-30
- 2025-12-01 to 2026-04-30

## Example commands

All outputs, GeoTIFF only, clean output, Xarray backups:
```
python scripts/flood_pipeline.py --project-id gen-lang-client-0296388721 --mode all --geotiff-only --clean-out-dir --xarray-backups --aoi-width-km 20 --aoi-height-km 80 --out-dir output/flood
```

Sentinel-1 series only (monthly, 2018+):
```
python scripts/flood_pipeline.py --project-id gen-lang-client-0296388721 --mode s1-series --s1-series-start 2018-01-01 --s1-series-end 2026-02-05 --aoi-width-km 20 --aoi-height-km 80 --out-dir output/flood
```

Snapshots in batches (12 months per batch):
```
python scripts/flood_pipeline.py --project-id gen-lang-client-0296388721 --mode snapshots --snapshots-geotiff --snapshots-offset 0 --snapshots-max 12 --aoi-width-km 20 --aoi-height-km 80 --out-dir output/flood

python scripts/flood_pipeline.py --project-id gen-lang-client-0296388721 --mode snapshots --snapshots-geotiff --snapshots-offset 12 --snapshots-max 12 --aoi-width-km 20 --aoi-height-km 80 --out-dir output/flood
```

Resume after interruption (skip already written snapshots):
```
python scripts/flood_pipeline.py --project-id gen-lang-client-0296388721 --mode snapshots --snapshots-geotiff --snapshots-max 12 --snapshots-resume --aoi-width-km 20 --aoi-height-km 80 --out-dir output/flood
```

Interactive export by date range (asks for MM/YYYY, exports 1 file per month):
```
python scripts/run_yearly_snapshots.py
```
Logs are written by default to `output/flood/logs/run_range_YYYYMMDD_HHMMSS.log`.
The script will print which months already exist (snapshots + master NetCDF) before prompting.
Direct access (double-click):
- `Flood data/Run Flood Download.bat`

Date range without prompt (MM/YYYY):
```
python scripts/run_yearly_snapshots.py --from 01/2024 --to 12/2024
python scripts/run_yearly_snapshots.py --from 01/2025 --to 12/2025
```

Custom date range + orbit (example: descending orbit only):
```
python scripts/run_yearly_snapshots.py --from 10/2024 --to 04/2025 --orbit DESCENDING --polarization VV
```

Output mode:
- `--output-mode merge` (default) merges into `output/flood/master/s1_flood_diff_series.nc`
- `--output-mode separate` keeps only the range-specific series file
Merging is handled inside `scripts/run_yearly_snapshots.py` (no separate merge command needed).

Output folders (organized):
- `output/flood/snapshots/` monthly GeoTIFFs (batches)
- `output/flood/xarray/` range-specific NetCDF series
- `output/flood/master/` merged master NetCDF (`s1_flood_diff_series.nc`)
- `output/flood/derived/` derived products (frequency map + timelapse GeoTIFF)
- `output/flood/logs/` run logs

Missing months handling:
- The script prints existing coverage and checks missing months after download.
- Use `--fill-missing yes` to auto-download missing ranges, or `--fill-missing no` to skip.

Resilience:
- Resume verification is enabled by default; corrupted GeoTIFFs are deleted and re-downloaded automatically.
- Pause/resume keys during snapshot export: `P` to pause/resume, `Q` to stop after current snapshot.
- Earth Engine init now retries on network/auth errors (configurable with `--ee-retries` and `--ee-retry-wait`).
- Preflight verification runs by default and logs to `output/flood/logs/verify_YYYYMMDD_HHMMSS.log` (use `--no-verify-existing` to skip).


Build a flood-frequency GeoTIFF from the NetCDF (counts months with flood_diff <= -1.0):
```
python scripts/build_flood_frequency.py --input output/flood/master/s1_flood_diff_series.nc
```
Output: `output/flood/derived/flood_diff_frequency.tif` (override with `--output`).

Timelapse GeoTIFF (single multiband file) is created automatically:
- `output/flood/derived/s1_flood_diff_timelapse.tif`
Apply QGIS style: `qgis/styles/flood_diff_timelapse.qml`

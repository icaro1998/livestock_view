# Dataset Catalog

Canonical inventory generated automatically from current workspace files.

| Dataset ID | Strong Arm | Category | Path | Files | Size | Date Range |
|---|---|---|---|---:|---:|---|
| `flood_master_10km` | `hydrology` | `flood_core` | `output/flood/master_10km` | 3 | 167.1MB | 2024-01-01 -> 2026-01-01 |
| `flood_water_evolution_2025` | `hydrology` | `flood_core` | `output/flood/water_evolution_10km_2025` | 29 | 1.8MB | 2025-01-31 -> 2025-12-31 |
| `flood_context_10km` | `topography` | `terrain_hydrology` | `output/flood/context_10km` | 12 | 12.1MB |  |
| `flood_hazard_rp20_100km` | `hydrology` | `hazard` | `output/flood/hazard_100km_rp20` | 5 | 23.9MB |  |
| `flood_hazard_rp50_100km` | `hydrology` | `hazard` | `output/flood/hazard_100km_rp50` | 5 | 24.3MB |  |
| `flood_hazard_rp100_100km` | `hydrology` | `hazard` | `output/flood/hazard_100km_rp100` | 5 | 24.6MB |  |
| `optical_aux_2025` | `hydrology` | `optical_aux` | `output/flood/additional_10km_2025` | 193 | 868.0MB | 2025-01-01 -> 2025-12-01 |
| `s2_truecolor_monthly_best_2025` | `world_imagery` | `satellite_rgb` | `output/sentinel2_truecolor_best_10km_2025` | 13 | 95.1MB | 2025-01-01 -> 2025-12-01 |
| `s2_truecolor_daily_best` | `world_imagery` | `satellite_rgb` | `output/sentinel2_truecolor_daily_10km` | 2 | 3.5MB | 2025-03-20 -> 2025-03-20 |
| `s2_truecolor_daily_s2cloudprob` | `world_imagery` | `satellite_rgb` | `output/sentinel2_truecolor_daily_10km_s2cloudprob` | 2 | 3.4MB | 2025-03-20 -> 2025-03-20 |
| `s2_truecolor_daily_cloudscoreplus` | `world_imagery` | `satellite_rgb` | `output/sentinel2_truecolor_daily_10km_csp` | 2 | 3.1MB | 2025-03-20 -> 2025-03-20 |
| `s2_truecolor_daily_mosaic_cloudscoreplus` | `world_imagery` | `satellite_rgb` | `output/sentinel2_truecolor_daily_10km_mosaic_csp` | 2 | 10.4MB | 2025-03-20 -> 2025-03-20 |
| `terrain_context_raw` | `topography` | `terrain_hydrology` | `output/terrain_context` | 35 | 26.5GB |  |
| `bundle_2025_10km` | `hydrology` | `bundle` | `output/dataset_bundle_2025_10km` | 240 | 962.6MB | 2024-01-01 -> 2026-01-01 |
| `legacy_flood_2025` | `hydrology` | `legacy` | `output/flood_2025` | 117 | 519.5MB | 2025-01-01 -> 2026-01-01 |

## Quick Rule

- For satellite RGB (photo-like): use `output/sentinel2_truecolor_*`.
- For flood analytics: use `output/flood/*` or the curated `output/dataset_bundle_2025_10km`.
- Legacy outputs are kept in place for script compatibility; do not delete without backup.

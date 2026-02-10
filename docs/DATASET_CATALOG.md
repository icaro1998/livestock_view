# Dataset Catalog

Canonical inventory generated automatically from current workspace files.

| Dataset ID | Strong Arm | Category | Path | Files | Size | Date Range |
|---|---|---|---|---:|---:|---|
| `flood_master_10km` | `hydrology` | `flood_core` | `output_by_buffer/10km/flood/master_10km` | 3 | 167.1MB | 2024-01-01 -> 2026-01-01 |
| `flood_water_evolution_2025` | `hydrology` | `flood_core` | `output_by_buffer/10km/flood/water_evolution_10km_2025` | 29 | 1.9MB | 2025-01-31 -> 2025-12-31 |
| `flood_context_10km` | `topography` | `terrain_hydrology` | `output_by_buffer/10km/flood/context_10km` | 12 | 12.1MB |  |
| `flood_hazard_rp20_30km` | `hydrology` | `hazard` | `output_by_buffer/30km/flood/hazard_30km_rp20` | 2 | 2.6MB |  |
| `flood_hazard_rp50_30km` | `hydrology` | `hazard` | `output_by_buffer/30km/flood/hazard_30km_rp50` | 2 | 2.7MB |  |
| `flood_hazard_rp100_30km` | `hydrology` | `hazard` | `output_by_buffer/30km/flood/hazard_30km_rp100` | 2 | 2.7MB |  |
| `optical_aux_2025` | `hydrology` | `optical_aux` | `output_by_buffer/10km/flood/additional_10km_2025` | 193 | 868.7MB | 2025-01-01 -> 2025-12-01 |
| `s2_truecolor_monthly_best_2025` | `world_imagery` | `satellite_rgb` | `output_by_buffer/10km/sentinel2_truecolor_best_10km_2025` | 13 | 95.1MB | 2025-01-01 -> 2025-12-01 |
| `s2_truecolor_daily_best` | `world_imagery` | `satellite_rgb` | `output_by_buffer/10km/sentinel2_truecolor_daily_10km` | 2 | 3.5MB | 2025-03-20 -> 2025-03-20 |
| `s2_truecolor_daily_s2cloudprob` | `world_imagery` | `satellite_rgb` | `output_by_buffer/10km/sentinel2_truecolor_daily_10km_s2cloudprob` | 2 | 3.5MB | 2025-03-20 -> 2025-03-20 |
| `s2_truecolor_daily_cloudscoreplus` | `world_imagery` | `satellite_rgb` | `output_by_buffer/10km/sentinel2_truecolor_daily_10km_csp` | 2 | 3.1MB | 2025-03-20 -> 2025-03-20 |
| `s2_truecolor_daily_mosaic_cloudscoreplus` | `world_imagery` | `satellite_rgb` | `output_by_buffer/10km/sentinel2_truecolor_daily_10km_mosaic_csp` | 2 | 10.4MB | 2025-03-20 -> 2025-03-20 |
| `terrain_context_raw` | `topography` | `terrain_hydrology` | `output_by_buffer/30km/flood/context_30km` | 9 | 107.9MB |  |
| `bundle_2025_10km` | `hydrology` | `bundle` | `output_by_buffer/10km/dataset_bundle_2025_10km` | 240 | 963.3MB | 2024-01-01 -> 2026-01-01 |
| `legacy_flood_2025` | `hydrology` | `legacy` | `output_by_buffer/30km/flood_30km` | 44 | 1.6GB | 2024-01-01 -> 2026-01-01 |

## Quick Rule

- For satellite RGB (photo-like): use `output_by_buffer/10km/sentinel2_truecolor_*` or `output_by_buffer/30km/sentinel2_truecolor_*`.
- For flood analytics: use `output_by_buffer/10km/flood/*`, `output_by_buffer/30km/flood/*`, or `output_by_buffer/30km/flood_30km/*`.
- Keep paths under `output_by_buffer/*` as the canonical separated structure.

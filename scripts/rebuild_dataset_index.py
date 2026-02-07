#!/usr/bin/env python3
"""
Build a repository dataset catalog without moving/deleting files.

Outputs:
- output/_index/DATASET_INDEX.csv
- docs/DATASET_CATALOG.md
- output/_index/open_datasets.ps1
- output/_index/STRONG_ARMS.md
- output/strong_arms/*
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output"
DOCS = ROOT / "docs"
INDEX_DIR = OUTPUT / "_index"
STRONG_ARMS_DIR = OUTPUT / "strong_arms"


DATE_RE_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})")
DATE_RE_YM = re.compile(r"(\d{4}-\d{2})(?!-\d{2})")


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    strong_arm: str
    category: str
    rel_path: str
    description: str


ARM_META = {
    "hydrology": {
        "title": "Hydrology",
        "description": "Water-focused datasets (flood masks, flood series, hazards, hydro auxiliaries).",
    },
    "topography": {
        "title": "Topography",
        "description": "Ground/elevation datasets (DEM, slope, terrain derivatives, contours).",
    },
    "world_imagery": {
        "title": "World Imagery",
        "description": "Photo-like satellite RGB imagery snapshots (Sentinel-2 true color).",
    },
}


DATASETS: list[DatasetSpec] = [
    DatasetSpec(
        "flood_master_10km",
        "hydrology",
        "flood_core",
        "output/flood/master_10km",
        "Sentinel-1 monthly master series",
    ),
    DatasetSpec(
        "flood_water_evolution_2025",
        "hydrology",
        "flood_core",
        "output/flood/water_evolution_10km_2025",
        "Monthly water masks/overflow/frequency for 2025",
    ),
    DatasetSpec(
        "flood_context_10km",
        "topography",
        "terrain_hydrology",
        "output/flood/context_10km",
        "DEM, slope, hillshade, JRC water",
    ),
    DatasetSpec(
        "flood_hazard_rp20_100km",
        "hydrology",
        "hazard",
        "output/flood/hazard_100km_rp20",
        "JRC hazard depth RP20",
    ),
    DatasetSpec(
        "flood_hazard_rp50_100km",
        "hydrology",
        "hazard",
        "output/flood/hazard_100km_rp50",
        "JRC hazard depth RP50",
    ),
    DatasetSpec(
        "flood_hazard_rp100_100km",
        "hydrology",
        "hazard",
        "output/flood/hazard_100km_rp100",
        "JRC hazard depth RP100",
    ),
    DatasetSpec(
        "optical_aux_2025",
        "hydrology",
        "optical_aux",
        "output/flood/additional_10km_2025",
        "Dynamic World + S3 OLCI + S2 auxiliary layers",
    ),
    DatasetSpec(
        "s2_truecolor_monthly_best_2025",
        "world_imagery",
        "satellite_rgb",
        "output/sentinel2_truecolor_best_10km_2025",
        "S2 SR Harmonized monthly best-scene true color",
    ),
    DatasetSpec(
        "s2_truecolor_daily_best",
        "world_imagery",
        "satellite_rgb",
        "output/sentinel2_truecolor_daily_10km",
        "S2 SR daily true color (best scene)",
    ),
    DatasetSpec(
        "s2_truecolor_daily_s2cloudprob",
        "world_imagery",
        "satellite_rgb",
        "output/sentinel2_truecolor_daily_10km_s2cloudprob",
        "S2 SR daily true color masked with S2 cloud probability",
    ),
    DatasetSpec(
        "s2_truecolor_daily_cloudscoreplus",
        "world_imagery",
        "satellite_rgb",
        "output/sentinel2_truecolor_daily_10km_csp",
        "S2 SR daily true color masked with Cloud Score+",
    ),
    DatasetSpec(
        "s2_truecolor_daily_mosaic_cloudscoreplus",
        "world_imagery",
        "satellite_rgb",
        "output/sentinel2_truecolor_daily_10km_mosaic_csp",
        "S2 SR daily mosaic (+/- window) masked with Cloud Score+",
    ),
    DatasetSpec(
        "terrain_context_raw",
        "topography",
        "terrain_hydrology",
        "output/terrain_context",
        "Terrain context and derived contours",
    ),
    DatasetSpec(
        "bundle_2025_10km",
        "hydrology",
        "bundle",
        "output/dataset_bundle_2025_10km",
        "Curated consolidated bundle for 2025 analysis",
    ),
    DatasetSpec(
        "legacy_flood_2025",
        "hydrology",
        "legacy",
        "output/flood_2025",
        "Legacy 2025 outputs retained for compatibility",
    ),
]


def _human_size(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size_bytes)
    for u in units:
        if value < 1024.0 or u == units[-1]:
            return f"{value:.1f}{u}"
        value /= 1024.0
    return f"{size_bytes}B"


def _to_windows_path(path: Path) -> str:
    s = str(path)
    m = re.match(r"^/mnt/([a-zA-Z])/(.*)$", s)
    if m:
        drive = m.group(1).upper()
        rest = m.group(2).replace("/", "\\")
        return f"{drive}:\\{rest}"
    return s


def _iter_files(path: Path) -> Iterable[Path]:
    if not path.exists():
        return []
    return (p for p in path.rglob("*") if p.is_file())


def _extract_dates_from_name(name: str) -> list[str]:
    out: list[str] = []
    for m in DATE_RE_ISO.findall(name):
        out.append(m)
    for m in DATE_RE_YM.findall(name):
        out.append(f"{m}-01")
    return out


def _collect_stats(path: Path) -> dict[str, str]:
    if not path.exists():
        return {
            "exists": "no",
            "file_count": "0",
            "tif_count": "0",
            "nc_count": "0",
            "csv_count": "0",
            "size_bytes": "0",
            "size_human": "0B",
            "date_first": "",
            "date_last": "",
            "latest_mtime_utc": "",
        }

    files = list(_iter_files(path))
    size_bytes = sum(p.stat().st_size for p in files)
    tif_count = sum(1 for p in files if p.suffix.lower() in {".tif", ".tiff"})
    nc_count = sum(1 for p in files if p.suffix.lower() == ".nc")
    csv_count = sum(1 for p in files if p.suffix.lower() == ".csv")
    dates = sorted({d for p in files for d in _extract_dates_from_name(p.name)})

    latest_mtime = ""
    if files:
        latest = max(p.stat().st_mtime for p in files)
        latest_mtime = datetime.utcfromtimestamp(latest).isoformat() + "Z"

    return {
        "exists": "yes",
        "file_count": str(len(files)),
        "tif_count": str(tif_count),
        "nc_count": str(nc_count),
        "csv_count": str(csv_count),
        "size_bytes": str(size_bytes),
        "size_human": _human_size(size_bytes),
        "date_first": dates[0] if dates else "",
        "date_last": dates[-1] if dates else "",
        "latest_mtime_utc": latest_mtime,
    }


def build_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for spec in DATASETS:
        abs_path = ROOT / spec.rel_path
        stats = _collect_stats(abs_path)
        row = {
            "dataset_id": spec.dataset_id,
            "strong_arm": spec.strong_arm,
            "category": spec.category,
            "path": str(abs_path),
            "path_windows": _to_windows_path(abs_path),
            "rel_path": spec.rel_path,
            "description": spec.description,
            **stats,
        }
        rows.append(row)
    return rows


def write_csv(rows: list[dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "dataset_id",
        "strong_arm",
        "category",
        "path",
        "path_windows",
        "rel_path",
        "description",
        "exists",
        "file_count",
        "tif_count",
        "nc_count",
        "csv_count",
        "size_bytes",
        "size_human",
        "date_first",
        "date_last",
        "latest_mtime_utc",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def write_markdown(rows: list[dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Dataset Catalog")
    lines.append("")
    lines.append("Canonical inventory generated automatically from current workspace files.")
    lines.append("")
    lines.append("| Dataset ID | Strong Arm | Category | Path | Files | Size | Date Range |")
    lines.append("|---|---|---|---|---:|---:|---|")
    for r in rows:
        date_range = ""
        if r["date_first"] and r["date_last"]:
            date_range = f'{r["date_first"]} -> {r["date_last"]}'
        lines.append(
            f'| `{r["dataset_id"]}` | `{r["strong_arm"]}` | `{r["category"]}` | `{r["rel_path"]}` | '
            f'{r["file_count"]} | {r["size_human"]} | {date_range} |'
        )
    lines.append("")
    lines.append("## Quick Rule")
    lines.append("")
    lines.append("- For satellite RGB (photo-like): use `output/sentinel2_truecolor_*`.")
    lines.append("- For flood analytics: use `output/flood/*` or the curated `output/dataset_bundle_2025_10km`.")
    lines.append("- Legacy outputs are kept in place for script compatibility; do not delete without backup.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_powershell(rows: list[dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Auto-generated dataset shortcuts")
    lines.append("$DatasetPaths = @{")
    for r in rows:
        lines.append(f"  '{r['dataset_id']}' = '{r['path_windows']}'")
    lines.append("}")
    lines.append("")
    lines.append("$DatasetStrongArms = @{")
    for r in rows:
        lines.append(f"  '{r['dataset_id']}' = '{r['strong_arm']}'")
    lines.append("}")
    lines.append("")
    lines.append("function Show-Datasets { $DatasetPaths.GetEnumerator() | Sort-Object Name }")
    lines.append("function Show-StrongArms { $DatasetStrongArms.GetEnumerator() | Sort-Object Name }")
    lines.append("function Show-ArmDatasets([string]$Arm) {")
    lines.append("  $DatasetStrongArms.GetEnumerator() | Where-Object { $_.Value -eq $Arm } | Sort-Object Name")
    lines.append("}")
    lines.append("")
    lines.append("function Open-Dataset([string]$Name) {")
    lines.append("  if (-not $DatasetPaths.ContainsKey($Name)) {")
    lines.append("    Write-Host \"Unknown dataset id: $Name\" -ForegroundColor Red")
    lines.append("    Show-Datasets")
    lines.append("    return")
    lines.append("  }")
    lines.append("  $p = $DatasetPaths[$Name]")
    lines.append("  if (-not (Test-Path $p)) {")
    lines.append("    Write-Host \"Path not found: $p\" -ForegroundColor Red")
    lines.append("    return")
    lines.append("  }")
    lines.append("  explorer.exe $p")
    lines.append("}")
    lines.append("")
    lines.append(
        "Write-Host 'Loaded dataset shortcuts. Use: Show-Datasets / Show-StrongArms / Show-ArmDatasets <arm> / Open-Dataset <dataset_id>'"
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def write_strong_arms(rows: list[dict[str, str]], out_dir: Path, md_path: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    md_lines: list[str] = []
    md_lines.append("# Strong Arms")
    md_lines.append("")
    md_lines.append("Operational grouping into three branches:")
    md_lines.append("- `hydrology`")
    md_lines.append("- `topography`")
    md_lines.append("- `world_imagery`")
    md_lines.append("")

    for arm_key, meta in ARM_META.items():
        arm_rows = [r for r in rows if r["strong_arm"] == arm_key]
        arm_dir = out_dir / arm_key
        arm_dir.mkdir(parents=True, exist_ok=True)

        # Branch CSV
        csv_path = arm_dir / "DATASETS.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "dataset_id",
                    "category",
                    "path_windows",
                    "rel_path",
                    "description",
                    "file_count",
                    "size_human",
                    "date_first",
                    "date_last",
                ],
            )
            w.writeheader()
            for r in arm_rows:
                w.writerow({k: r[k] for k in w.fieldnames})

        # Branch README
        readme_lines: list[str] = []
        readme_lines.append(f"# {meta['title']}")
        readme_lines.append("")
        readme_lines.append(meta["description"])
        readme_lines.append("")
        readme_lines.append("Datasets:")
        for r in arm_rows:
            date_range = ""
            if r["date_first"] and r["date_last"]:
                date_range = f" ({r['date_first']} -> {r['date_last']})"
            readme_lines.append(
                f"- `{r['dataset_id']}`: `{r['rel_path']}` [{r['size_human']}, files={r['file_count']}]"
                f"{date_range}"
            )
        readme_lines.append("")
        readme_lines.append("This is an index view only. Original files remain in their canonical locations.")
        (arm_dir / "README.md").write_text("\n".join(readme_lines), encoding="utf-8")

        md_lines.append(f"## {meta['title']}")
        md_lines.append("")
        md_lines.append(meta["description"])
        md_lines.append("")
        md_lines.append(f"- Index folder: `output/strong_arms/{arm_key}`")
        md_lines.append(f"- CSV: `output/strong_arms/{arm_key}/DATASETS.csv`")
        md_lines.append("")
        for r in arm_rows:
            md_lines.append(f"- `{r['dataset_id']}` -> `{r['rel_path']}`")
        md_lines.append("")

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(md_lines), encoding="utf-8")


def main() -> int:
    rows = build_rows()

    csv_path = INDEX_DIR / "DATASET_INDEX.csv"
    md_path = DOCS / "DATASET_CATALOG.md"
    ps_path = INDEX_DIR / "open_datasets.ps1"
    arms_md_path = INDEX_DIR / "STRONG_ARMS.md"

    write_csv(rows, csv_path)
    write_markdown(rows, md_path)
    write_powershell(rows, ps_path)
    write_strong_arms(rows, STRONG_ARMS_DIR, arms_md_path)

    print(f"Wrote: {csv_path}")
    print(f"Wrote: {md_path}")
    print(f"Wrote: {ps_path}")
    print(f"Wrote: {arms_md_path}")
    print(f"Wrote: {STRONG_ARMS_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

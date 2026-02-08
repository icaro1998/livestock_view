#!/usr/bin/env python3
"""
Create placeholder monthly snapshots for missing months in a date range.

This is useful to keep temporal animations continuous when Sentinel-1 has
no images for specific months.
"""

from __future__ import annotations

import argparse
import calendar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
from typing import Optional

import numpy as np
import rasterio


_SNAPSHOT_RE = re.compile(r"^s1_flood_diff_(\d{4})-(\d{2})-(\d{2})\.tif$")


@dataclass(frozen=True)
class MonthPoint:
    year: int
    month: int

    @property
    def month_index(self) -> int:
        return self.year * 12 + (self.month - 1)

    @property
    def eom_label(self) -> str:
        day = calendar.monthrange(self.year, self.month)[1]
        return f"{self.year:04d}-{self.month:02d}-{day:02d}"

    @property
    def filename(self) -> str:
        return f"s1_flood_diff_{self.eom_label}.tif"


def _parse_mm_yyyy(value: str) -> MonthPoint:
    raw = value.strip()
    parts = raw.split("/")
    if len(parts) != 2:
        raise ValueError("Expected MM/YYYY format.")
    month_str, year_str = parts
    if not (month_str.isdigit() and year_str.isdigit()):
        raise ValueError("Month and year must be numeric.")
    month = int(month_str)
    year = int(year_str)
    if not (1 <= month <= 12):
        raise ValueError("Month must be 01..12.")
    return MonthPoint(year=year, month=month)


def _month_range(start: MonthPoint, end: MonthPoint) -> list[MonthPoint]:
    s = start.month_index
    e = end.month_index
    if e < s:
        raise ValueError("'to' month must be after or equal to 'from'.")
    out: list[MonthPoint] = []
    for idx in range(s, e + 1):
        year = idx // 12
        month = (idx % 12) + 1
        out.append(MonthPoint(year=year, month=month))
    return out


def _existing_snapshots(snapshots_dir: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for path in snapshots_dir.glob("s1_flood_diff_*.tif"):
        m = _SNAPSHOT_RE.match(path.name)
        if not m:
            continue
        out[path.name] = path
    return out


def _extract_month_point(name: str) -> Optional[MonthPoint]:
    m = _SNAPSHOT_RE.match(name)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2))
    return MonthPoint(year=year, month=month)


def _nearest_template(target: MonthPoint, existing: dict[str, Path]) -> Path:
    candidates: list[tuple[int, Path]] = []
    for name, path in existing.items():
        pt = _extract_month_point(name)
        if pt is None:
            continue
        diff = abs(pt.month_index - target.month_index)
        candidates.append((diff, path))
    if not candidates:
        raise RuntimeError("No existing snapshots available as template.")
    candidates.sort(key=lambda x: (x[0], str(x[1])))
    return candidates[0][1]


def _write_placeholder_from_template(
    out_path: Path,
    template_path: Path,
    method: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if method == "copy-nearest":
        shutil.copy2(template_path, out_path)
        with rasterio.open(out_path, "r+") as dst:
            dst.update_tags(
                placeholder="true",
                placeholder_method=method,
                placeholder_source=template_path.name,
                placeholder_created_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            )
        return

    with rasterio.open(template_path) as src:
        profile = src.profile.copy()
        profile.update(driver="GTiff")
        dtype = np.dtype(src.dtypes[0])
        if method == "nan":
            if np.issubdtype(dtype, np.floating):
                fill_value = np.nan
                profile["nodata"] = np.nan
            else:
                # If source dtype is integer, use zero fallback.
                fill_value = 0
                profile["nodata"] = 0
        else:
            fill_value = 0
            profile["nodata"] = 0

        data = np.full(
            (src.count, src.height, src.width),
            fill_value,
            dtype=src.dtypes[0],
        )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(data)
            for idx in range(1, src.count + 1):
                try:
                    desc = src.descriptions[idx - 1]
                    if desc:
                        dst.set_band_description(idx, desc)
                except Exception:
                    pass
            dst.update_tags(
                placeholder="true",
                placeholder_method=method,
                placeholder_source=template_path.name,
                placeholder_created_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create placeholder snapshot GeoTIFFs for missing months."
    )
    parser.add_argument("--from", dest="from_month", required=True, help="MM/YYYY")
    parser.add_argument("--to", dest="to_month", required=True, help="MM/YYYY")
    parser.add_argument(
        "--snapshots-dir",
        default="output/flood/snapshots",
        help="Directory containing s1_flood_diff_YYYY-MM-DD.tif files.",
    )
    parser.add_argument(
        "--method",
        choices=["nan", "zero", "copy-nearest"],
        default="nan",
        help="Placeholder content strategy.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files if present.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print actions; do not write files.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    snapshots_dir = Path(args.snapshots_dir)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    start = _parse_mm_yyyy(args.from_month)
    end = _parse_mm_yyyy(args.to_month)
    months = _month_range(start, end)

    existing = _existing_snapshots(snapshots_dir)
    missing = [m for m in months if m.filename not in existing]

    print(f"Snapshots dir: {snapshots_dir}")
    print(f"Range: {start.eom_label} .. {end.eom_label} ({len(months)} months)")
    print(f"Existing in range: {len(months) - len(missing)}")
    print(f"Missing in range: {len(missing)}")

    created = 0
    skipped = 0

    for month in missing:
        out_path = snapshots_dir / month.filename
        if out_path.exists() and not args.force:
            print(f"SKIP (exists): {out_path.name}")
            skipped += 1
            continue
        template = _nearest_template(month, existing)
        print(f"CREATE: {out_path.name} (template={template.name}, method={args.method})")
        if not args.dry_run:
            _write_placeholder_from_template(out_path, template, args.method)
            existing[out_path.name] = out_path
        created += 1

    print(f"Done. created={created} skipped={skipped} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

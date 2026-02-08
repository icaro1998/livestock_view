#!/usr/bin/env python3
"""
Validate GeoTIFF and NetCDF outputs by opening each file and reading a tiny sample.
Exits with code 1 when corrupted/unreadable files are found.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import rasterio
import xarray as xr


DEFAULT_PATTERNS = [
    "flood_30km/s1_flood_diff_20*.tif",
    "flood_30km/surface_water_*.tif",
    "flood_30km/terrain_*.tif",
    "sentinel2_truecolor_best_30km_2025/s2_truecolor_2025-*.tif",
    "flood/*.tif",
    "flood/*.tiff",
    "flood/**/*.tif",
    "flood/**/*.tiff",
    "flood_30km/*.nc",
    "flood/*.nc",
    "flood/**/*.nc",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate GeoTIFF and NetCDF outputs under an output root."
    )
    parser.add_argument("--root", default="output", help="Output root directory.")
    parser.add_argument(
        "--pattern",
        action="append",
        dest="patterns",
        default=[],
        help="Glob pattern relative to --root. Can be passed multiple times.",
    )
    parser.add_argument(
        "--fail-if-empty",
        action="store_true",
        help="Return non-zero if no files matched.",
    )
    return parser.parse_args()


def _unique_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in sorted(paths):
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _validate_tif(path: Path) -> tuple[bool, str]:
    try:
        with rasterio.open(path) as ds:
            if ds.count < 1:
                return False, "no bands"
            h = min(5, ds.height)
            w = min(5, ds.width)
            ds.read(1, window=((0, h), (0, w)))
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def _validate_nc(path: Path) -> tuple[bool, str]:
    try:
        ds = xr.open_dataset(path)
        ds.close()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    args = _parse_args()
    root = Path(args.root)
    patterns = args.patterns or DEFAULT_PATTERNS

    matched: list[Path] = []
    for pattern in patterns:
        matched.extend(root.glob(pattern))
    files = _unique_paths([p for p in matched if p.is_file()])

    tifs = [p for p in files if p.suffix.lower() in {".tif", ".tiff"}]
    ncs = [p for p in files if p.suffix.lower() == ".nc"]

    bad: list[tuple[str, str]] = []
    for path in tifs:
        ok, detail = _validate_tif(path)
        if not ok:
            bad.append((str(path), detail))
    for path in ncs:
        ok, detail = _validate_nc(path)
        if not ok:
            bad.append((str(path), detail))

    print(f"Root: {root}")
    print(f"Matched files: {len(files)}")
    print(f"TIFF: {len(tifs)} NC: {len(ncs)}")
    print(f"BAD: {len(bad)}")
    for path, error in bad:
        print(f"- {path} :: {error}")

    if args.fail_if_empty and not files:
        return 2
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())

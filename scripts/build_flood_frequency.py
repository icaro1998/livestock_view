#!/usr/bin/env python3
"""
Build a flood frequency GeoTIFF from a Sentinel-1 flood_diff NetCDF.
Counts how many months are <= threshold (default -1.0) per pixel.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import xarray as xr
import rioxarray  # noqa: F401 - enables .rio

from flood_pipeline import _ensure_transform, _set_spatial_dims


def _infer_output_path(in_path: Path) -> Path:
    base_dir = in_path.parent.parent if in_path.parent.name == "master" else in_path.parent
    return base_dir / "derived" / "flood_diff_frequency.tif"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a flood frequency GeoTIFF from a flood_diff NetCDF."
    )
    parser.add_argument(
        "--input",
        default="output/flood/master/s1_flood_diff_series.nc",
        help="Input NetCDF (default: output/flood/master/s1_flood_diff_series.nc)",
    )
    parser.add_argument(
        "--var",
        default="flood_diff",
        help="Variable name (default: flood_diff)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=-1.0,
        help="Flood threshold (<= value counted as wet month). Default: -1.0",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output GeoTIFF path (default: output/flood/derived/flood_diff_frequency.tif).",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Input not found: {in_path}")

    ds = xr.open_dataset(in_path)
    if args.var not in ds.data_vars:
        raise SystemExit(f"Variable '{args.var}' not found in {in_path.name}")

    da = ds[args.var]
    if "time" not in da.dims:
        raise SystemExit("Input has no time dimension; cannot compute frequency.")

    out_path = Path(args.output) if args.output else _infer_output_path(in_path)

    mask = da <= args.threshold
    freq = mask.sum("time").astype("int16").rename("flood_frequency")

    freq = _set_spatial_dims(freq)
    freq = freq.rio.write_crs("EPSG:4326", inplace=False)
    freq = _ensure_transform(freq)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    freq.rio.to_raster(
        out_path,
        compress="LZW",
        tiled=True,
        blockxsize=256,
        blockysize=256,
        BIGTIFF="IF_SAFER",
    )
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

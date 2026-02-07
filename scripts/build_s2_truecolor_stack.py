#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

import rasterio


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Sentinel-2 true-color RGB stacks (B4,B3,B2).")
    p.add_argument(
        "--input-dir",
        default="output/flood/additional_10km_2025/sentinel2_sr_harmonized",
        help="Directory containing s2_B2/B3/B4_YYYY-MM.tif files.",
    )
    p.add_argument(
        "--out-dir",
        default="output/flood/additional_10km_2025/sentinel2_truecolor",
        help="Directory to write 3-band true-color TIFFs.",
    )
    p.add_argument(
        "--month",
        default="",
        help="Optional month YYYY-MM. If omitted, builds all detected months.",
    )
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def discover_months(input_dir: Path) -> list[str]:
    pat = re.compile(r"^s2_B4_(\d{4}-\d{2})\.tif$")
    months: list[str] = []
    for p in sorted(input_dir.glob("s2_B4_*.tif")):
        m = pat.match(p.name)
        if m:
            months.append(m.group(1))
    return months


def build_rgb_for_month(input_dir: Path, out_dir: Path, month: str, overwrite: bool) -> Path:
    b4 = input_dir / f"s2_B4_{month}.tif"  # red
    b3 = input_dir / f"s2_B3_{month}.tif"  # green
    b2 = input_dir / f"s2_B2_{month}.tif"  # blue
    for p in (b4, b3, b2):
        if not p.exists():
            raise FileNotFoundError(f"Missing input band: {p}")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"s2_truecolor_{month}.tif"
    if out_path.exists() and not overwrite:
        return out_path

    with rasterio.open(b4) as src_r, rasterio.open(b3) as src_g, rasterio.open(b2) as src_b:
        if (
            src_r.width != src_g.width
            or src_r.width != src_b.width
            or src_r.height != src_g.height
            or src_r.height != src_b.height
        ):
            raise RuntimeError(f"Band shapes differ for month {month}")

        profile = src_r.profile.copy()
        profile.update(
            count=3,
            compress="deflate",
            predictor=2,
            tiled=True,
            BIGTIFF="IF_SAFER",
        )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(src_r.read(1), 1)  # R
            dst.write(src_g.read(1), 2)  # G
            dst.write(src_b.read(1), 3)  # B
            if src_r.descriptions:
                dst.set_band_description(1, "B4_red")
            if src_g.descriptions:
                dst.set_band_description(2, "B3_green")
            if src_b.descriptions:
                dst.set_band_description(3, "B2_blue")
    return out_path


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    if args.month:
        months = [args.month]
    else:
        months = discover_months(input_dir)
    if not months:
        raise RuntimeError("No months found to build true-color stacks.")

    print(f"Input:  {input_dir}")
    print(f"Output: {out_dir}")
    print(f"Months: {len(months)}")
    ok = 0
    for month in months:
        out_path = build_rgb_for_month(input_dir, out_dir, month, overwrite=bool(args.overwrite))
        print(f"  {month}: {out_path}")
        ok += 1
    print(f"Built: {ok}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

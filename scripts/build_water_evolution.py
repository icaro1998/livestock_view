#!/usr/bin/env python3
"""
Build yearly/monthly water evolution products from Sentinel-1 monthly backscatter.

Inputs:
- NetCDF with a monthly time dimension and a `backscatter` variable

Outputs:
- Monthly binary water masks (GeoTIFF): masks/water_mask_YYYY-MM-DD.tif
- Monthly overflow masks (GeoTIFF, optional): overflow/overflow_mask_YYYY-MM-DD.tif
- Optional monthly backscatter GeoTIFFs: backscatter/backscatter_YYYY-MM-DD.tif
- Year summary layers:
  - derived/water_frequency_months.tif
  - derived/water_frequency_fraction.tif
  - derived/water_threshold_median.tif
  - derived/permanent_water_mask.tif
- CSV summary with centroid trajectory:
  - derived/water_monthly_summary.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
import rioxarray  # noqa: F401 - enables .rio

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.export_geotiff import ensure_dataarray, export_geotiff, normalize_spatial_da


def _infer_default_input() -> Path:
    preferred = [
        Path("output/flood_2025/xarray/s1_flood_diff_series_descending_vv_2025-01-01_2026-01-01.nc"),
        Path("output/flood/master/s1_flood_diff_series.nc"),
    ]
    for p in preferred:
        if p.exists():
            return p
    return preferred[0]


def _infer_default_out_dir(in_path: Path) -> Path:
    base = in_path.parent.parent if in_path.parent.name == "xarray" else in_path.parent
    return base / "water_evolution"


def _month_label(ts: Any) -> str:
    try:
        return np.datetime_as_string(ts, unit="D")
    except Exception:
        return str(ts)


def _otsu_threshold(values: np.ndarray, bins: int = 256) -> float:
    finite = np.asarray(values, dtype="float64")
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return float("nan")
    if np.allclose(finite.min(), finite.max()):
        return float(finite.min())

    lo = np.nanpercentile(finite, 0.5)
    hi = np.nanpercentile(finite, 99.5)
    clipped = finite[(finite >= lo) & (finite <= hi)]
    if clipped.size < 32:
        clipped = finite

    hist, edges = np.histogram(clipped, bins=bins)
    hist = hist.astype("float64")
    centers = (edges[:-1] + edges[1:]) / 2.0

    w0 = np.cumsum(hist)
    w1 = np.cumsum(hist[::-1])[::-1]
    mu0 = np.cumsum(hist * centers) / np.maximum(w0, 1e-12)
    mu1 = (np.cumsum((hist * centers)[::-1]) / np.maximum(w1[::-1], 1e-12))[::-1]
    sigma_b2 = w0[:-1] * w1[1:] * (mu0[:-1] - mu1[1:]) ** 2
    idx = int(np.argmax(sigma_b2))
    return float(centers[idx])


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = p2 - p1
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _pick_threshold(finite: np.ndarray, method: str, fixed: float, q: float) -> float:
    if finite.size == 0:
        return float("nan")
    if method == "fixed":
        return float(fixed)
    if method == "quantile":
        return float(np.nanquantile(finite, q))
    if method == "otsu":
        return _otsu_threshold(finite)
    raise ValueError(f"Unsupported threshold method: {method}")


def _majority_filter(mask: np.ndarray, min_neighbors: int, iterations: int) -> np.ndarray:
    if min_neighbors <= 0:
        return mask
    if min_neighbors > 9:
        raise ValueError("min_neighbors must be between 1 and 9 for a 3x3 neighborhood.")
    out = mask.astype(np.uint8)
    h, w = out.shape
    for _ in range(max(1, iterations)):
        p = np.pad(out, 1, mode="edge")
        total = np.zeros((h, w), dtype=np.uint8)
        for dy in range(3):
            for dx in range(3):
                total = total + p[dy : dy + h, dx : dx + w]
        out = (total >= min_neighbors).astype(np.uint8)
    return out.astype(bool)


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build water evolution products from Sentinel-1 monthly backscatter."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=_infer_default_input(),
        help="Input NetCDF with monthly `backscatter` and time dim.",
    )
    parser.add_argument(
        "--var",
        default="backscatter",
        help="Input variable name (default: backscatter).",
    )
    parser.add_argument(
        "--start",
        default="",
        help="Optional start month YYYY-MM (inclusive). Example: 2024-01",
    )
    parser.add_argument(
        "--end",
        default="",
        help="Optional end month YYYY-MM (inclusive). Example: 2025-12",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=0,
        help="Shortcut for full year range. Example: --year 2025",
    )
    parser.add_argument(
        "--threshold-method",
        choices=["otsu", "fixed", "quantile"],
        default="otsu",
        help="Water threshold method on backscatter (water = value <= threshold).",
    )
    parser.add_argument(
        "--fixed-threshold",
        type=float,
        default=-16.0,
        help="Used when --threshold-method fixed.",
    )
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.12,
        help="Used when --threshold-method quantile (0-1).",
    )
    parser.add_argument(
        "--global-threshold",
        action="store_true",
        help="Compute one threshold over the full selected period and reuse it for all months.",
    )
    parser.add_argument(
        "--min-neighbors",
        type=int,
        default=0,
        help="Post-filter: keep water pixel only if at least N neighbors (3x3) are water. 0 disables.",
    )
    parser.add_argument(
        "--neighbor-iters",
        type=int,
        default=1,
        help="Post-filter iterations for --min-neighbors.",
    )
    parser.add_argument(
        "--min-water-pixels",
        type=int,
        default=25,
        help="Minimum wet pixels to report centroid for a month.",
    )
    parser.add_argument(
        "--permanent-min-months",
        type=int,
        default=10,
        help="Months threshold to label permanent water in frequency products.",
    )
    parser.add_argument(
        "--write-overflow-masks",
        action="store_true",
        help="Write monthly overflow masks = water_mask AND NOT permanent_water_mask.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (default inferred from input).",
    )
    parser.add_argument(
        "--write-backscatter",
        action="store_true",
        help="Also export monthly backscatter GeoTIFFs.",
    )
    parser.add_argument(
        "--crs",
        default="EPSG:4326",
        help="CRS fallback when missing (default EPSG:4326).",
    )
    return parser.parse_args()


def _subset_months(da: xr.DataArray, start: str, end: str, year: int) -> xr.DataArray:
    if "time" not in da.dims:
        raise ValueError("Input variable has no time dimension.")

    out = da.sortby("time")
    if year:
        t0 = np.datetime64(f"{year:04d}-01-01")
        t1 = np.datetime64(f"{year + 1:04d}-01-01")
        out = out.sel(time=(out["time"] >= t0) & (out["time"] < t1))
        return out

    if start:
        out = out.sel(time=out["time"] >= np.datetime64(f"{start}-01"))
    if end:
        year_s, month_s = end.split("-")
        y = int(year_s)
        m = int(month_s)
        if m == 12:
            end_exclusive = np.datetime64(f"{y + 1:04d}-01-01")
        else:
            end_exclusive = np.datetime64(f"{y:04d}-{m + 1:02d}-01")
        out = out.sel(time=out["time"] < end_exclusive)
    return out


def main() -> int:
    args = parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input not found: {args.input}")
    if not (0.0 < args.quantile < 1.0):
        raise SystemExit("--quantile must be between 0 and 1.")

    ds = xr.open_dataset(args.input)
    da_raw = ensure_dataarray(ds, var=args.var)
    da_time = _subset_months(da_raw, start=args.start, end=args.end, year=args.year)
    if da_time.sizes.get("time", 0) == 0:
        raise SystemExit("No monthly slices in selected range.")

    out_dir = args.out_dir or _infer_default_out_dir(args.input)
    masks_dir = out_dir / "masks"
    overflow_dir = out_dir / "overflow"
    bs_dir = out_dir / "backscatter"
    derived_dir = out_dir / "derived"
    masks_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)
    if args.write_overflow_masks:
        overflow_dir.mkdir(parents=True, exist_ok=True)
    if args.write_backscatter:
        bs_dir.mkdir(parents=True, exist_ok=True)

    print("Building water evolution products")
    print(f"Input: {args.input}")
    print(f"Var: {args.var}")
    print(f"Threshold method: {args.threshold_method}")
    print(f"Months: {da_time.sizes.get('time', 0)}")
    print(f"Output dir: {out_dir}")
    if args.global_threshold:
        print("Threshold mode: global")
    if args.min_neighbors > 0:
        print(
            f"Post-filter: 3x3 majority min_neighbors={args.min_neighbors} "
            f"iterations={args.neighbor_iters}"
        )
    print(f"Permanent min months: {args.permanent_min_months}")

    mask_slices: list[xr.DataArray] = []
    valid_slices: list[xr.DataArray] = []
    threshold_slices: list[xr.DataArray] = []
    summary_rows: list[dict[str, Any]] = []

    prev_lon = None
    prev_lat = None
    global_threshold = None

    if args.global_threshold:
        full_values = np.asarray(da_time.values, dtype="float64")
        full_finite = full_values[np.isfinite(full_values)]
        global_threshold = _pick_threshold(
            full_finite,
            method=args.threshold_method,
            fixed=args.fixed_threshold,
            q=args.quantile,
        )
        print(f"Global threshold value: {global_threshold:.6f}")

    for i in range(da_time.sizes["time"]):
        month = da_time.isel(time=i).squeeze()
        month = normalize_spatial_da(month, crs=args.crs).transpose("y", "x")

        values = np.asarray(month.values, dtype="float64")
        finite = values[np.isfinite(values)]
        if global_threshold is not None:
            threshold = float(global_threshold)
        else:
            threshold = _pick_threshold(
                finite,
                method=args.threshold_method,
                fixed=args.fixed_threshold,
                q=args.quantile,
            )

        valid = np.isfinite(values)
        wet = valid & (values <= threshold)
        if args.min_neighbors > 0:
            wet = _majority_filter(
                wet,
                min_neighbors=int(args.min_neighbors),
                iterations=int(args.neighbor_iters),
            )

        wet_count = int(np.count_nonzero(wet))
        valid_count = int(np.count_nonzero(valid))
        wet_fraction = (wet_count / valid_count) if valid_count else float("nan")

        label = _month_label(month["time"].values)
        mask_da = xr.DataArray(
            wet.astype("uint8"),
            dims=("y", "x"),
            coords={"y": month["y"].values, "x": month["x"].values},
            name="water_mask",
            attrs={
                "long_name": "Binary monthly water mask",
                "water_definition": "1 if backscatter <= monthly threshold else 0",
                "threshold_method": args.threshold_method,
                "threshold_value": threshold,
                "time_label": label,
            },
        )
        mask_da = normalize_spatial_da(mask_da, crs=args.crs).transpose("y", "x")

        valid_da = xr.DataArray(
            valid.astype("uint8"),
            dims=("y", "x"),
            coords={"y": month["y"].values, "x": month["x"].values},
            name="valid_mask",
        )
        valid_da = normalize_spatial_da(valid_da, crs=args.crs).transpose("y", "x")

        thr_da = xr.DataArray(
            np.full(mask_da.shape, threshold, dtype="float32"),
            dims=("y", "x"),
            coords={"y": mask_da["y"].values, "x": mask_da["x"].values},
            name="monthly_threshold",
        )
        thr_da = normalize_spatial_da(thr_da, crs=args.crs).transpose("y", "x")

        date_str = label[:10]
        out_mask = masks_dir / f"water_mask_{date_str}.tif"
        export_geotiff(mask_da, out_mask, nodata=255, dtype="uint8", compress="deflate")

        if args.write_backscatter:
            out_bs = bs_dir / f"backscatter_{date_str}.tif"
            export_geotiff(month, out_bs, nodata=-9999.0, dtype="float32", compress="deflate")

        mask_slices.append(mask_da.expand_dims(time=[month["time"].values]))
        valid_slices.append(valid_da.expand_dims(time=[month["time"].values]))
        threshold_slices.append(thr_da.expand_dims(time=[month["time"].values]))

        centroid_lon = ""
        centroid_lat = ""
        shift_km = ""
        if wet_count >= args.min_water_pixels:
            yy, xx = np.meshgrid(mask_da["y"].values, mask_da["x"].values, indexing="ij")
            centroid_lon_val = float(np.mean(xx[wet]))
            centroid_lat_val = float(np.mean(yy[wet]))
            centroid_lon = f"{centroid_lon_val:.8f}"
            centroid_lat = f"{centroid_lat_val:.8f}"
            if prev_lon is not None and prev_lat is not None:
                shift_km = f"{_haversine_km(prev_lon, prev_lat, centroid_lon_val, centroid_lat_val):.3f}"
            prev_lon, prev_lat = centroid_lon_val, centroid_lat_val

        summary_rows.append(
            {
                "month": date_str[:7],
                "date": date_str,
                "threshold": f"{threshold:.6f}" if np.isfinite(threshold) else "",
                "wet_pixels": wet_count,
                "valid_pixels": valid_count,
                "wet_fraction": f"{wet_fraction:.6f}" if np.isfinite(wet_fraction) else "",
                "centroid_lon": centroid_lon,
                "centroid_lat": centroid_lat,
                "centroid_shift_km": shift_km,
                "mask_path": str(out_mask.resolve()),
            }
        )
        frac_text = f"{wet_fraction:.3%}" if np.isfinite(wet_fraction) else "n/a"
        print(
            f"[{date_str}] threshold={threshold:.4f} wet={wet_count}/{valid_count} "
            f"({frac_text})"
        )

    mask_stack = xr.concat(mask_slices, dim="time")
    valid_stack = xr.concat(valid_slices, dim="time")
    thr_stack = xr.concat(threshold_slices, dim="time")

    wet_count_da = mask_stack.sum("time").astype("int16")
    valid_count_da = valid_stack.sum("time").astype("int16")
    freq_months = wet_count_da.where(valid_count_da > 0, other=-1).rename("water_frequency_months")
    freq_fraction = (
        wet_count_da.astype("float32") / valid_count_da.where(valid_count_da > 0)
    ).rename("water_frequency_fraction")
    threshold_median = thr_stack.median("time").astype("float32").rename("water_threshold_median")
    permanent_mask = (
        (freq_months >= int(args.permanent_min_months)) & (freq_months >= 0)
    ).astype("uint8").rename("permanent_water_mask")

    export_geotiff(
        freq_months,
        derived_dir / "water_frequency_months.tif",
        nodata=-1,
        dtype="int16",
        compress="deflate",
    )
    export_geotiff(
        freq_fraction,
        derived_dir / "water_frequency_fraction.tif",
        nodata=-9999.0,
        dtype="float32",
        compress="deflate",
    )
    export_geotiff(
        threshold_median,
        derived_dir / "water_threshold_median.tif",
        nodata=-9999.0,
        dtype="float32",
        compress="deflate",
    )
    export_geotiff(
        permanent_mask,
        derived_dir / "permanent_water_mask.tif",
        nodata=255,
        dtype="uint8",
        compress="deflate",
    )

    if args.write_overflow_masks:
        overflow_stack = ((mask_stack == 1) & (permanent_mask == 0)).astype("uint8").rename(
            "overflow_mask"
        )
        for i in range(overflow_stack.sizes["time"]):
            month_ov = overflow_stack.isel(time=i).squeeze()
            date_str = _month_label(month_ov["time"].values)[:10]
            out_overflow = overflow_dir / f"overflow_mask_{date_str}.tif"
            export_geotiff(
                month_ov,
                out_overflow,
                nodata=255,
                dtype="uint8",
                compress="deflate",
            )

            ov_values = np.asarray(month_ov.values, dtype="uint8")
            ov_count = int(np.count_nonzero(ov_values == 1))
            valid_count = int(summary_rows[i]["valid_pixels"])
            summary_rows[i]["overflow_pixels"] = ov_count
            summary_rows[i]["overflow_fraction"] = (
                f"{(ov_count / valid_count):.6f}" if valid_count > 0 else ""
            )
            summary_rows[i]["overflow_path"] = str(out_overflow.resolve())
    else:
        for row in summary_rows:
            row["overflow_pixels"] = ""
            row["overflow_fraction"] = ""
            row["overflow_path"] = ""

    for row in summary_rows:
        row["permanent_min_months"] = int(args.permanent_min_months)
    _write_csv(summary_rows, derived_dir / "water_monthly_summary.csv")

    print("Done.")
    print(f"Masks:   {masks_dir}")
    if args.write_overflow_masks:
        print(f"Overflow:{overflow_dir}")
    if args.write_backscatter:
        print(f"Backsc.: {bs_dir}")
    print(f"Derived: {derived_dir}")
    print(f"Summary: {derived_dir / 'water_monthly_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

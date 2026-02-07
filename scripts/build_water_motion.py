#!/usr/bin/env python3
"""
MVP water-motion builder (date-referenced timeline, fused masks, and change layers).

Input:
- NetCDF time series with at least one variable (default: backscatter)

Outputs:
- 02_daily_masks/YYYY-MM-DD/{water_s1.tif, water_fused.tif, confidence.tif}
- 03_changes/YYYY-MM-DD/{change_vs_prev.tif, gain_intensity.tif, loss_intensity.tif}
- 05_derived/{permanent_water.tif, ephemeral_frequency.tif}
- 06_qgis/timelapse_manifest.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
import rioxarray  # noqa: F401

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


def _infer_default_out_dir() -> Path:
    return Path("output/flood_motion/mvp")


def _date_label(ts: Any) -> str:
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


def _majority_filter(mask: np.ndarray, min_neighbors: int, iterations: int) -> np.ndarray:
    if min_neighbors <= 0:
        return mask
    if min_neighbors > 9:
        raise ValueError("min_neighbors must be in [1,9] for 3x3 neighborhood.")
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


def _pick_threshold(values: np.ndarray, method: str, fixed: float, q: float) -> float:
    finite = np.asarray(values, dtype="float64")
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return float("nan")
    if method == "fixed":
        return float(fixed)
    if method == "quantile":
        return float(np.nanquantile(finite, q))
    if method == "otsu":
        return _otsu_threshold(finite)
    raise ValueError(f"Unsupported threshold method: {method}")


def _subset_dates(da: xr.DataArray, start: str, end: str) -> xr.DataArray:
    out = da.sortby("time")
    if start:
        out = out.sel(time=out["time"] >= np.datetime64(start))
    if end:
        out = out.sel(time=out["time"] < np.datetime64(end))
    return out


def _scan_s2_date_keys(s2_dir: Path) -> set[str]:
    if not s2_dir.exists():
        return set()
    keys: set[str] = set()
    for p in s2_dir.glob("*.tif"):
        stem = p.stem
        # Accept common forms: *_YYYY-MM-DD or *_YYYY-MM
        tail = stem.split("_")[-1]
        if len(tail) == 10 and tail[4] == "-" and tail[7] == "-":
            keys.add(tail)
        elif len(tail) == 7 and tail[4] == "-":
            # Represent month-level data as first day for coarse matching.
            keys.add(f"{tail}-01")
    return keys


def _write_manifest(rows: list[dict[str, Any]], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "has_s1",
        "has_s2",
        "s1_path",
        "s2_path",
        "fused_path",
        "confidence_path",
        "change_path",
        "gain_path",
        "loss_path",
        "threshold",
        "wet_pixels",
        "valid_pixels",
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build date-referenced water motion MVP products.")
    p.add_argument("--input", type=Path, default=_infer_default_input())
    p.add_argument("--var", default="backscatter")
    p.add_argument("--start", default="", help="Inclusive date (YYYY-MM-DD).")
    p.add_argument("--end", default="", help="Exclusive date (YYYY-MM-DD).")
    p.add_argument("--threshold-method", choices=["otsu", "fixed", "quantile"], default="quantile")
    p.add_argument("--fixed-threshold", type=float, default=-16.0)
    p.add_argument("--quantile", type=float, default=0.12)
    p.add_argument("--global-threshold", action="store_true")
    p.add_argument("--min-neighbors", type=int, default=0)
    p.add_argument("--neighbor-iters", type=int, default=1)
    p.add_argument("--permanent-min-fraction", type=float, default=0.8)
    p.add_argument("--s2-dir", type=Path, default=Path("output/sentinel2_truecolor_best_10km_2025"))
    p.add_argument("--out-dir", type=Path, default=_infer_default_out_dir())
    p.add_argument("--crs", default="EPSG:4326")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"Input not found: {args.input}")
    if not (0.0 < args.quantile < 1.0):
        raise SystemExit("--quantile must be in (0,1).")
    if not (0.0 <= args.permanent_min_fraction <= 1.0):
        raise SystemExit("--permanent-min-fraction must be in [0,1].")

    out_root = args.out_dir
    masks_root = out_root / "02_daily_masks"
    change_root = out_root / "03_changes"
    derived_root = out_root / "05_derived"
    qgis_root = out_root / "06_qgis"
    for d in (masks_root, change_root, derived_root, qgis_root):
        d.mkdir(parents=True, exist_ok=True)

    ds = xr.open_dataset(args.input)
    da = ensure_dataarray(ds, var=args.var)
    if "time" not in da.dims:
        raise SystemExit(f"Variable '{args.var}' has no time dimension.")
    da = _subset_dates(da, args.start, args.end)
    if da.sizes.get("time", 0) == 0:
        raise SystemExit("No time slices in selected range.")

    s2_keys = _scan_s2_date_keys(args.s2_dir)
    global_thr: float | None = None
    if args.global_threshold:
        global_thr = _pick_threshold(np.asarray(da.values), args.threshold_method, args.fixed_threshold, args.quantile)

    prev_fused: np.ndarray | None = None
    mask_stack: list[xr.DataArray] = []
    valid_stack: list[xr.DataArray] = []
    manifest_rows: list[dict[str, Any]] = []

    for i in range(da.sizes["time"]):
        sl = da.isel(time=i).squeeze()
        sl = normalize_spatial_da(sl, crs=args.crs).transpose("y", "x")
        values = np.asarray(sl.values, dtype="float64")
        valid = np.isfinite(values)
        thr = float(global_thr) if global_thr is not None else _pick_threshold(values, args.threshold_method, args.fixed_threshold, args.quantile)
        wet_s1 = valid & (values <= thr)
        if args.min_neighbors > 0:
            wet_s1 = _majority_filter(wet_s1, args.min_neighbors, args.neighbor_iters)

        date_str = _date_label(sl["time"].values)[:10]
        frame_dir = masks_root / date_str
        frame_dir.mkdir(parents=True, exist_ok=True)

        # MVP fusion: S1-only mask while preserving S2 availability in manifest/confidence.
        has_s2 = date_str in s2_keys or f"{date_str[:7]}-01" in s2_keys
        conf_val = 0.85 if has_s2 else 0.70
        fused = wet_s1.astype("uint8")
        conf = np.where(valid, conf_val, np.nan).astype("float32")

        s1_da = xr.DataArray(fused, dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="water_s1")
        fused_da = xr.DataArray(fused, dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="water_fused")
        conf_da = xr.DataArray(conf, dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="confidence")
        valid_da = xr.DataArray(valid.astype("uint8"), dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="valid_mask")

        s1_da = normalize_spatial_da(s1_da, crs=args.crs).transpose("y", "x")
        fused_da = normalize_spatial_da(fused_da, crs=args.crs).transpose("y", "x")
        conf_da = normalize_spatial_da(conf_da, crs=args.crs).transpose("y", "x")
        valid_da = normalize_spatial_da(valid_da, crs=args.crs).transpose("y", "x")

        s1_path = frame_dir / "water_s1.tif"
        fused_path = frame_dir / "water_fused.tif"
        conf_path = frame_dir / "confidence.tif"
        export_geotiff(s1_da, s1_path, nodata=255, dtype="uint8", compress="deflate")
        export_geotiff(fused_da, fused_path, nodata=255, dtype="uint8", compress="deflate")
        export_geotiff(conf_da, conf_path, nodata=-9999.0, dtype="float32", compress="deflate")

        change_path = ""
        gain_path = ""
        loss_path = ""
        if prev_fused is not None:
            cdir = change_root / date_str
            cdir.mkdir(parents=True, exist_ok=True)
            gain = ((prev_fused == 0) & (fused == 1)).astype("uint8")
            loss = ((prev_fused == 1) & (fused == 0)).astype("uint8")
            change = np.where(gain == 1, 1, np.where(loss == 1, -1, 0)).astype("int8")

            change_da = xr.DataArray(change, dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="change_vs_prev")
            gain_da = xr.DataArray(gain.astype("float32"), dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="gain_intensity")
            loss_da = xr.DataArray(loss.astype("float32"), dims=("y", "x"), coords={"y": sl["y"].values, "x": sl["x"].values}, name="loss_intensity")
            change_da = normalize_spatial_da(change_da, crs=args.crs).transpose("y", "x")
            gain_da = normalize_spatial_da(gain_da, crs=args.crs).transpose("y", "x")
            loss_da = normalize_spatial_da(loss_da, crs=args.crs).transpose("y", "x")

            change_path_p = cdir / "change_vs_prev.tif"
            gain_path_p = cdir / "gain_intensity.tif"
            loss_path_p = cdir / "loss_intensity.tif"
            export_geotiff(change_da, change_path_p, nodata=-128, dtype="int8", compress="deflate")
            export_geotiff(gain_da, gain_path_p, nodata=-9999.0, dtype="float32", compress="deflate")
            export_geotiff(loss_da, loss_path_p, nodata=-9999.0, dtype="float32", compress="deflate")
            change_path = str(change_path_p.resolve())
            gain_path = str(gain_path_p.resolve())
            loss_path = str(loss_path_p.resolve())

        prev_fused = fused
        mask_stack.append(fused_da.expand_dims(time=[sl["time"].values]))
        valid_stack.append(valid_da.expand_dims(time=[sl["time"].values]))

        manifest_rows.append(
            {
                "date": date_str,
                "has_s1": "1",
                "has_s2": "1" if has_s2 else "0",
                "s1_path": str(s1_path.resolve()),
                "s2_path": "",
                "fused_path": str(fused_path.resolve()),
                "confidence_path": str(conf_path.resolve()),
                "change_path": change_path,
                "gain_path": gain_path,
                "loss_path": loss_path,
                "threshold": f"{thr:.6f}" if np.isfinite(thr) else "",
                "wet_pixels": str(int(np.count_nonzero(fused == 1))),
                "valid_pixels": str(int(np.count_nonzero(valid))),
            }
        )
        print(f"[{date_str}] wet={int(np.count_nonzero(fused == 1))} valid={int(np.count_nonzero(valid))} has_s2={has_s2}")

    mask_cube = xr.concat(mask_stack, dim="time")
    valid_cube = xr.concat(valid_stack, dim="time")
    freq = (
        mask_cube.sum("time").astype("float32") / valid_cube.sum("time").where(valid_cube.sum("time") > 0)
    ).rename("water_frequency_fraction")
    perm = (freq >= float(args.permanent_min_fraction)).astype("uint8").rename("permanent_water")

    export_geotiff(freq, derived_root / "ephemeral_frequency.tif", nodata=-9999.0, dtype="float32", compress="deflate")
    export_geotiff(perm, derived_root / "permanent_water.tif", nodata=255, dtype="uint8", compress="deflate")

    manifest_path = qgis_root / "timelapse_manifest.csv"
    _write_manifest(manifest_rows, manifest_path)
    print("Done.")
    print(f"Output: {out_root}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


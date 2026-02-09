#!/usr/bin/env python3
"""
Download additional Earth Engine datasets for a local AOI:
- GOOGLE/DYNAMICWORLD/V1 (monthly composites)
- COPERNICUS/S3/OLCI (monthly composites)
- COPERNICUS/S2_SR_HARMONIZED (monthly composites, cloud-masked)

Outputs are GeoTIFF files organized by dataset and month.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import sys
from typing import Iterable

import ee
import numpy as np
import xarray as xr
import xee  # noqa: F401
import rasterio
from rasterio.windows import Window

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.export_geotiff import export_geotiff


M_PER_DEGREE = 111320.0
DEFAULT_LAT = -13.700278
DEFAULT_LON = -63.927778
DEFAULT_BUFFER_KM = 10.0
MIN_VALID_TIF_BYTES = 1024


DATASET_AVAILABILITY = {
    # Reference: Earth Engine catalog availability windows.
    "dynamicworld": ("2015-06-27", None),
    "s3olci": ("2016-10-18", None),
    "sentinel2": ("2017-03-28", None),
}


@dataclass(frozen=True)
class MonthRange:
    label: str
    start: str
    end: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download Dynamic World, S3 OLCI, and Sentinel-2 SR Harmonized monthly composites."
    )
    p.add_argument("--project-id", default="gen-lang-client-0296388721")
    p.add_argument("--lat", type=float, default=DEFAULT_LAT)
    p.add_argument("--lon", type=float, default=DEFAULT_LON)
    p.add_argument("--buffer-km", type=float, default=DEFAULT_BUFFER_KM)
    p.add_argument("--start", default="2025-01-01", help="Inclusive start date (YYYY-MM-DD).")
    p.add_argument("--end", default="2026-01-01", help="Exclusive end date (YYYY-MM-DD).")
    p.add_argument(
        "--datasets",
        default="dynamicworld,s3olci,sentinel2",
        help="Comma list: dynamicworld,s3olci,sentinel2",
    )
    p.add_argument(
        "--s3-bands",
        default="Oa06_radiance,Oa08_radiance,Oa17_radiance,Oa21_radiance",
        help="Comma list of OLCI bands to export.",
    )
    p.add_argument(
        "--s2-bands",
        default="B2,B3,B4,B8,B11",
        help="Comma list of Sentinel-2 SR bands to export.",
    )
    p.add_argument(
        "--s2-cloudy-max",
        type=float,
        default=80.0,
        help="Max CLOUDY_PIXEL_PERCENTAGE for Sentinel-2 monthly input images.",
    )
    p.add_argument("--out-dir", default="output/flood/additional_10km_2025")
    p.add_argument("--skip-existing", action="store_true")
    return p.parse_args()


def _km_to_deg_lat(km: float) -> float:
    return km / 110.574


def _km_to_deg_lon(km: float, lat: float) -> float:
    return km / (111.320 * math.cos(math.radians(lat)))


def make_aoi(lat: float, lon: float, buffer_km: float) -> ee.Geometry:
    return ee.Geometry.Point(lon, lat).buffer(buffer_km * 1000).bounds()


def month_ranges(start: str, end: str) -> list[MonthRange]:
    s = datetime.fromisoformat(start).date().replace(day=1)
    e = datetime.fromisoformat(end).date().replace(day=1)
    if e <= s:
        raise ValueError("--end must be after --start")

    out: list[MonthRange] = []
    y, m = s.year, s.month
    while date(y, m, 1) < e:
        next_y = y + (m // 12)
        next_m = 1 if m == 12 else m + 1
        start_d = date(y, m, 1)
        end_d = date(next_y, next_m, 1)
        out.append(MonthRange(label=f"{y:04d}-{m:02d}", start=start_d.isoformat(), end=end_d.isoformat()))
        y, m = next_y, next_m
    return out


def ee_init(project_id: str) -> None:
    ee.Initialize(project=project_id)


def open_xee_dataset(
    img_or_ic,
    geometry: ee.Geometry,
    crs: str | None = None,
    projection: ee.Projection | None = None,
    scale: float | None = None,
) -> xr.Dataset:
    kwargs: dict[str, object] = {"engine": "ee", "geometry": geometry}
    if crs is not None:
        kwargs["crs"] = crs
    if projection is not None:
        kwargs["projection"] = projection
    if scale is not None:
        crs_value = None
        if crs is not None:
            crs_value = crs
        elif projection is not None:
            try:
                crs_value = projection.crs().getInfo()
            except Exception:
                crs_value = None
        # When using EPSG:4326, xee expects scale in degrees.
        if isinstance(crs_value, str) and crs_value.upper() == "EPSG:4326" and scale > 1:
            scale = scale / M_PER_DEGREE
        kwargs["scale"] = scale
    return xr.open_dataset(img_or_ic, **kwargs)


def _first_existing(items: Iterable[str], valid_set: set[str]) -> list[str]:
    return [x for x in items if x in valid_set]


def _is_valid_tif(path: Path) -> bool:
    if not path.exists():
        return False
    if path.stat().st_size < MIN_VALID_TIF_BYTES:
        return False
    try:
        with rasterio.open(path) as ds:
            if ds.count < 1:
                return False
            h = min(1, ds.height)
            w = min(1, ds.width)
            ds.read(1, window=Window(0, 0, w, h))
        return True
    except Exception:
        return False


def _drop_if_invalid(path: Path) -> None:
    if path.exists() and not _is_valid_tif(path):
        path.unlink(missing_ok=True)


def _warn_temporal_coverage(requested: set[str], start: str, end: str) -> None:
    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()
    print("Dataset temporal coverage check")
    for ds in sorted(requested):
        win = DATASET_AVAILABILITY.get(ds)
        if not win:
            continue
        d0 = datetime.fromisoformat(win[0]).date()
        d1 = datetime.fromisoformat(win[1]).date() if win[1] else None
        if d1 is None:
            overlap = end_d > d0
            cover_text = f"{d0.isoformat()} -> present"
        else:
            overlap = (end_d > d0) and (start_d <= d1)
            cover_text = f"{d0.isoformat()} -> {d1.isoformat()}"
        print(f" - {ds}: {cover_text}")
        if not overlap:
            print(
                f"   WARNING: requested window {start_d.isoformat()} -> {end_d.isoformat()} "
                "is outside availability."
            )


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        res = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        return str(pid) in (res.stdout or "")
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists():
        raw = lock_path.read_text(encoding="utf-8").strip()
        prev_pid = 0
        if raw:
            try:
                prev_pid = int(raw.split(",", 1)[0].strip())
            except ValueError:
                prev_pid = 0
        if prev_pid > 0 and _pid_alive(prev_pid):
            raise RuntimeError(
                f"Another download_additional_datasets run is active for this out-dir "
                f"(pid={prev_pid}). Lock file: {lock_path}"
            )
        # stale lock
        lock_path.unlink(missing_ok=True)

    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    pid = os.getpid()
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(f"{pid},{datetime.now(timezone.utc).isoformat()}\n")
    return pid


def release_lock(lock_path: Path, owner_pid: int) -> None:
    if owner_pid <= 0 or not lock_path.exists():
        return
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
        pid = int(raw.split(",", 1)[0].strip()) if raw else 0
    except Exception:
        pid = 0
    if pid == owner_pid:
        lock_path.unlink(missing_ok=True)


def export_dynamicworld(
    aoi: ee.Geometry,
    ranges: list[MonthRange],
    out_dir: Path,
    skip_existing: bool,
    summary_rows: list[dict[str, object]],
) -> None:
    dataset_dir = out_dir / "dynamicworld"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    for m in ranges:
        out_water = dataset_dir / f"dw_water_prob_{m.label}.tif"
        out_fveg = dataset_dir / f"dw_flooded_veg_prob_{m.label}.tif"
        out_label = dataset_dir / f"dw_label_mode_{m.label}.tif"
        expected = [out_water, out_fveg, out_label]
        if skip_existing and all(_is_valid_tif(p) for p in expected):
            print(f"[DynamicWorld {m.label}] skipped (all outputs exist)")
            summary_rows.append(
                {"dataset": "dynamicworld", "month": m.label, "image_count": "", "status": "skipped", "detail": ""}
            )
            continue

        for p in expected:
            _drop_if_invalid(p)

        try:
            ic = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1").filterBounds(aoi).filterDate(m.start, m.end)
            count = int(ic.size().getInfo())
            if count <= 0:
                print(f"[DynamicWorld {m.label}] no images")
                summary_rows.append(
                    {
                        "dataset": "dynamicworld",
                        "month": m.label,
                        "image_count": 0,
                        "status": "no_data",
                        "detail": "",
                    }
                )
                continue

            water = ic.select("water").mean().rename("dw_water_prob")
            flooded_veg = ic.select("flooded_vegetation").mean().rename("dw_flooded_veg_prob")
            label_mode = ic.select("label").reduce(ee.Reducer.mode()).rename("dw_label_mode")
            img = water.addBands([flooded_veg, label_mode]).clip(aoi)

            ds = open_xee_dataset(img, geometry=aoi, crs="EPSG:4326", scale=10)

            export_geotiff(ds["dw_water_prob"], out_water, dtype="float32", nodata=-9999.0)
            export_geotiff(ds["dw_flooded_veg_prob"], out_fveg, dtype="float32", nodata=-9999.0)
            export_geotiff(ds["dw_label_mode"], out_label, dtype="uint8", nodata=255)

            print(f"[DynamicWorld {m.label}] images={count} -> {out_water.name}, {out_fveg.name}, {out_label.name}")
            summary_rows.append(
                {"dataset": "dynamicworld", "month": m.label, "image_count": count, "status": "ok", "detail": ""}
            )
        except Exception as exc:
            print(f"[DynamicWorld {m.label}] ERROR: {exc}")
            summary_rows.append(
                {
                    "dataset": "dynamicworld",
                    "month": m.label,
                    "image_count": "",
                    "status": "error",
                    "detail": str(exc),
                }
            )
            continue


def export_s3_olci(
    aoi: ee.Geometry,
    ranges: list[MonthRange],
    bands: list[str],
    out_dir: Path,
    skip_existing: bool,
    summary_rows: list[dict[str, object]],
) -> None:
    dataset_dir = out_dir / "s3_olci"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Validate requested bands against first image.
    base_ic = ee.ImageCollection("COPERNICUS/S3/OLCI").filterBounds(aoi)
    base_count = int(base_ic.size().getInfo())
    if base_count <= 0:
        raise RuntimeError("S3 OLCI has no images for this AOI.")
    available = set(ee.Image(base_ic.first()).bandNames().getInfo())
    selected = _first_existing(bands, available)
    if not selected:
        raise ValueError(f"None of requested S3 bands exist. Requested={bands}")
    print(f"S3 bands selected: {selected}")

    for m in ranges:
        band_paths = [dataset_dir / f"s3_{b}_{m.label}.tif" for b in selected]
        ndwi_path = dataset_dir / f"s3_ndwi_{m.label}.tif"
        expected = [*band_paths, ndwi_path]
        if skip_existing and all(_is_valid_tif(p) for p in expected):
            print(f"[S3 OLCI {m.label}] skipped (all outputs exist)")
            summary_rows.append(
                {"dataset": "s3_olci", "month": m.label, "image_count": "", "status": "skipped", "detail": ""}
            )
            continue

        for p in expected:
            _drop_if_invalid(p)

        try:
            ic = ee.ImageCollection("COPERNICUS/S3/OLCI").filterBounds(aoi).filterDate(m.start, m.end)
            count = int(ic.size().getInfo())
            if count <= 0:
                print(f"[S3 OLCI {m.label}] no images")
                summary_rows.append(
                    {"dataset": "s3_olci", "month": m.label, "image_count": 0, "status": "no_data", "detail": ""}
                )
                continue

            med = ic.select(selected).median()
            if "Oa06_radiance" in selected and "Oa17_radiance" in selected:
                ndwi = med.expression(
                    "(g - n) / (g + n)",
                    {"g": med.select("Oa06_radiance"), "n": med.select("Oa17_radiance")},
                ).rename("s3_ndwi")
                img = med.addBands(ndwi).clip(aoi)
            else:
                img = med.clip(aoi)

            ds = open_xee_dataset(img, geometry=aoi, crs="EPSG:4326", scale=300)

            for b in selected:
                export_geotiff(ds[b], dataset_dir / f"s3_{b}_{m.label}.tif", dtype="float32", nodata=-9999.0)
            if "s3_ndwi" in ds.data_vars:
                export_geotiff(ds["s3_ndwi"], ndwi_path, dtype="float32", nodata=-9999.0)

            print(f"[S3 OLCI {m.label}] images={count} -> {len(selected)} bands + ndwi")
            summary_rows.append(
                {"dataset": "s3_olci", "month": m.label, "image_count": count, "status": "ok", "detail": ""}
            )
        except Exception as exc:
            print(f"[S3 OLCI {m.label}] ERROR: {exc}")
            summary_rows.append(
                {"dataset": "s3_olci", "month": m.label, "image_count": "", "status": "error", "detail": str(exc)}
            )
            continue


def _mask_s2_sr_clouds(image: ee.Image) -> ee.Image:
    # Remove clouds, cloud shadows, cirrus, snow/ice, no-data and saturated pixels.
    scl = image.select("SCL")
    invalid = (
        scl.eq(0)
        .Or(scl.eq(1))
        .Or(scl.eq(3))
        .Or(scl.eq(8))
        .Or(scl.eq(9))
        .Or(scl.eq(10))
        .Or(scl.eq(11))
    )
    return image.updateMask(invalid.Not())


def export_sentinel2(
    aoi: ee.Geometry,
    ranges: list[MonthRange],
    bands: list[str],
    cloudy_max: float,
    out_dir: Path,
    skip_existing: bool,
    summary_rows: list[dict[str, object]],
) -> None:
    dataset_dir = out_dir / "sentinel2_sr_harmonized"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    base_ic = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterBounds(aoi)
    base_count = int(base_ic.size().getInfo())
    if base_count <= 0:
        raise RuntimeError("Sentinel-2 SR Harmonized has no images for this AOI.")
    available = set(ee.Image(base_ic.first()).bandNames().getInfo())
    selected = _first_existing(bands, available)
    if not selected:
        raise ValueError(f"None of requested Sentinel-2 bands exist. Requested={bands}")
    print(f"S2 bands selected: {selected}")

    for m in ranges:
        band_paths = [dataset_dir / f"s2_{b}_{m.label}.tif" for b in selected]
        index_paths: list[Path] = []
        if "B3" in selected and "B8" in selected:
            index_paths.append(dataset_dir / f"s2_ndwi_{m.label}.tif")
        if "B3" in selected and "B11" in selected:
            index_paths.append(dataset_dir / f"s2_mndwi_{m.label}.tif")
        expected = [*band_paths, *index_paths]

        if skip_existing and all(_is_valid_tif(p) for p in expected):
            print(f"[Sentinel-2 {m.label}] skipped (all outputs exist)")
            summary_rows.append(
                {"dataset": "sentinel2", "month": m.label, "image_count": "", "status": "skipped", "detail": ""}
            )
            continue

        for p in expected:
            _drop_if_invalid(p)

        try:
            ic = (
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(aoi)
                .filterDate(m.start, m.end)
                .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", cloudy_max))
                .map(_mask_s2_sr_clouds)
            )
            count = int(ic.size().getInfo())
            if count <= 0:
                print(f"[Sentinel-2 {m.label}] no images")
                summary_rows.append(
                    {"dataset": "sentinel2", "month": m.label, "image_count": 0, "status": "no_data", "detail": ""}
                )
                continue

            med = ic.select(selected).median()
            img = med
            if "B3" in selected and "B8" in selected:
                ndwi = med.expression("(g - n) / (g + n)", {"g": med.select("B3"), "n": med.select("B8")}).rename(
                    "s2_ndwi"
                )
                img = img.addBands(ndwi)
            if "B3" in selected and "B11" in selected:
                mndwi = med.expression("(g - s) / (g + s)", {"g": med.select("B3"), "s": med.select("B11")}).rename(
                    "s2_mndwi"
                )
                img = img.addBands(mndwi)
            img = img.clip(aoi)

            ds = open_xee_dataset(img, geometry=aoi, crs="EPSG:4326", scale=10)

            for b in selected:
                export_geotiff(ds[b], dataset_dir / f"s2_{b}_{m.label}.tif", dtype="uint16", nodata=65535)
            if "s2_ndwi" in ds.data_vars:
                export_geotiff(ds["s2_ndwi"], dataset_dir / f"s2_ndwi_{m.label}.tif", dtype="float32", nodata=-9999.0)
            if "s2_mndwi" in ds.data_vars:
                export_geotiff(
                    ds["s2_mndwi"], dataset_dir / f"s2_mndwi_{m.label}.tif", dtype="float32", nodata=-9999.0
                )

            print(f"[Sentinel-2 {m.label}] images={count} -> {len(expected)} outputs")
            summary_rows.append(
                {"dataset": "sentinel2", "month": m.label, "image_count": count, "status": "ok", "detail": ""}
            )
        except Exception as exc:
            print(f"[Sentinel-2 {m.label}] ERROR: {exc}")
            summary_rows.append(
                {"dataset": "sentinel2", "month": m.label, "image_count": "", "status": "error", "detail": str(exc)}
            )
            continue


def write_summary(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "month", "image_count", "status", "detail"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    lock_path = out_dir / ".download_additional_datasets.lock"
    lock_owner_pid = acquire_lock(lock_path)

    try:
        alias = {
            "s2": "sentinel2",
            "s2sr": "sentinel2",
            "sentinel-2": "sentinel2",
            "dynamic-world": "dynamicworld",
            "s3": "s3olci",
        }
        requested_raw = {x.strip().lower() for x in args.datasets.split(",") if x.strip()}
        requested = {alias.get(x, x) for x in requested_raw}
        valid = {"dynamicworld", "s3olci", "sentinel2"}
        unknown = requested - valid
        if unknown:
            raise ValueError(f"Unknown dataset(s): {sorted(unknown)}. Valid: {sorted(valid)}")

        ranges = month_ranges(args.start, args.end)
        ee_init(args.project_id)
        aoi = make_aoi(args.lat, args.lon, args.buffer_km)

        print("Download configuration")
        print(f" AOI center: ({args.lat}, {args.lon})")
        print(f" Buffer km:  {args.buffer_km}")
        print(f" Range:      {ranges[0].label} -> {ranges[-1].label} ({len(ranges)} months)")
        print(f" Datasets:   {sorted(requested)}")
        print(f" Out dir:    {out_dir}")
        print(f" Lock file:  {lock_path}")
        _warn_temporal_coverage(requested, args.start, args.end)

        rows: list[dict[str, object]] = []
        if "dynamicworld" in requested:
            export_dynamicworld(
                aoi=aoi,
                ranges=ranges,
                out_dir=out_dir,
                skip_existing=bool(args.skip_existing),
                summary_rows=rows,
            )
        if "s3olci" in requested:
            bands = [x.strip() for x in args.s3_bands.split(",") if x.strip()]
            export_s3_olci(
                aoi=aoi,
                ranges=ranges,
                bands=bands,
                out_dir=out_dir,
                skip_existing=bool(args.skip_existing),
                summary_rows=rows,
            )
        if "sentinel2" in requested:
            bands = [x.strip() for x in args.s2_bands.split(",") if x.strip()]
            export_sentinel2(
                aoi=aoi,
                ranges=ranges,
                bands=bands,
                cloudy_max=float(args.s2_cloudy_max),
                out_dir=out_dir,
                skip_existing=bool(args.skip_existing),
                summary_rows=rows,
            )

        summary_path = out_dir / "download_summary.csv"
        write_summary(summary_path, rows)
        print(f"Summary: {summary_path}")
        return 0
    finally:
        release_lock(lock_path, lock_owner_pid)


if __name__ == "__main__":
    raise SystemExit(main())

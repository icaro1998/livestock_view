#!/usr/bin/env python3
"""
Download Sentinel-2 SR Harmonized true-color snapshots.

Each GeoTIFF is a 3-band RGB raster:
- Band 1: B4 (red)
- Band 2: B3 (green)
- Band 3: B2 (blue)
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import sys

import ee
import xarray as xr
import xee  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.export_geotiff import export_geotiff


M_PER_DEGREE = 111320.0


@dataclass(frozen=True)
class TimeRange:
    label: str
    start: str
    end: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download Sentinel-2 SR Harmonized true-color snapshots.")
    p.add_argument("--project-id", default="gen-lang-client-0296388721")
    p.add_argument("--lat", type=float, default=-13.700278)
    p.add_argument("--lon", type=float, default=-63.927778)
    p.add_argument("--buffer-km", type=float, default=10.0)
    p.add_argument("--start", default="2025-01-01", help="Inclusive start date (YYYY-MM-DD).")
    p.add_argument("--end", default="2026-01-01", help="Exclusive end date (YYYY-MM-DD).")
    p.add_argument(
        "--mode",
        choices=["monthly", "daily"],
        default="monthly",
        help="monthly: one best scene per month; daily: one best scene for --date.",
    )
    p.add_argument(
        "--date",
        default="",
        help="Required in --mode daily. Date format: YYYY-MM-DD",
    )
    p.add_argument(
        "--daily-window-days",
        type=int,
        default=0,
        help="In --mode daily, include +/- N extra days around --date for gap-filling mosaics.",
    )
    p.add_argument(
        "--strategy",
        choices=["best_scene", "mosaic"],
        default="mosaic",
        help="best_scene: single least-cloudy scene. mosaic: merge multiple scenes to improve coverage.",
    )
    p.add_argument(
        "--max-cloud",
        type=float,
        default=40.0,
        help="Maximum CLOUDY_PIXEL_PERCENTAGE preferred for image selection.",
    )
    p.add_argument(
        "--cloud-mask-source",
        choices=["none", "s2cloudprob", "cloudscoreplus"],
        default="none",
        help="Optional pixel-level cloud mask source.",
    )
    p.add_argument(
        "--s2cloudprob-threshold",
        type=float,
        default=40.0,
        help="For --cloud-mask-source s2cloudprob: keep pixels with probability <= threshold (0..100).",
    )
    p.add_argument(
        "--cloudscore-threshold",
        type=float,
        default=0.60,
        help="For --cloud-mask-source cloudscoreplus: keep pixels with cs >= threshold (0..1).",
    )
    p.add_argument(
        "--out-dir",
        default="output/sentinel2_truecolor_best_10km_2025",
        help="Output directory for monthly true-color snapshots.",
    )
    p.add_argument("--skip-existing", action="store_true")
    return p.parse_args()


def month_ranges(start: str, end: str) -> list[TimeRange]:
    s = datetime.fromisoformat(start).date().replace(day=1)
    e = datetime.fromisoformat(end).date().replace(day=1)
    if e <= s:
        raise ValueError("--end must be after --start")

    out: list[TimeRange] = []
    y, m = s.year, s.month
    while date(y, m, 1) < e:
        next_y = y + (m // 12)
        next_m = 1 if m == 12 else m + 1
        start_d = date(y, m, 1)
        end_d = date(next_y, next_m, 1)
        out.append(TimeRange(label=f"{y:04d}-{m:02d}", start=start_d.isoformat(), end=end_d.isoformat()))
        y, m = next_y, next_m
    return out


def daily_range(day_iso: str) -> list[TimeRange]:
    d = datetime.fromisoformat(day_iso).date()
    d2 = d.fromordinal(d.toordinal() + 1)
    return [TimeRange(label=d.isoformat(), start=d.isoformat(), end=d2.isoformat())]


def daily_range_with_window(day_iso: str, window_days: int) -> list[TimeRange]:
    d = datetime.fromisoformat(day_iso).date()
    if window_days < 0:
        raise ValueError("--daily-window-days must be >= 0")
    start = d.fromordinal(d.toordinal() - window_days)
    end = d.fromordinal(d.toordinal() + window_days + 1)
    return [TimeRange(label=d.isoformat(), start=start.isoformat(), end=end.isoformat())]


def make_aoi(lat: float, lon: float, buffer_km: float) -> ee.Geometry:
    return ee.Geometry.Point(lon, lat).buffer(buffer_km * 1000).bounds()


def open_xee_dataset(
    img: ee.Image,
    geometry: ee.Geometry,
    crs: str = "EPSG:4326",
    scale_m: float = 10.0,
) -> xr.Dataset:
    scale = scale_m
    if crs.upper() == "EPSG:4326":
        scale = scale_m / M_PER_DEGREE
    return xr.open_dataset(img, engine="ee", geometry=geometry, crs=crs, scale=scale)


def _iso_utc_from_millis(value: object) -> str:
    if value is None:
        return ""
    try:
        ms = float(value)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def _pick_best_image(ic: ee.ImageCollection, max_cloud: float) -> tuple[ee.Image | None, int, int, str]:
    total_count = int(ic.size().getInfo())
    if total_count <= 0:
        return None, 0, 0, "no_data"

    preferred = ic.filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", max_cloud))
    preferred_count = int(preferred.size().getInfo())
    if preferred_count > 0:
        chosen = ee.Image(preferred.sort("CLOUDY_PIXEL_PERCENTAGE", True).first())
        return chosen, total_count, preferred_count, "best_under_max_cloud"

    chosen = ee.Image(ic.sort("CLOUDY_PIXEL_PERCENTAGE", True).first())
    return chosen, total_count, preferred_count, "best_available"


def _apply_cloud_mask(
    img: ee.Image,
    source: str,
    s2cloudprob_threshold: float,
    cloudscore_threshold: float,
) -> ee.Image:
    if source == "none":
        return img

    idx = img.get("system:index")
    if source == "s2cloudprob":
        cp_col = ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY").filter(ee.Filter.eq("system:index", idx))
        cp = ee.Image(
            ee.Algorithms.If(
                cp_col.size().gt(0),
                cp_col.first(),
                ee.Image.constant(0).rename("probability"),
            )
        )
        mask = cp.select("probability").lte(float(s2cloudprob_threshold))
        return img.updateMask(mask)

    if source == "cloudscoreplus":
        cs_col = ee.ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED").filter(
            ee.Filter.eq("system:index", idx)
        )
        cs = ee.Image(
            ee.Algorithms.If(
                cs_col.size().gt(0),
                cs_col.first(),
                ee.Image.constant(1).rename("cs"),
            )
        )
        mask = cs.select("cs").gte(float(cloudscore_threshold))
        return img.updateMask(mask)

    return img


def _build_mosaic_image(
    ic: ee.ImageCollection,
    source: str,
    s2cloudprob_threshold: float,
    cloudscore_threshold: float,
    max_cloud: float,
) -> tuple[ee.Image | None, int, int, int, str]:
    total_count = int(ic.size().getInfo())
    if total_count <= 0:
        return None, 0, 0, 0, "no_data"

    preferred = ic.filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", max_cloud))
    preferred_count = int(preferred.size().getInfo())
    work_ic = preferred if preferred_count > 0 else ic
    pool_count = preferred_count if preferred_count > 0 else total_count
    selection_mode = "mosaic_under_max_cloud" if preferred_count > 0 else "mosaic_all_available"

    def _mask_fn(im: ee.Image) -> ee.Image:
        return _apply_cloud_mask(
            ee.Image(im),
            source=source,
            s2cloudprob_threshold=s2cloudprob_threshold,
            cloudscore_threshold=cloudscore_threshold,
        )

    # mosaic() prioritizes later images. We sort descending cloud so cleaner scenes are later.
    masked = work_ic.map(_mask_fn).sort("CLOUDY_PIXEL_PERCENTAGE", False)
    mosaic = ee.Image(masked.mosaic())
    return mosaic, total_count, preferred_count, pool_count, selection_mode


def main() -> int:
    args = parse_args()
    ee.Initialize(project=args.project_id)
    aoi = make_aoi(args.lat, args.lon, args.buffer_km)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / ("s2_truecolor_daily_summary.csv" if args.mode == "daily" else "s2_truecolor_monthly_summary.csv")

    if args.mode == "daily":
        if not args.date:
            raise ValueError("--date is required in --mode daily.")
        ranges = daily_range_with_window(args.date, args.daily_window_days)
    else:
        ranges = month_ranges(args.start, args.end)

    print(f"Sentinel-2 {args.mode} true-color")
    print(f" AOI center: ({args.lat}, {args.lon})")
    print(f" Buffer km:  {args.buffer_km}")
    if args.mode == "daily":
        print(f" Date:       {ranges[0].label}")
        if args.daily_window_days > 0:
            print(f"  - window:  {ranges[0].start} -> {ranges[0].end} (end exclusive)")
    else:
        print(f" Range:      {ranges[0].label} -> {ranges[-1].label} ({len(ranges)} months)")
    print(f" Max cloud:  {args.max_cloud}")
    print(f" Strategy:   {args.strategy}")
    print(f" Cloud mask: {args.cloud_mask_source}")
    if args.cloud_mask_source == "s2cloudprob":
        print(f"  - probability <= {args.s2cloudprob_threshold}")
    if args.cloud_mask_source == "cloudscoreplus":
        print(f"  - cs >= {args.cloudscore_threshold}")
    print(f" Out dir:    {out_dir}")

    rows: list[dict[str, object]] = []
    for m in ranges:
        out_tif = out_dir / f"s2_truecolor_{m.label}.tif"
        if args.skip_existing and out_tif.exists():
            print(f"[{m.label}] skipped (exists)")
            rows.append(
                {
                    "month": m.label,
                    "status": "skipped",
                    "image_count_total": "",
                    "image_count_under_cloud": "",
                    "selected_cloudy_pct": "",
                    "selected_sensing_time_utc": "",
                    "selected_system_index": "",
                    "selected_asset_id": "",
                    "selection_mode": "",
                    "strategy": args.strategy,
                    "images_used": "",
                    "output_tif": str(out_tif),
                }
            )
            continue

        ic = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(aoi)
            .filterDate(m.start, m.end)
            .select(["B4", "B3", "B2"])
        )
        if args.strategy == "best_scene":
            best_img, total_count, under_cloud_count, mode = _pick_best_image(ic, args.max_cloud)
            if best_img is None:
                print(f"[{m.label}] no images")
                rows.append(
                    {
                        "month": m.label,
                        "status": "no_data",
                        "image_count_total": 0,
                        "image_count_under_cloud": 0,
                        "selected_cloudy_pct": "",
                        "selected_sensing_time_utc": "",
                        "selected_system_index": "",
                        "selected_asset_id": "",
                        "selection_mode": "no_data",
                        "strategy": args.strategy,
                        "images_used": 0,
                        "output_tif": "",
                    }
                )
                continue

            props = best_img.toDictionary(
                ["system:index", "system:id", "system:time_start", "CLOUDY_PIXEL_PERCENTAGE"]
            ).getInfo()
            sensing_time = _iso_utc_from_millis(props.get("system:time_start"))
            cloudy_pct = props.get("CLOUDY_PIXEL_PERCENTAGE", "")
            system_index = props.get("system:index", "")
            asset_id = props.get("system:id", "")

            selected = _apply_cloud_mask(
                best_img,
                source=args.cloud_mask_source,
                s2cloudprob_threshold=args.s2cloudprob_threshold,
                cloudscore_threshold=args.cloudscore_threshold,
            )
            images_used = 1
        else:
            selected, total_count, under_cloud_count, images_used, mode = _build_mosaic_image(
                ic=ic,
                source=args.cloud_mask_source,
                s2cloudprob_threshold=args.s2cloudprob_threshold,
                cloudscore_threshold=args.cloudscore_threshold,
                max_cloud=args.max_cloud,
            )
            if selected is None:
                print(f"[{m.label}] no images")
                rows.append(
                    {
                        "month": m.label,
                        "status": "no_data",
                        "image_count_total": 0,
                        "image_count_under_cloud": 0,
                        "selected_cloudy_pct": "",
                        "selected_sensing_time_utc": "",
                        "selected_system_index": "",
                        "selected_asset_id": "",
                        "selection_mode": "no_data",
                        "strategy": args.strategy,
                        "images_used": 0,
                        "output_tif": "",
                    }
                )
                continue

            # For mosaics, metadata is representative from least-cloudy scene in pool.
            ref = ee.Image(ic.sort("CLOUDY_PIXEL_PERCENTAGE", True).first())
            props = ref.toDictionary(
                ["system:index", "system:id", "system:time_start", "CLOUDY_PIXEL_PERCENTAGE"]
            ).getInfo()
            sensing_time = _iso_utc_from_millis(props.get("system:time_start"))
            cloudy_pct = props.get("CLOUDY_PIXEL_PERCENTAGE", "")
            system_index = props.get("system:index", "")
            asset_id = props.get("system:id", "")

        if selected is None:
            print(f"[{m.label}] no images")
            rows.append(
                {
                    "month": m.label,
                    "status": "no_data",
                    "image_count_total": 0,
                    "image_count_under_cloud": 0,
                    "selected_cloudy_pct": "",
                    "selected_sensing_time_utc": "",
                    "selected_system_index": "",
                    "selected_asset_id": "",
                    "selection_mode": "no_data",
                    "strategy": args.strategy,
                    "images_used": 0,
                    "output_tif": "",
                }
            )
            continue

        ds = open_xee_dataset(ee.Image(selected).clip(aoi), geometry=aoi, crs="EPSG:4326", scale_m=10.0)
        rgb = xr.concat([ds["B4"], ds["B3"], ds["B2"]], dim="band").assign_coords(band=[1, 2, 3])
        export_geotiff(rgb, out_tif, dtype="uint16", nodata=0)

        print(
            f"[{m.label}] ok total={total_count} under_cloud={under_cloud_count} used={images_used} "
            f"cloud_ref={cloudy_pct} time_ref={sensing_time}"
        )
        rows.append(
            {
                "month": m.label,
                "status": "ok",
                "image_count_total": total_count,
                "image_count_under_cloud": under_cloud_count,
                "selected_cloudy_pct": cloudy_pct,
                "selected_sensing_time_utc": sensing_time,
                "selected_system_index": system_index,
                "selected_asset_id": asset_id,
                "selection_mode": mode,
                "strategy": args.strategy,
                "images_used": images_used,
                "cloud_mask_source": args.cloud_mask_source,
                "s2cloudprob_threshold": args.s2cloudprob_threshold if args.cloud_mask_source == "s2cloudprob" else "",
                "cloudscore_threshold": args.cloudscore_threshold if args.cloud_mask_source == "cloudscoreplus" else "",
                "output_tif": str(out_tif),
            }
        )

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "month",
                "status",
                "image_count_total",
                "image_count_under_cloud",
                "selected_cloudy_pct",
                "selected_sensing_time_utc",
                "selected_system_index",
                "selected_asset_id",
                "selection_mode",
                "strategy",
                "images_used",
                "cloud_mask_source",
                "s2cloudprob_threshold",
                "cloudscore_threshold",
                "output_tif",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Summary: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

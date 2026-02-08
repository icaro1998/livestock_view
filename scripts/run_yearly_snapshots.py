#!/usr/bin/env python3
"""
Interactive wrapper to export Sentinel-1 monthly snapshots for a date range.
Generates one GeoTIFF per month (flood_diff) and supports resume by default.
"""

from __future__ import annotations

import os
import sys
import argparse
import re
import shutil
import atexit
from datetime import datetime
from pathlib import Path
from subprocess import run
from typing import Optional

import numpy as np
import rasterio
from rasterio.windows import Window
from rasterio.transform import from_origin


MIN_YEAR = 2018
BATCH_SIZE = 12
_SNAPSHOT_RE = re.compile(r"s1_flood_diff_(\d{4})-(\d{2})-\d{2}\.tif$")


def _parse_mm_yyyy(value: str) -> tuple[int, int]:
    raw = value.strip()
    parts = raw.split("/")
    if len(parts) != 2:
        raise ValueError("Expected MM/YYYY format.")
    month_str, year_str = parts
    if not (month_str.isdigit() and year_str.isdigit()):
        raise ValueError("Month and year must be numeric.")
    month = int(month_str)
    year = int(year_str)
    if month < 1 or month > 12:
        raise ValueError("Month must be 01-12.")
    if year < MIN_YEAR:
        raise ValueError(f"Year must be >= {MIN_YEAR}.")
    return year, month


def _next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _month_to_int(year: int, month: int) -> int:
    return year * 12 + (month - 1)


def _int_to_month(value: int) -> tuple[int, int]:
    year = value // 12
    month = value % 12 + 1
    return year, month


def _format_month(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _acquire_run_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        f"pid={os.getpid()}\n"
        f"started={datetime.now().isoformat(timespec='seconds')}\n"
        f"cwd={Path.cwd()}\n"
    ).encode("utf-8")
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, payload)
            return fd
        except FileExistsError:
            owner_pid = -1
            try:
                text = lock_path.read_text(encoding="utf-8")
                match = re.search(r"pid=(\d+)", text)
                if match:
                    owner_pid = int(match.group(1))
            except Exception:
                pass
            if owner_pid > 0 and not _pid_exists(owner_pid):
                try:
                    lock_path.unlink()
                    continue
                except Exception:
                    pass
            raise RuntimeError(
                f"Another run appears active (lock: {lock_path}, pid: {owner_pid})."
            )


def _release_run_lock(lock_fd: int, lock_path: Path) -> None:
    try:
        os.close(lock_fd)
    except Exception:
        pass
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _coverage_lines(snapshots_dir: Path, master_path: Path) -> list[str]:
    lines = [
        "Existing data:",
        f"  - snapshots: {snapshots_dir}",
        f"  - master:    {master_path}",
    ]
    snapshot_months = _list_snapshot_months(snapshots_dir)
    if snapshot_months:
        for line in _format_ranges(_ranges_from_months(snapshot_months)):
            lines.append(f"  - Snapshots: {line}")
    else:
        lines.append("  - Snapshots: none")

    netcdf_months = _list_netcdf_months(master_path)
    if netcdf_months:
        for line in _format_ranges(_ranges_from_months(netcdf_months)):
            lines.append(f"  - Master NetCDF: {line}")
    else:
        if master_path.exists():
            lines.append("  - Master NetCDF: present (time parsing unavailable)")
        else:
            lines.append("  - Master NetCDF: none")
    return lines


def _ranges_from_months(months: list[int]) -> list[tuple[int, int]]:
    if not months:
        return []
    months = sorted(set(months))
    ranges: list[tuple[int, int]] = []
    start = prev = months[0]
    for current in months[1:]:
        if current == prev + 1:
            prev = current
            continue
        ranges.append((start, prev))
        start = prev = current
    ranges.append((start, prev))
    return ranges


def _format_ranges(ranges: list[tuple[int, int]]) -> list[str]:
    lines = []
    for start, end in ranges:
        sy, sm = _int_to_month(start)
        ey, em = _int_to_month(end)
        count = end - start + 1
        if start == end:
            lines.append(f"{_format_month(sy, sm)} ({count} month)")
        else:
            lines.append(f"{_format_month(sy, sm)} → {_format_month(ey, em)} ({count} months)")
    return lines


def _list_snapshot_months(out_dir: Path) -> list[int]:
    months: list[int] = []
    if not out_dir.exists():
        return months
    for path in out_dir.glob("s1_flood_diff_*.tif"):
        match = _SNAPSHOT_RE.match(path.name)
        if not match:
            continue
        year = int(match.group(1))
        month = int(match.group(2))
        months.append(_month_to_int(year, month))
    return months


def _list_netcdf_months(nc_path: Path) -> list[int]:
    if not nc_path.exists():
        return []
    try:
        import xarray as xr
    except Exception:
        return []
    try:
        with xr.open_dataset(nc_path) as ds:
            if "time" not in ds:
                return []
            months = set()
            for t in ds["time"].values:
                try:
                    label = str(t)[:7]
                    year = int(label[:4])
                    month = int(label[5:7])
                    months.add(_month_to_int(year, month))
                except Exception:
                    continue
            return sorted(months)
    except Exception:
        return []


def _print_existing_coverage(snapshots_dir: Path, master_path: Path) -> None:
    for line in _coverage_lines(snapshots_dir, master_path):
        print(line)


def _resolve_dirs(base_dir: Path) -> dict[str, Path]:
    return {
        "base": base_dir,
        "snapshots": base_dir / "snapshots",
        "series": base_dir / "xarray",
        "master": base_dir / "master",
        "derived": base_dir / "derived",
        "logs": base_dir / "logs",
    }


def _ensure_dirs(dirs: dict[str, Path]) -> None:
    for key, path in dirs.items():
        if key == "base":
            continue
        path.mkdir(parents=True, exist_ok=True)


def _move_if_newer(src: Path, dest: Path) -> None:
    if dest.exists():
        try:
            if src.stat().st_mtime <= dest.stat().st_mtime:
                src.unlink(missing_ok=True)
                return
        except Exception:
            pass
        try:
            dest.unlink()
        except Exception:
            pass
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(src), str(dest))
    except PermissionError:
        # File is likely in use (e.g., open in QGIS). Try copy and keep source.
        try:
            shutil.copy2(str(src), str(dest))
            print(f"Warning: {src.name} is in use; copied to {dest} and left original.")
        except Exception:
            print(f"Warning: {src.name} is in use; could not move or copy. Skipping.")


def _migrate_existing(base_dir: Path, dirs: dict[str, Path]) -> None:
    if not base_dir.exists():
        return
    # Move legacy snapshots into snapshots/
    for path in base_dir.glob("s1_flood_diff_*.tif"):
        dest = dirs["snapshots"] / path.name
        _move_if_newer(path, dest)
    # Move NetCDF series into xarray/ or master/
    for path in base_dir.glob("s1_flood_diff_series*.nc"):
        if path.name == "s1_flood_diff_series.nc":
            dest = dirs["master"] / path.name
        else:
            dest = dirs["series"] / path.name
        _move_if_newer(path, dest)
    # Move frequency outputs into derived/
    for path in base_dir.glob("flood_diff_frequency*.tif"):
        dest = dirs["derived"] / path.name
        _move_if_newer(path, dest)
    # Move logs into logs/
    for path in base_dir.glob("run*.log"):
        dest = dirs["logs"] / path.name
        _move_if_newer(path, dest)


def _find_series_file(search_dir: Path, start: str, end: str) -> Optional[Path]:
    pattern = f"s1_flood_diff_series_*_{start}_{end}.nc"
    matches = sorted(search_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _move_series_file(src_dir: Path, dest_dir: Path, start: str, end: str) -> Optional[Path]:
    series_path = _find_series_file(src_dir, start, end)
    if not series_path:
        return None
    dest = dest_dir / series_path.name
    _move_if_newer(series_path, dest)
    return dest


def _verify_raster(path: Path) -> tuple[Optional[bool], str]:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False, "empty"
        with rasterio.open(path) as src:
            if src.count < 1:
                return False, "no bands"
            src.read(1, window=Window(0, 0, 1, 1))
        return True, "ok"
    except PermissionError:
        return None, "in use"
    except Exception as exc:
        return False, f"error: {exc}"


def _verify_netcdf(path: Path) -> tuple[Optional[bool], str]:
    if not path.exists():
        return False, "missing"
    try:
        import xarray as xr
    except Exception:
        return None, "xarray unavailable"
    try:
        ds = xr.open_dataset(path)
        # Touch a small bit of metadata to ensure it parses.
        _ = list(ds.data_vars)
        ds.close()
        return True, "ok"
    except PermissionError:
        return None, "in use"
    except Exception as exc:
        return False, f"error: {exc}"


def _verify_existing_files(dirs: dict[str, Path], log_path: Path, delete_bad: bool = True) -> None:
    lines = ["Verification summary:"]
    ok = bad = skipped = deleted = 0

    def _handle(path: Path, status: Optional[bool], detail: str) -> None:
        nonlocal ok, bad, skipped, deleted
        if status is True:
            ok += 1
            return
        if status is None:
            skipped += 1
            lines.append(f"  - SKIP (in use): {path.name}")
            return
        bad += 1
        lines.append(f"  - BAD  ({detail}): {path.name}")
        if delete_bad:
            try:
                path.unlink()
                deleted += 1
                lines.append(f"    deleted: {path.name}")
            except Exception:
                lines.append(f"    delete failed (in use or permission): {path.name}")

    # Snapshots
    for path in dirs["snapshots"].glob("s1_flood_diff_*.tif"):
        status, detail = _verify_raster(path)
        _handle(path, status, detail)

    # Derived rasters
    for path in dirs["derived"].glob("*.tif"):
        status, detail = _verify_raster(path)
        _handle(path, status, detail)

    # NetCDF series + master
    for path in dirs["series"].glob("*.nc"):
        status, detail = _verify_netcdf(path)
        _handle(path, status, detail)
    for path in dirs["master"].glob("*.nc"):
        status, detail = _verify_netcdf(path)
        _handle(path, status, detail)

    lines.append(f"Totals: ok={ok}, bad={bad}, deleted={deleted}, skipped={skipped}")
    _append_log(log_path, lines)
    for line in lines:
        print(line)


def _set_spatial_dims(da):
    if "lat" in da.dims and "lon" in da.dims:
        da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)
    elif "y" in da.dims and "x" in da.dims:
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)
    return da


def _ensure_transform(da):
    try:
        if "lat" in da.coords and "lon" in da.coords:
            y = np.asarray(da["lat"].values)
            x = np.asarray(da["lon"].values)
        elif "y" in da.coords and "x" in da.coords:
            y = np.asarray(da["y"].values)
            x = np.asarray(da["x"].values)
        else:
            return da
        if x.size < 2 or y.size < 2:
            return da
        x_res = float(abs(x[1] - x[0]))
        y_res = float(abs(y[1] - y[0]))
        x_min = float(np.min(x))
        y_max = float(np.max(y))
        transform = from_origin(x_min - x_res / 2, y_max + y_res / 2, x_res, y_res)
        return da.rio.write_transform(transform, inplace=False)
    except Exception:
        return da


def _merge_series(master_path: Path, new_path: Path) -> int:
    try:
        import xarray as xr
    except Exception as exc:
        print(f"Merge skipped: xarray not available ({exc}).")
        return 0
    datasets = []
    if master_path.exists():
        with xr.open_dataset(master_path) as ds:
            datasets.append(ds.load())
    with xr.open_dataset(new_path) as ds:
        datasets.append(ds.load())
    merged = xr.concat(datasets, dim="time").sortby("time")
    merged = merged.groupby("time").last()
    master_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_netcdf(master_path)
    return 0


def _build_timelapse_geotiff(series_path: Path, out_path: Path) -> int:
    try:
        import xarray as xr
        import rioxarray  # noqa: F401
    except Exception as exc:
        print(f"Timelapse GeoTIFF skipped: missing deps ({exc}).")
        return 0
    if not series_path.exists():
        print(f"Timelapse GeoTIFF skipped: missing {series_path.name}")
        return 0
    with xr.open_dataset(series_path) as ds:
        if "flood_diff" not in ds.data_vars:
            print("Timelapse GeoTIFF skipped: flood_diff not found.")
            return 0
        da = ds["flood_diff"]
        if "time" not in da.dims:
            print("Timelapse GeoTIFF skipped: time dimension missing.")
            return 0
        labels = [str(t)[:10] for t in ds["time"].values]
        da = da.rename({"time": "band"})
        da = da.assign_coords(band=np.arange(1, da.sizes["band"] + 1))
        if "lat" in da.dims and "lon" in da.dims:
            da = da.transpose("band", "lat", "lon")
        elif "y" in da.dims and "x" in da.dims:
            da = da.transpose("band", "y", "x")
        else:
            other_dims = [d for d in da.dims if d != "band"]
            da = da.transpose("band", *other_dims)
        da = _set_spatial_dims(da)
        da = da.rio.write_crs("EPSG:4326", inplace=False)
        da = _ensure_transform(da)
        try:
            for idx, label in enumerate(labels, start=1):
                da = da.rio.write_band_description(idx, label)
        except Exception:
            pass
        out_path.parent.mkdir(parents=True, exist_ok=True)
        da.rio.to_raster(
            out_path,
            compress="LZW",
            tiled=True,
            blockxsize=256,
            blockysize=256,
            BIGTIFF="IF_SAFER",
        )
    print(f"Timelapse GeoTIFF written: {out_path}")
    return 0


def _append_log(log_path: Path, lines: list[str]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"\n[{timestamp}] run_yearly_snapshots summary\n")
        for line in lines:
            handle.write(f"{line}\n")


def _months_between(start: str, end: str) -> list[int]:
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    s_int = _month_to_int(s.year, s.month)
    e_int = _month_to_int(e.year, e.month)
    return list(range(s_int, e_int))


def _split_range_by_months(start: str, end: str, chunk_months: int) -> list[tuple[str, str]]:
    if chunk_months <= 0:
        return [(start, end)]
    months = _months_between(start, end)
    if not months:
        return []
    chunks: list[tuple[str, str]] = []
    i = 0
    while i < len(months):
        chunk = months[i : i + chunk_months]
        sy, sm = _int_to_month(chunk[0])
        ey, em = _int_to_month(chunk[-1])
        c_start = f"{sy:04d}-{sm:02d}-01"
        end_year, end_month = _next_month(ey, em)
        c_end = f"{end_year:04d}-{end_month:02d}-01"
        chunks.append((c_start, c_end))
        i += chunk_months
    return chunks


def _missing_months_for_range(out_dir: Path, start: str, end: str) -> list[int]:
    requested = set(_months_between(start, end))
    existing = set(_list_snapshot_months(out_dir))
    missing = sorted(requested - existing)
    return missing


def _prompt_yes_no(question: str) -> bool:
    if not sys.stdin.isatty():
        return False
    while True:
        answer = input(f"{question} (Y/N): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("Please answer Y or N.")


def _prompt_range() -> tuple[str, str]:
    while True:
        raw_from = input(f'From (MM/YYYY, >= {MIN_YEAR}) e.g. "01/2020": ').strip()
        raw_to = input('To (MM/YYYY) e.g. "01/2026": ').strip()
        try:
            from_year, from_month = _parse_mm_yyyy(raw_from)
            to_year, to_month = _parse_mm_yyyy(raw_to)
        except ValueError as exc:
            print(f"Invalid range: {exc}")
            continue
        if (to_year, to_month) < (from_year, from_month):
            print("Invalid range: 'To' must be after or equal to 'From'.")
            continue
        start = f"{from_year:04d}-{from_month:02d}-01"
        end_year, end_month = _next_month(to_year, to_month)
        end = f"{end_year:04d}-{end_month:02d}-01"
        return start, end


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Sentinel-1 monthly snapshots for a date range."
    )
    parser.add_argument(
        "--from",
        dest="from_month",
        help='Start month (MM/YYYY), e.g. "01/2020".',
    )
    parser.add_argument(
        "--to",
        dest="to_month",
        help='End month (MM/YYYY), e.g. "01/2026".',
    )
    parser.add_argument(
        "--orbit",
        choices=["AUTO", "ASCENDING", "DESCENDING"],
        default="AUTO",
        help="Sentinel-1 orbit selection.",
    )
    parser.add_argument(
        "--polarization",
        choices=["VV", "VH"],
        default="VV",
        help="Sentinel-1 polarization.",
    )
    parser.add_argument(
        "--freq",
        choices=["M", "ME", "MS"],
        default="ME",
        help="Monthly aggregation frequency.",
    )
    parser.add_argument(
        "--agg",
        choices=["min", "median", "mean"],
        default="median",
        help="Monthly aggregation method.",
    )
    parser.add_argument(
        "--fallback-agg",
        choices=["none", "mean"],
        default="mean",
        help="Fallback aggregation if a chunk fails (recommended: mean for large runs).",
    )
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--out-dir", default="", help="Base output directory (default: output/flood)")
    parser.add_argument("--project-id", default="")
    parser.add_argument("--log-file", default="")
    parser.add_argument(
        "--fill-missing",
        choices=["ask", "yes", "no"],
        default="ask",
        help="After download, fill missing months (ask/yes/no).",
    )
    parser.add_argument(
        "--output-mode",
        choices=["merge", "separate"],
        default="merge",
        help="Merge into master NetCDF or keep separate series only.",
    )
    parser.add_argument(
        "--verify-existing",
        action="store_true",
        help="Verify existing files before starting (default: on).",
    )
    parser.add_argument(
        "--no-verify-existing",
        action="store_false",
        dest="verify_existing",
        help="Skip preflight verification of existing files.",
    )
    parser.add_argument(
        "--timelapse-only",
        action="store_true",
        help="Build timelapse GeoTIFF from existing master series and exit.",
    )
    parser.add_argument(
        "--chunk-months",
        type=int,
        default=0,
        help="Split requested range into chunks of N months (recommended for large runs).",
    )
    parser.add_argument(
        "--validate-outputs",
        action="store_true",
        help="Validate generated TIFF/NetCDF outputs at the end (default: on).",
    )
    parser.add_argument(
        "--no-validate-outputs",
        action="store_false",
        dest="validate_outputs",
        help="Skip end-of-run raster/NetCDF validation.",
    )
    parser.set_defaults(verify_existing=True, validate_outputs=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    base_dir = Path(args.out_dir or os.getenv("FLOOD_OUT_DIR") or "output/flood")
    lock_path = base_dir / ".run_yearly_snapshots.lock"
    try:
        lock_fd = _acquire_run_lock(lock_path)
    except RuntimeError as exc:
        print(f"Error: {exc}")
        return 2
    atexit.register(_release_run_lock, lock_fd, lock_path)
    dirs = _resolve_dirs(base_dir)
    _ensure_dirs(dirs)
    _migrate_existing(base_dir, dirs)
    master_path = dirs["master"] / "s1_flood_diff_series.nc"
    _print_existing_coverage(dirs["snapshots"], master_path)
    verify_log = dirs["logs"] / f"verify_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    if args.verify_existing:
        _verify_existing_files(dirs, verify_log, delete_bad=True)
    if args.timelapse_only:
        timelapse_source = master_path
        if not timelapse_source.exists():
            candidates = sorted(dirs["series"].glob("s1_flood_diff_series_*.nc"),
                                key=lambda p: p.stat().st_mtime, reverse=True)
            timelapse_source = candidates[0] if candidates else None
        if timelapse_source is None or not timelapse_source.exists():
            print("Timelapse build skipped: no series file found.")
            return 0
        timelapse_path = dirs["derived"] / "s1_flood_diff_timelapse.tif"
        _build_timelapse_geotiff(timelapse_source, timelapse_path)
        return 0
    if args.from_month or args.to_month:
        if not (args.from_month and args.to_month):
            print("Error: provide both --from and --to (MM/YYYY).")
            return 2
        try:
            from_year, from_month = _parse_mm_yyyy(args.from_month)
            to_year, to_month = _parse_mm_yyyy(args.to_month)
        except ValueError as exc:
            print(f"Invalid range: {exc}")
            return 2
        if (to_year, to_month) < (from_year, from_month):
            print("Invalid range: 'To' must be after or equal to 'From'.")
            return 2
        start = f"{from_year:04d}-{from_month:02d}-01"
        end_year, end_month = _next_month(to_year, to_month)
        end = f"{end_year:04d}-{end_month:02d}-01"
    else:
        start, end = _prompt_range()

    script_dir = Path(__file__).resolve().parent
    pipeline = script_dir / "flood_pipeline.py"
    if not pipeline.exists():
        print(f"Error: missing pipeline at {pipeline}")
        return 2

    out_dir_path = dirs["snapshots"]
    series_dir = dirs["series"]
    master_path = dirs["master"] / "s1_flood_diff_series.nc"
    project_id = args.project_id or os.getenv("EE_PROJECT_ID")
    log_file = args.log_file or os.getenv("FLOOD_LOG_FILE")
    if not log_file:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(dirs["logs"] / f"run_range_{stamp}.log")
    log_path = Path(log_file)

    def _run_pipeline(
        range_start: str,
        range_end: str,
        append_log: bool,
        agg_override: Optional[str] = None,
    ) -> int:
        out_dir = str(out_dir_path)
        agg_value = agg_override or args.agg
        cmd = [
            sys.executable,
            str(pipeline),
            "--mode",
            "snapshots",
            "--snapshots-geotiff",
            "--snapshots-max",
            str(max(1, int(args.batch_size))),
            "--snapshots-resume",
            "--resume-verify",
            "--log",
            "--no-series-generic",
            "--pause-key",
            "--s1-series-start",
            range_start,
            "--s1-series-end",
            range_end,
            "--s1-orbit",
            args.orbit,
            "--s1-polarization",
            args.polarization,
            "--s1-series-freq",
            args.freq,
            "--s1-series-agg",
            agg_value,
            "--out-dir",
            out_dir,
            "--log-file",
            str(log_path),
        ]
        if append_log:
            cmd.append("--log-append")
        if project_id:
            cmd.extend(["--project-id", project_id])
        print(f"Range: {range_start} → {range_end} (end exclusive, agg={agg_value})")
        print(f"Running: {' '.join(cmd)}")
        return run(cmd).returncode

    def _run_output_validation() -> int:
        validator = script_dir / "validate_rasters.py"
        if not validator.exists():
            print(f"Validation skipped: missing validator at {validator}")
            return 0
        cmd = [
            sys.executable,
            str(validator),
            "--root",
            str(base_dir),
            "--pattern",
            "snapshots/*.tif",
            "--pattern",
            "derived/*.tif",
            "--pattern",
            "series/*.nc",
            "--pattern",
            "master/*.nc",
        ]
        print(f"Validating outputs: {' '.join(cmd)}")
        return run(cmd).returncode

    def _merge_into_master(series_path: Optional[Path]) -> int:
        if args.output_mode == "separate":
            print("Output mode: separate (no master NetCDF update).")
            return 0
        if series_path is None or not series_path.exists():
            print("Warning: no series file found to merge.")
            return 0
        print(f"Merging into master: {master_path.name}")
        return _merge_series(master_path, series_path)

    ranges = _split_range_by_months(start, end, max(0, int(args.chunk_months)))
    if not ranges:
        print("No months in requested range. Nothing to do.")
        return 0
    if len(ranges) > 1:
        print(f"Chunk mode enabled: {len(ranges)} chunks of up to {args.chunk_months} months.")

    series_path: Optional[Path] = None
    for idx, (r_start, r_end) in enumerate(ranges, start=1):
        print(f"Chunk {idx}/{len(ranges)}: {r_start} → {r_end} (end exclusive)")
        result_code = _run_pipeline(r_start, r_end, append_log=(idx > 1))
        if result_code != 0 and args.agg == "median" and args.fallback_agg == "mean":
            retry_note = (
                f"Chunk {idx}/{len(ranges)} failed with agg=median; retrying with agg=mean."
            )
            print(retry_note)
            _append_log(log_path, [retry_note])
            result_code = _run_pipeline(r_start, r_end, append_log=True, agg_override="mean")
        if result_code != 0:
            _append_log(
                log_path,
                _coverage_lines(out_dir_path, master_path)
                + [f"Download failed in chunk {idx}/{len(ranges)} with exit code {result_code}."],
            )
            return result_code

        chunk_series = _move_series_file(out_dir_path, series_dir, r_start, r_end)
        if chunk_series is not None:
            series_path = chunk_series
        merge_code = _merge_into_master(chunk_series)
        if merge_code != 0:
            _append_log(
                log_path,
                _coverage_lines(out_dir_path, master_path)
                + [f"Merge failed in chunk {idx}/{len(ranges)} with exit code {merge_code}."],
            )
            return merge_code

    missing = _missing_months_for_range(out_dir_path, start, end)
    if missing:
        missing_lines = _format_ranges(_ranges_from_months(missing))
        summary = _coverage_lines(out_dir_path, master_path)
        summary += [f"Requested range: {start} → {end} (end exclusive)"]
        summary += ["Missing months detected:"] + [f"  - {line}" for line in missing_lines]
        summary += [
            "Note: missing months may indicate no Sentinel-1 images for the chosen orbit/polarization."
        ]
        _append_log(log_path, summary)
        print("\n".join(summary))
        fill_mode = args.fill_missing
        should_fill = False
        if fill_mode == "yes":
            should_fill = True
        elif fill_mode == "ask":
            should_fill = _prompt_yes_no("Download missing ranges now?")
        if should_fill:
            ranges = _ranges_from_months(missing)
            for start_int, end_int in ranges:
                sy, sm = _int_to_month(start_int)
                ey, em = _int_to_month(end_int)
                range_start = f"{sy:04d}-{sm:02d}-01"
                end_year, end_month = _next_month(ey, em)
                range_end = f"{end_year:04d}-{end_month:02d}-01"
                rc = _run_pipeline(range_start, range_end, append_log=True)
                if rc != 0:
                    _append_log(log_path, [f"Missing-range download failed: {range_start} → {range_end}"])
                    return rc
                series_path = _move_series_file(out_dir_path, series_dir, range_start, range_end)
                merge_rc = _merge_into_master(series_path)
                if merge_rc != 0:
                    _append_log(log_path, [f"Missing-range merge failed: {range_start} → {range_end}"])
                    return merge_rc
            remaining = _missing_months_for_range(out_dir_path, start, end)
            if remaining:
                remaining_lines = _format_ranges(_ranges_from_months(remaining))
                _append_log(
                    log_path,
                    ["Still missing after retry:"]
                    + [f"  - {line}" for line in remaining_lines]
                    + [
                        "Note: missing months may indicate no Sentinel-1 images for the chosen orbit/polarization."
                    ],
                )
            else:
                _append_log(log_path, ["All missing months filled successfully."])
    else:
        _append_log(
            log_path,
            _coverage_lines(out_dir_path, master_path)
            + [f"Requested range: {start} → {end} (end exclusive)"]
            + ["No missing months detected in requested range."],
        )
    # Build single timelapse GeoTIFF from master (preferred) or last range series.
    timelapse_source = master_path if master_path.exists() else series_path
    if timelapse_source is not None and timelapse_source.exists():
        timelapse_path = dirs["derived"] / "s1_flood_diff_timelapse.tif"
        _build_timelapse_geotiff(timelapse_source, timelapse_path)
        _append_log(log_path, [f"Timelapse GeoTIFF: {timelapse_path}"])
    else:
        _append_log(log_path, ["Timelapse GeoTIFF skipped: no source series found."])

    if args.validate_outputs:
        validate_code = _run_output_validation()
        if validate_code != 0:
            _append_log(log_path, [f"Output validation failed with exit code {validate_code}."])
            return validate_code
        _append_log(log_path, ["Output validation passed."])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Flood depth + flood events pipeline using Google Earth Engine + Xee.
Outputs GeoTIFF + NetCDF into the output directory.

Examples (PowerShell):
  python scripts/flood_pipeline.py --project-id gen-lang-client-0296388721 --mode both
  python scripts/flood_pipeline.py --mode depth --depth-dataset jrc-v2 --return-period 100
  python scripts/flood_pipeline.py --mode depth --depth-dataset jrc-v1 --return-period 100
  python scripts/flood_pipeline.py --mode events --start 2000-01-01 --end 2018-12-31
  python scripts/flood_pipeline.py --mode s1 --s1-before 2019-03 --s1-after 2019-04
  python scripts/flood_pipeline.py --mode snapshots --snapshots-geotiff --snapshots-max 6
  python scripts/flood_pipeline.py --mode precip --precip-start 2018-01-01 --precip-end 2020-01-01
  python scripts/flood_pipeline.py --mode floodplain
  python scripts/flood_pipeline.py --mode context
  python scripts/flood_pipeline.py --mode all
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import atexit
import sys
import math
from pathlib import Path
import threading
import time
from typing import Optional, Tuple

import rasterio
from rasterio.windows import Window

import ee
import numpy as np
import xarray as xr
import xee  # noqa: F401 - registers the xarray "ee" engine
import rioxarray  # noqa: F401 - enables GeoTIFF export
from rasterio.transform import from_origin

try:
    from tqdm import tqdm as _tqdm
except Exception:
    _tqdm = None


DEFAULT_LAT = -13.700278  # 13°42'01"S
DEFAULT_LON = -63.927778  # 63°55'40"W
DEFAULT_BUFFER_KM = 20.0
MIN_DATA_DATE = datetime(2018, 1, 1)
MIN_DATA_DATE_STR = MIN_DATA_DATE.date().isoformat()
S1_START_DEFAULT = MIN_DATA_DATE_STR
S1_END_DEFAULT = datetime.utcnow().date().isoformat()
M_PER_DEGREE = 111320.0
PROGRESS = True
PROGRESS_INTERVAL = 1.0
GEOTIFF_ONLY = False
BACKUP_XARRAY = False
BACKUP_DIR: Optional[Path] = None
LOG_PATH: Optional[Path] = None


class _Tee:
    def __init__(self, *streams, file_stream=None):
        self._streams = streams
        self._file_stream = file_stream

    def write(self, data):
        for stream in self._streams:
            try:
                stream.write(data)
            except Exception:
                pass
        if self._file_stream is not None:
            try:
                self._file_stream.write(data.replace("\r", "\n"))
                self._file_stream.flush()
            except Exception:
                pass

    def flush(self):
        for stream in self._streams:
            try:
                stream.flush()
            except Exception:
                pass
        if self._file_stream is not None:
            try:
                self._file_stream.flush()
            except Exception:
                pass

    def isatty(self):
        return False


@dataclass
class DateRange:
    start: str
    end: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flood depth + flood events pipeline.")
    parser.add_argument("--project-id", default="gen-lang-client-0296388721")
    parser.add_argument(
        "--mode",
        choices=[
            "depth",
            "events",
            "both",
            "s1",
            "precip",
            "floodplain",
            "s1-series",
            "snapshots",
            "context",
            "all",
        ],
        default="all",
    )

    # AOI
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT)
    parser.add_argument("--lon", type=float, default=DEFAULT_LON)
    parser.add_argument("--buffer-km", type=float, default=DEFAULT_BUFFER_KM)
    parser.add_argument("--aoi-width-km", type=float, default=0.0)
    parser.add_argument("--aoi-height-km", type=float, default=0.0)

    # Flood depth options
    parser.add_argument("--depth-dataset", choices=["jrc-v1", "jrc-v2", "wri"], default="jrc-v2")
    parser.add_argument("--return-period", type=int, default=100)
    parser.add_argument("--wri-scenario", choices=["historical", "rcp4p5", "rcp8p5"], default="historical")
    parser.add_argument("--wri-year", type=int, default=2010)

    # Flood events options (Global Flood Database)
    parser.add_argument("--start", default=MIN_DATA_DATE_STR)
    parser.add_argument("--end", default="2018-12-31")
    parser.add_argument("--max-events", type=int, default=100)

    # Sentinel-1 flood detection options
    parser.add_argument("--s1-start", default=S1_START_DEFAULT)
    parser.add_argument("--s1-end", default=S1_END_DEFAULT)
    parser.add_argument("--s1-before", default="2019-03")
    parser.add_argument("--s1-after", default="2019-04")
    parser.add_argument("--s1-orbit", choices=["AUTO", "ASCENDING", "DESCENDING"], default="AUTO")
    parser.add_argument("--s1-polarization", choices=["VV", "VH"], default="VV")
    parser.add_argument("--s1-scale", type=float, default=30.0)
    parser.add_argument("--s1-series-start", default=S1_START_DEFAULT)
    parser.add_argument("--s1-series-end", default=S1_END_DEFAULT)
    parser.add_argument("--s1-series-freq", choices=["M", "ME", "MS"], default="ME")
    parser.add_argument("--s1-series-agg", choices=["min", "median", "mean"], default="median")
    parser.add_argument("--snapshots-geotiff", action="store_true")
    parser.add_argument("--snapshots-max", type=int, default=0)
    parser.add_argument("--snapshots-offset", type=int, default=0)
    parser.add_argument("--snapshots-interactive", action="store_true")
    parser.add_argument("--snapshots-resume", action="store_true")
    parser.add_argument("--resume-verify", action="store_true")
    parser.add_argument("--no-resume-verify", action="store_false", dest="resume_verify")
    parser.add_argument("--pause-key", action="store_true")
    parser.add_argument("--no-pause-key", action="store_false", dest="pause_key")
    parser.add_argument("--no-series-generic", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--no-progress", action="store_false", dest="progress")
    parser.add_argument("--progress-interval", type=float, default=1.0)
    parser.add_argument("--geotiff-only", action="store_true")
    parser.add_argument("--clean-out-dir", action="store_true")
    parser.add_argument("--xarray-backups", action="store_true")
    parser.add_argument("--xarray-backups-dir", default="")
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--log-file", default="")
    parser.add_argument("--log-append", action="store_true")

    # Precipitation options (ERA5 monthly)
    parser.add_argument("--precip-start", default="2018-01-01")
    parser.add_argument("--precip-end", default="2020-01-01")
    parser.add_argument("--precip-scale", type=float, default=25000.0)

    # Floodplain options (GFPLAIN250m)
    parser.add_argument("--floodplain-scale", type=float, default=250.0)

    # Context / high-resolution terrain + surface water
    parser.add_argument("--context-scale", type=float, default=30.0)

    # Output
    parser.add_argument("--out-dir", default="output/flood")
    parser.add_argument("--ee-retries", type=int, default=5)
    parser.add_argument("--ee-retry-wait", type=float, default=5.0)

    parser.set_defaults(progress=True, resume_verify=True, pause_key=True)
    return parser.parse_args()


def ee_init(project_id: str, retries: int = 5, wait: float = 5.0) -> None:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            ee.Initialize(project=project_id)
            return
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            delay = wait * (2 ** (attempt - 1))
            print(
                f"Earth Engine init failed ({type(exc).__name__}). "
                f"Retrying in {delay:.0f}s ({attempt}/{retries})..."
            )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc


def _km_to_deg_lat(km: float) -> float:
    return km / 110.574


def _km_to_deg_lon(km: float, lat: float) -> float:
    return km / (111.320 * math.cos(math.radians(lat)))


def make_aoi(
    lat: float,
    lon: float,
    buffer_km: float,
    width_km: float,
    height_km: float,
) -> ee.Geometry:
    if width_km > 0 and height_km > 0:
        half_w = _km_to_deg_lon(width_km / 2.0, lat)
        half_h = _km_to_deg_lat(height_km / 2.0)
        return ee.Geometry.Rectangle([lon - half_w, lat - half_h, lon + half_w, lat + half_h])
    return ee.Geometry.Point(lon, lat).buffer(buffer_km * 1000).bounds()


def _transpose_spatial(da: xr.DataArray, y_dim: str, x_dim: str) -> xr.DataArray:
    # Ensure spatial dims are the last two and ordered (y, x) for rioxarray.
    dims = list(da.dims)
    if y_dim in dims and x_dim in dims:
        other = [d for d in dims if d not in (y_dim, x_dim)]
        da = da.transpose(*other, y_dim, x_dim)
    return da


def _set_spatial_dims(da: xr.DataArray) -> xr.DataArray:
    if "lat" in da.dims and "lon" in da.dims:
        da = _transpose_spatial(da, "lat", "lon")
        da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)
    elif "y" in da.dims and "x" in da.dims:
        da = _transpose_spatial(da, "y", "x")
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)
    elif "latitude" in da.dims and "longitude" in da.dims:
        da = _transpose_spatial(da, "latitude", "longitude")
        da = da.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=False)
    else:
        raise ValueError(f"Unsupported spatial dims: {da.dims}")
    return da


def _ensure_transform(da: xr.DataArray) -> xr.DataArray:
    try:
        transform = da.rio.transform(recalc=True)
        if not transform.is_identity:
            return da.rio.write_transform(transform, inplace=False)
    except Exception:
        pass

    # Fallback: derive transform from coordinates.
    x_dim = da.rio.x_dim or ("lon" if "lon" in da.dims else "x")
    y_dim = da.rio.y_dim or ("lat" if "lat" in da.dims else "y")
    x = np.asarray(da.coords[x_dim].values)
    y = np.asarray(da.coords[y_dim].values)

    if x.size == 0 or y.size == 0:
        return da

    if x.size > 1:
        x_res = float(np.mean(np.diff(x)))
    else:
        x_res = 1.0
    if y.size > 1:
        y_res = float(np.mean(np.diff(y)))
    else:
        y_res = -1.0

    x_res = abs(x_res)
    y_res = abs(y_res)
    x_min = float(np.min(x))
    y_max = float(np.max(y))
    transform = from_origin(x_min - x_res / 2, y_max + y_res / 2, x_res, y_res)
    return da.rio.write_transform(transform, inplace=False)


def _squeeze_non_spatial(da: xr.DataArray) -> xr.DataArray:
    spatial = {"lat", "lon", "x", "y", "latitude", "longitude"}
    squeeze_dims = [d for d in da.dims if d not in spatial and da.sizes.get(d, 0) == 1]
    if squeeze_dims:
        da = da.squeeze(squeeze_dims)
    return da


def _print_written(path: Path, prefix: Optional[str] = None, total: Optional[int] = None) -> None:
    try:
        size_mb = path.stat().st_size / (1024 * 1024)
        if total and total > 0:
            total_mb = total / (1024 * 1024)
            msg = f"{size_mb:.2f} MB / {total_mb:.2f} MB"
        else:
            msg = f"{size_mb:.2f} MB"
        label = prefix or "Wrote"
        print(f"{label} {path.name} {msg}")
    except Exception:
        label = prefix or "Wrote"
        print(f"{label} {path.name}")


def _estimate_nbytes(obj) -> Optional[int]:
    try:
        return int(obj.nbytes)
    except Exception:
        return None


def _tqdm_iter(iterator, total: Optional[int], desc: str):
    if _tqdm is None or not PROGRESS:
        return iterator
    return _tqdm(iterator, total=total, desc=desc, unit="snap")


def _is_valid_raster(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        with rasterio.open(path) as src:
            if src.count < 1:
                return False
            # Read a 1x1 window to confirm data access.
            src.read(1, window=Window(0, 0, 1, 1))
        return True
    except Exception:
        return False


def _start_pause_listener():
    if not sys.stdin.isatty():
        return None, None, None, None
    try:
        import msvcrt
    except Exception:
        return None, None, None, None

    pause_event = threading.Event()
    quit_event = threading.Event()
    stop_listener = threading.Event()

    def _listener():
        print("Controls: [P]ause/resume, [Q]uit after current snapshot.")
        while not stop_listener.is_set():
            if msvcrt.kbhit():
                ch = msvcrt.getwch().lower()
                if ch == "p":
                    if pause_event.is_set():
                        pause_event.clear()
                        print("Resumed.")
                    else:
                        pause_event.set()
                        print("Paused. Press P to resume.")
                elif ch == "q":
                    quit_event.set()
                    print("Stop requested. Will exit after current snapshot.")
            time.sleep(0.1)

    thread = threading.Thread(target=_listener, daemon=True)
    thread.start()
    return pause_event, quit_event, stop_listener, thread


def _maybe_pause(pause_event, quit_event) -> bool:
    if pause_event is None or quit_event is None:
        return False
    if quit_event.is_set():
        return True
    if pause_event.is_set():
        while pause_event.is_set():
            if quit_event.is_set():
                return True
            time.sleep(0.5)
    return quit_event.is_set()


def _progress_bar(current: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return ""
    filled = int(round(width * (current / total)))
    filled = min(width, max(0, filled))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _clamp_iso_date(value: str, min_date: datetime, label: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed < min_date:
        clamped = min_date.date().isoformat()
        print(f"{label} {value} < {clamped}; clamping to {clamped}.")
        return clamped
    return value


def _ensure_end_not_before_start(start: str, end: str, label: str) -> Tuple[str, str]:
    try:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
    except ValueError:
        return start, end
    if e < s:
        print(f"{label} end {end} < start {start}; clamping end to {start}.")
        return start, start
    return start, end


def _format_progress(label: str, current: int, total: Optional[int]) -> str:
    current_mb = current / (1024 * 1024)
    if total and total > 0:
        total_mb = total / (1024 * 1024)
        pct = min(100.0, (current / total) * 100.0)
        bar = _progress_bar(current, total, width=20)
        return f"{label}: {bar} {current_mb:.1f} MB / {total_mb:.1f} MB ({pct:.0f}%)"
    return f"{label}: {current_mb:.1f} MB"


def _monitor_file(path: Path, stop: threading.Event, label: str, total: Optional[int]) -> None:
    last = -1
    while not stop.is_set():
        if path.exists():
            size = path.stat().st_size
            if size != last:
                print(_format_progress(label, size, total), end="\r", flush=True)
                last = size
        time.sleep(PROGRESS_INTERVAL)
    print("")


def _write_with_progress(write_fn, path: Path, label: str, total: Optional[int]) -> None:
    if not PROGRESS:
        write_fn()
        return
    stop = threading.Event()
    thread = threading.Thread(target=_monitor_file, args=(path, stop, label, total), daemon=True)
    thread.start()
    try:
        write_fn()
    finally:
        stop.set()
        thread.join(timeout=2)


def save_geotiff(
    ds: xr.Dataset,
    var_name: str,
    out_path: Path,
    done_label: Optional[str] = None,
    total_override: Optional[int] = None,
) -> None:
    da = ds[var_name]
    # If extra dims remain, select the first index for each.
    extra_dims = [d for d in da.dims if d not in ("lat", "lon", "x", "y", "latitude", "longitude")]
    if extra_dims:
        da = da.isel({d: 0 for d in extra_dims})
    da = _squeeze_non_spatial(da)
    da = _set_spatial_dims(da)
    da = da.rio.write_crs("EPSG:4326", inplace=False)
    da = _ensure_transform(da)
    # XEE may inject a scale_factor tied to coordinate resolution (e.g. 1/3600),
    # which would incorrectly scale pixel values in GeoTIFF output.
    attrs = dict(da.attrs)
    encoding = dict(da.encoding)
    for key in ("scale_factor", "add_offset"):
        attrs.pop(key, None)
        encoding.pop(key, None)
    da.attrs = attrs
    da.encoding = encoding
    out_path.parent.mkdir(parents=True, exist_ok=True)
    total = total_override if total_override is not None else _estimate_nbytes(da)
    progress_label = f"{done_label} writing" if done_label else f"Writing {out_path.name}"
    _write_with_progress(
        lambda: da.rio.to_raster(
            out_path,
            compress="LZW",
            tiled=True,
            blockxsize=256,
            blockysize=256,
            BIGTIFF="IF_SAFER",
        ),
        out_path,
        progress_label,
        total,
    )
    _print_written(out_path, prefix=done_label, total=total)


def save_netcdf(ds: xr.Dataset, out_path: Path, done_label: Optional[str] = None) -> None:
    if GEOTIFF_ONLY:
        print(f"Skipped NetCDF (geotiff-only): {out_path.name}")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    total = _estimate_nbytes(ds)
    label = f"{done_label} writing" if done_label else f"Writing {out_path.name}"
    _write_with_progress(lambda: ds.to_netcdf(out_path), out_path, label, total)
    _print_written(out_path, prefix=done_label, total=total)


def save_netcdf_backup(ds: xr.Dataset, filename: str) -> None:
    if not BACKUP_XARRAY or BACKUP_DIR is None:
        return
    backup_path = BACKUP_DIR / filename
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    total = _estimate_nbytes(ds)
    label = f"Backup writing {backup_path.name}"
    _write_with_progress(lambda: ds.to_netcdf(backup_path), backup_path, label, total)
    _print_written(backup_path, prefix="Backup saved:", total=total)


def setup_logging(out_dir: Path, log_file: str, log_enabled: bool, append: bool) -> Optional[Path]:
    if not log_enabled and not log_file:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    if log_file:
        log_path = Path(log_file)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = out_dir / f"run_{stamp}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    # Line-buffered log stream so external tailers can watch progress in real time.
    log_stream = open(log_path, mode, encoding="utf-8", buffering=1)
    tee = _Tee(sys.stdout, file_stream=log_stream)
    sys.stdout = tee
    sys.stderr = tee

    def _close():
        try:
            log_stream.flush()
            log_stream.close()
        except Exception:
            pass

    atexit.register(_close)
    print(f"Logging to: {log_path}")
    return log_path


def open_xee_dataset(
    ic_or_img,
    geometry: ee.Geometry,
    crs: Optional[str] = None,
    projection: Optional[ee.Projection] = None,
    scale: Optional[float] = None,
) -> xr.Dataset:
    kwargs = {"engine": "ee", "geometry": geometry}
    if crs is not None:
        kwargs["crs"] = crs
    if projection is not None:
        kwargs["projection"] = projection
    if scale is not None:
        # Xee expects scale in CRS units. Convert meters -> degrees for EPSG:4326.
        crs_value = None
        if crs is not None:
            crs_value = crs
        elif projection is not None:
            try:
                crs_value = projection.crs().getInfo()
            except Exception:
                crs_value = None
        if isinstance(crs_value, str) and crs_value.upper() == "EPSG:4326" and scale > 1:
            scale = scale / M_PER_DEGREE
        kwargs["scale"] = scale
    return xr.open_dataset(ic_or_img, **kwargs)


def first_var_name(ds: xr.Dataset) -> str:
    if not ds.data_vars:
        raise ValueError("Dataset has no data variables")
    return next(iter(ds.data_vars))


def get_jrc_v1_depth(aoi: ee.Geometry, return_period: int) -> Tuple[xr.Dataset, str]:
    ic = (
        ee.ImageCollection("JRC/CEMS_GLOFAS/FloodHazard/v1")
        .filterBounds(aoi)
        .filter(ee.Filter.eq("return_period", return_period))
    )
    # Preserve native projection from source images.
    proj = ic.first().select(0).projection()
    img = ic.mosaic().clip(aoi)
    ds = open_xee_dataset(img, geometry=aoi, projection=proj, scale=90)
    band = first_var_name(ds)
    return ds, band


def get_jrc_depth(aoi: ee.Geometry, return_period: int) -> Tuple[xr.Dataset, str]:
    band = f"RP{return_period}_depth"
    ic = ee.ImageCollection("JRC/CEMS_GLOFAS/FloodHazard/v2_1").select(band).filterBounds(aoi)
    # Preserve native projection from source images; mosaic() projection can degrade to 1 degree.
    proj = ic.first().select(0).projection()
    img = ic.mosaic().clip(aoi)
    ds = open_xee_dataset(img, geometry=aoi, projection=proj, scale=90)
    return ds, band


def get_wri_depth(
    aoi: ee.Geometry,
    return_period: int,
    scenario: str,
    year: int,
) -> Tuple[xr.Dataset, str]:
    ic = (
        ee.ImageCollection("WRI/Aqueduct_Flood_Hazard_Maps/V2")
        .filterMetadata("floodtype", "equals", "inunriver")
        .filterMetadata("climatescenario", "equals", scenario)
        .filterMetadata("returnperiod", "equals", return_period)
        .filterMetadata("year", "equals", year)
        .select("inundation_depth")
    )
    img = ic.mosaic().clip(aoi)
    ds = open_xee_dataset(img, geometry=aoi, projection=img.projection(), scale=1000)
    return ds, "inundation_depth"


def clip_events_range(start: str, end: str) -> DateRange:
    # Global Flood Database covers 2000-2018, but we avoid pre-2018 data by default.
    min_date = datetime(2018, 1, 1)
    max_date = datetime(2018, 12, 31)
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    if s < min_date:
        s = min_date
    if s > max_date:
        s = max_date
    if e < min_date:
        e = min_date
    if e > max_date:
        e = max_date
    if e < s:
        e = s
    return DateRange(start=s.date().isoformat(), end=e.date().isoformat())


def get_gfd_events(aoi: ee.Geometry, start: str, end: str, max_events: int) -> xr.Dataset:
    ic = (
        ee.ImageCollection("GLOBAL_FLOOD_DB/MODIS_EVENTS/V1")
        .filterBounds(aoi)
        .filterDate(start, end)
        .limit(max_events)
    )
    # Use the first image's projection as a template.
    proj = ic.first().select(0).projection()
    ds = open_xee_dataset(ic, geometry=aoi, projection=proj, scale=30)
    return ds


def select_month(ds: xr.Dataset, month: str) -> xr.Dataset:
    sel = ds.sel(time=ds["time"].dt.strftime("%Y-%m") == month)
    if "time" not in sel.dims or sel.sizes.get("time", 0) == 0:
        raise ValueError(f"No data found for month {month}")
    return sel.isel(time=0)


def get_s1_collection(
    aoi: ee.Geometry,
    start: str,
    end: str,
    orbit: str,
    polarization: str,
) -> ee.ImageCollection:
    orbit = orbit.upper()
    return (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterDate(start, end)
        .filterBounds(aoi)
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", polarization))
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filter(ee.Filter.eq("orbitProperties_pass", orbit))
        .select(polarization)
    )


def count_s1_images(
    aoi: ee.Geometry,
    start: str,
    end: str,
    orbit: str,
    polarization: str,
) -> int:
    try:
        return int(get_s1_collection(aoi, start, end, orbit, polarization).size().getInfo())
    except Exception:
        return -1

def choose_s1_orbit(
    aoi: ee.Geometry,
    start: str,
    end: str,
    polarization: str,
) -> str:
    base = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterDate(start, end)
        .filterBounds(aoi)
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", polarization))
    )
    asc = base.filter(ee.Filter.eq("orbitProperties_pass", "ASCENDING")).size().getInfo()
    desc = base.filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING")).size().getInfo()
    chosen = "ASCENDING" if asc >= desc else "DESCENDING"
    print(f"Sentinel-1 orbit counts — ASCENDING: {asc}, DESCENDING: {desc}. Using {chosen}.")
    return chosen


def get_s1_flood_diff(
    aoi: ee.Geometry,
    start: str,
    end: str,
    before_month: str,
    after_month: str,
    orbit: str,
    polarization: str,
    scale: float,
) -> Tuple[xr.Dataset, str]:
    ic = get_s1_collection(aoi, start, end, orbit, polarization)
    # Use EPSG:4326 to avoid projection/geometry mismatches that can yield NaN scales.
    ds = open_xee_dataset(ic, geometry=aoi, crs="EPSG:4326", scale=scale)
    ds = ds.sortby("time")
    ds_monthly = ds.resample(time="ME").min("time")

    before = select_month(ds_monthly, before_month)
    after = select_month(ds_monthly, after_month)

    var = polarization if polarization in ds_monthly.data_vars else first_var_name(ds_monthly)
    flood = before[var] - after[var]

    out = xr.Dataset(
        {
            "before": before[var],
            "after": after[var],
            "flood_diff": flood,
        }
    )
    return out, "flood_diff"

def _normalize_freq(freq: str) -> str:
    # pandas 3.0 removed "M" alias; map to month-end ("ME")
    return "ME" if freq == "M" else freq


def _resample_agg(ds: xr.Dataset, freq: str, agg: str) -> xr.Dataset:
    freq = _normalize_freq(freq)
    if agg == "min":
        return ds.resample(time=freq).min("time")
    if agg == "median":
        return ds.resample(time=freq).median("time")
    if agg == "mean":
        return ds.resample(time=freq).mean("time")
    raise ValueError(f"Unsupported agg: {agg}")


def _time_label(value) -> str:
    try:
        return np.datetime_as_string(value, unit="D")
    except Exception:
        return str(value)


def get_s1_series(
    aoi: ee.Geometry,
    start: str,
    end: str,
    orbit: str,
    polarization: str,
    scale: float,
    freq: str,
    agg: str,
) -> xr.Dataset:
    ic = get_s1_collection(aoi, start, end, orbit, polarization)
    ds = open_xee_dataset(ic, geometry=aoi, crs="EPSG:4326", scale=scale)
    ds = ds.sortby("time")
    ds_monthly = _resample_agg(ds, freq, agg)
    var = polarization if polarization in ds_monthly.data_vars else first_var_name(ds_monthly)
    flood_diff = ds_monthly[var].shift(time=1) - ds_monthly[var]
    out = xr.Dataset(
        {
            "backscatter": ds_monthly[var],
            "flood_diff": flood_diff,
        }
    )
    return out


def get_era5_precip(
    aoi: ee.Geometry,
    start: str,
    end: str,
    scale: float,
) -> Tuple[xr.Dataset, str]:
    # ERA5 monthly can be unavailable in recent ranges in EE. Fall back to
    # ERA5-Land monthly aggregates when needed.
    candidates = [
        ("ECMWF/ERA5/MONTHLY", "total_precipitation"),
        ("ECMWF/ERA5_LAND/MONTHLY_AGGR", "total_precipitation_sum"),
    ]
    for collection, band in candidates:
        ic = ee.ImageCollection(collection).filterDate(start, end).select(band)
        try:
            count = int(ic.size().getInfo())
        except Exception:
            count = 0
        if count <= 0:
            continue

        proj = ic.first().select(0).projection()
        ds = open_xee_dataset(ic, geometry=aoi, projection=proj, scale=scale)
        ds = ds.sortby("time") * 1000.0  # m -> mm
        if band != "total_precipitation":
            ds = ds.rename({band: "total_precipitation"})
        ds.attrs["precip_source_collection"] = collection
        return ds, "total_precipitation"

    raise ValueError(
        f"No precipitation images found in range {start} → {end} "
        "for ERA5 monthly or ERA5-Land monthly aggregates."
    )


def get_gfplain(
    aoi: ee.Geometry,
    scale: float,
) -> Tuple[xr.Dataset, str]:
    img = ee.Image("IAHS/GFPLAIN250/v0").select("flood").clip(aoi)
    ds = open_xee_dataset(img, geometry=aoi, projection=img.projection(), scale=scale)
    return ds, "flood"


def clean_out_dir(out_dir: Path) -> None:
    if not out_dir.exists():
        return
    patterns = [
        "flood_depth_*.nc",
        "flood_depth_*.tif",
        "flood_events_gfd*.nc",
        "flood_events_gfd*.tif",
        "s1_flood_diff*.nc",
        "s1_flood_diff*.tif",
        "s1_before*.tif",
        "s1_after*.tif",
        "era5_precip*.nc",
        "era5_precip*.tif",
        "gfplain250*.nc",
        "gfplain250*.tif",
        "terrain_context.nc",
        "terrain_*.tif",
        "surface_water.nc",
        "surface_water_*.tif",
    ]
    removed = 0
    for pattern in patterns:
        for path in out_dir.glob(pattern):
            try:
                path.unlink()
                removed += 1
            except Exception:
                pass
    if removed:
        print(f"Cleaned {removed} output files in {out_dir}")


def get_terrain_context(aoi: ee.Geometry, scale: float) -> xr.Dataset:
    dem = ee.Image("USGS/SRTMGL1_003").select("elevation").rename("elevation").clip(aoi)
    # Earth Engine Python API exposes terrain derivatives as static methods.
    slope = ee.Terrain.slope(dem).rename("slope")
    aspect = ee.Terrain.aspect(dem).rename("aspect")
    hillshade = ee.Terrain.hillshade(dem).rename("hillshade")
    img = dem.addBands([slope, aspect, hillshade])
    ds = open_xee_dataset(img, geometry=aoi, projection=dem.projection(), scale=scale)
    return ds


def get_surface_water(aoi: ee.Geometry, scale: float) -> xr.Dataset:
    img = (
        ee.Image("JRC/GSW1_4/GlobalSurfaceWater")
        .select(["occurrence", "seasonality"])
        .clip(aoi)
    )
    ds = open_xee_dataset(img, geometry=aoi, projection=img.projection(), scale=scale)
    return ds


def main() -> None:
    args = parse_args()
    # Avoid pre-2018 data by default to keep outputs smaller.
    args.start = _clamp_iso_date(args.start, MIN_DATA_DATE, "events start")
    args.end = _clamp_iso_date(args.end, MIN_DATA_DATE, "events end")
    args.start, args.end = _ensure_end_not_before_start(args.start, args.end, "events range")
    args.s1_start = _clamp_iso_date(args.s1_start, MIN_DATA_DATE, "s1-start")
    args.s1_end = _clamp_iso_date(args.s1_end, MIN_DATA_DATE, "s1-end")
    args.s1_start, args.s1_end = _ensure_end_not_before_start(
        args.s1_start, args.s1_end, "s1 range"
    )
    args.s1_series_start = _clamp_iso_date(args.s1_series_start, MIN_DATA_DATE, "s1-series-start")
    args.s1_series_end = _clamp_iso_date(args.s1_series_end, MIN_DATA_DATE, "s1-series-end")
    args.s1_series_start, args.s1_series_end = _ensure_end_not_before_start(
        args.s1_series_start, args.s1_series_end, "s1-series range"
    )
    args.precip_start = _clamp_iso_date(args.precip_start, MIN_DATA_DATE, "precip-start")
    args.precip_end = _clamp_iso_date(args.precip_end, MIN_DATA_DATE, "precip-end")
    args.precip_start, args.precip_end = _ensure_end_not_before_start(
        args.precip_start, args.precip_end, "precip range"
    )
    global PROGRESS, PROGRESS_INTERVAL, GEOTIFF_ONLY, BACKUP_XARRAY, BACKUP_DIR, LOG_PATH
    PROGRESS = args.progress
    PROGRESS_INTERVAL = max(0.2, float(args.progress_interval))
    GEOTIFF_ONLY = bool(args.geotiff_only)
    BACKUP_XARRAY = bool(args.xarray_backups)

    out_dir = Path(args.out_dir)
    LOG_PATH = setup_logging(out_dir, args.log_file, args.log, args.log_append)
    if args.clean_out_dir:
        clean_out_dir(out_dir)
    if BACKUP_XARRAY:
        if args.xarray_backups_dir:
            BACKUP_DIR = Path(args.xarray_backups_dir)
        else:
            BACKUP_DIR = out_dir / "xarray_backups"
    ee_init(args.project_id, retries=args.ee_retries, wait=args.ee_retry_wait)
    aoi = make_aoi(args.lat, args.lon, args.buffer_km, args.aoi_width_km, args.aoi_height_km)
    print(
        "Run config:"
        f" mode={args.mode}"
        f" aoi=({args.lat},{args.lon})"
        f" buffer_km={args.buffer_km}"
        f" aoi_width_km={args.aoi_width_km}"
        f" aoi_height_km={args.aoi_height_km}"
        f" s1_scale={args.s1_scale}"
        f" s1_series_freq={args.s1_series_freq}"
        f" s1_series_agg={args.s1_series_agg}"
        f" out_dir={out_dir}"
        f" geotiff_only={GEOTIFF_ONLY}"
        f" xarray_backups={BACKUP_XARRAY}"
    )

    if args.mode in ("depth", "both", "all"):
        if args.depth_dataset == "jrc-v1":
            ds_depth, band = get_jrc_v1_depth(aoi, args.return_period)
            tag = f"jrc_v1_rp{args.return_period}"
        elif args.depth_dataset == "jrc-v2":
            ds_depth, band = get_jrc_depth(aoi, args.return_period)
            tag = f"jrc_v2_rp{args.return_period}"
        else:
            ds_depth, band = get_wri_depth(
                aoi, args.return_period, args.wri_scenario, args.wri_year
            )
            tag = f"wri_{args.wri_scenario}_{args.wri_year}_rp{args.return_period}"

        if band not in ds_depth.data_vars:
            band = first_var_name(ds_depth)
        save_netcdf_backup(ds_depth, f"flood_depth_{tag}.nc")
        save_netcdf(ds_depth, out_dir / f"flood_depth_{tag}.nc")
        save_geotiff(ds_depth, band, out_dir / f"flood_depth_{tag}.tif")
        print(f"Saved flood depth outputs: {tag}")

    if args.mode in ("events", "both", "all"):
        clipped = clip_events_range(args.start, args.end)
        ds_events = get_gfd_events(aoi, clipped.start, clipped.end, args.max_events)
        save_netcdf_backup(ds_events, "flood_events_gfd.nc")
        save_netcdf(ds_events, out_dir / "flood_events_gfd.nc")

        # Save a simple “any flood” mask (max over time) if time dim exists.
        if "time" in ds_events.dims:
            flood_var = "flooded" if "flooded" in ds_events.data_vars else first_var_name(ds_events)
            flooded_any = ds_events[flood_var].max("time")
            flooded_any = flooded_any.to_dataset(name="flooded_any")
            save_geotiff(flooded_any, "flooded_any", out_dir / "flood_events_gfd_any.tif")
        print("Saved flood events outputs (Global Flood Database)")

    if args.mode in ("s1", "all"):
        orbit = args.s1_orbit.upper()
        if orbit == "AUTO":
            orbit = choose_s1_orbit(aoi, args.s1_start, args.s1_end, args.s1_polarization)
        count = count_s1_images(aoi, args.s1_start, args.s1_end, orbit, args.s1_polarization)
        if count >= 0:
            print(f"Sentinel-1 images for orbit {orbit} ({args.s1_polarization}) in range: {count}")
        if count <= 0:
            print(
                f"No Sentinel-1 images for orbit {orbit} ({args.s1_polarization}) "
                f"in range {args.s1_start} → {args.s1_end}. Skipping."
            )
        else:
            ds_s1, band = get_s1_flood_diff(
                aoi,
                args.s1_start,
                args.s1_end,
                args.s1_before,
                args.s1_after,
                orbit,
                args.s1_polarization,
                args.s1_scale,
            )
            s1_tag = f"{orbit.lower()}_{args.s1_polarization.lower()}_{args.s1_before}_{args.s1_after}"
            save_netcdf_backup(ds_s1, f"s1_flood_diff_{s1_tag}.nc")
            save_netcdf(ds_s1, out_dir / f"s1_flood_diff_{s1_tag}.nc")
            save_geotiff(ds_s1, "flood_diff", out_dir / f"s1_flood_diff_{s1_tag}.tif")
            save_geotiff(ds_s1, "before", out_dir / f"s1_before_{s1_tag}.tif")
            save_geotiff(ds_s1, "after", out_dir / f"s1_after_{s1_tag}.tif")
            # Keep generic filenames for QGIS convenience.
            save_netcdf_backup(ds_s1, "s1_flood_diff.nc")
            save_netcdf(ds_s1, out_dir / "s1_flood_diff.nc")
            save_geotiff(ds_s1, "flood_diff", out_dir / "s1_flood_diff.tif")
            save_geotiff(ds_s1, "before", out_dir / "s1_before.tif")
            save_geotiff(ds_s1, "after", out_dir / "s1_after.tif")
            print("Saved Sentinel-1 flood difference outputs")

    if args.mode in ("s1-series", "snapshots", "all"):
        orbit = args.s1_orbit.upper()
        if orbit == "AUTO":
            orbit = choose_s1_orbit(aoi, args.s1_series_start, args.s1_series_end, args.s1_polarization)
        count = count_s1_images(aoi, args.s1_series_start, args.s1_series_end, orbit, args.s1_polarization)
        if count >= 0:
            print(f"Sentinel-1 images for orbit {orbit} ({args.s1_polarization}) in range: {count}")
        if count <= 0:
            print(
                f"No Sentinel-1 images for orbit {orbit} ({args.s1_polarization}) "
                f"in range {args.s1_series_start} → {args.s1_series_end}. Skipping."
            )
            ds_series = None
        else:
            ds_series = get_s1_series(
                aoi,
                args.s1_series_start,
                args.s1_series_end,
                orbit,
                args.s1_polarization,
                args.s1_scale,
                args.s1_series_freq,
                args.s1_series_agg,
            )
        tag = f"{orbit.lower()}_{args.s1_polarization.lower()}_{args.s1_series_start}_{args.s1_series_end}"
        if ds_series is not None:
            save_netcdf_backup(ds_series, f"s1_flood_diff_series_{tag}.nc")
            save_netcdf(ds_series, out_dir / f"s1_flood_diff_series_{tag}.nc")
            if not args.no_series_generic:
                save_netcdf_backup(ds_series, "s1_flood_diff_series.nc")
                save_netcdf(ds_series, out_dir / "s1_flood_diff_series.nc")
            print("Saved Sentinel-1 monthly series (backscatter + flood_diff)")

        if ds_series is not None and "time" in ds_series.dims:
            time_vals = ds_series["time"].values
            total = len(time_vals)
            print(f"Snapshots available: {total} ({_time_label(time_vals[0])} → {_time_label(time_vals[-1])}).")

        if ds_series is not None and args.snapshots_geotiff and "time" in ds_series.dims:
            time_vals = ds_series["time"].values
            total = len(time_vals)
            offset = max(0, args.snapshots_offset)
            batch_size = args.snapshots_max if args.snapshots_max > 0 else 0
            if args.snapshots_interactive and batch_size <= 0:
                batch_size = 12
                print("snapshots-max not set; defaulting batch size to 12 for interactive mode.")
            if offset >= total:
                print(f"Snapshot offset {offset} >= total snapshots {total}. Nothing to export.")
            else:
                remaining_total = total - offset
                batch_total = 1 if batch_size <= 0 else math.ceil(remaining_total / batch_size)
                batch_num = 0
                pause_event = quit_event = stop_listener = pause_thread = None
                if args.pause_key:
                    pause_event, quit_event, stop_listener, pause_thread = _start_pause_listener()
                stop_all = False
                while offset < total:
                    if _maybe_pause(pause_event, quit_event):
                        stop_all = True
                        break
                    remaining = total - offset
                    to_export = remaining if batch_size <= 0 else min(remaining, batch_size)
                    first_label = _time_label(time_vals[offset])
                    last_label = _time_label(time_vals[offset + to_export - 1])
                    batch_num += 1
                    batch_label = f"Batch {batch_num}/{batch_total}"
                    print(
                        f"{batch_label}: exporting {to_export} from offset {offset} "
                        f"({first_label} → {last_label})."
                    )
                    if BACKUP_XARRAY and BACKUP_DIR is not None:
                        batch_ds = ds_series.isel(time=slice(offset, offset + to_export))
                        backup_name = f"s1_flood_diff_series_batch_{first_label}_{last_label}.nc"
                        save_netcdf_backup(batch_ds, backup_name)

                    use_tqdm = _tqdm is not None and PROGRESS
                    iterator = _tqdm_iter(
                        enumerate(time_vals[offset : offset + to_export]),
                        total=to_export,
                        desc=f"{batch_label} snapshots",
                    ) if use_tqdm else enumerate(time_vals[offset : offset + to_export])
                    skipped = 0
                    written = 0
                    for batch_idx, t in iterator:
                        if _maybe_pause(pause_event, quit_event):
                            stop_all = True
                            break
                        label = _time_label(t).replace(":", "-")
                        snap = ds_series.isel(time=offset + batch_idx)
                        snap = snap[["flood_diff"]]
                        out_path = out_dir / f"s1_flood_diff_{label}.tif"
                        if args.snapshots_resume and out_path.exists():
                            if args.resume_verify and not _is_valid_raster(out_path):
                                print(f"Corrupt snapshot detected, deleting: {out_path.name}")
                                try:
                                    out_path.unlink()
                                except Exception:
                                    pass
                            else:
                                skipped += 1
                                if PROGRESS and not use_tqdm:
                                    bar = _progress_bar(batch_idx + 1, to_export)
                                    print(
                                        f"{batch_label} {bar} {batch_idx + 1}/{to_export} (skip)",
                                        end="\r",
                                        flush=True,
                                    )
                                continue
                        done_label = f"Snapshot {batch_idx + 1}/{to_export} saved:"
                        save_geotiff(snap, "flood_diff", out_path, done_label=done_label)
                        written += 1
                        if PROGRESS and not use_tqdm:
                            bar = _progress_bar(batch_idx + 1, to_export)
                            print(
                                f"{batch_label} {bar} {batch_idx + 1}/{to_export}",
                                end="\r",
                                flush=True,
                            )
                    if PROGRESS and not use_tqdm:
                        print("")
                    if args.snapshots_resume:
                        print(f"{batch_label} done: {written} written, {skipped} skipped.")

                    offset += to_export
                    if stop_all:
                        break
                    if not args.snapshots_interactive or offset >= total:
                        break
                    try:
                        answer = input("Continue to next batch? (Y/N): ").strip().lower()
                    except EOFError:
                        answer = "n"
                    if answer not in ("y", "yes"):
                        print("Stopping batch export.")
                        break
                if stop_listener is not None and pause_thread is not None:
                    stop_listener.set()
                    pause_thread.join(timeout=1)
                if stop_all:
                    print("Paused/quit requested. Exiting snapshot export.")

    if args.mode in ("precip", "all"):
        ds_pr, var = get_era5_precip(aoi, args.precip_start, args.precip_end, args.precip_scale)
        save_netcdf_backup(ds_pr, "era5_precip.nc")
        save_netcdf(ds_pr, out_dir / "era5_precip.nc")
        pr_mean = ds_pr[var].mean("time").to_dataset(name="precip_mean_mm")
        save_geotiff(pr_mean, "precip_mean_mm", out_dir / "era5_precip_mean.tif")
        print("Saved ERA5 precipitation outputs")

    if args.mode in ("context", "all"):
        ds_terrain = get_terrain_context(aoi, args.context_scale)
        save_netcdf_backup(ds_terrain, "terrain_context.nc")
        save_netcdf(ds_terrain, out_dir / "terrain_context.nc")
        for var in ds_terrain.data_vars:
            save_geotiff(ds_terrain, var, out_dir / f"terrain_{var}.tif")
        print("Saved terrain context outputs (elevation, slope, aspect, hillshade)")

        ds_water = get_surface_water(aoi, args.context_scale)
        save_netcdf_backup(ds_water, "surface_water.nc")
        save_netcdf(ds_water, out_dir / "surface_water.nc")
        for var in ds_water.data_vars:
            save_geotiff(ds_water, var, out_dir / f"surface_water_{var}.tif")
        print("Saved surface water outputs (occurrence, seasonality)")

    if args.mode in ("floodplain", "all"):
        ds_fp, var = get_gfplain(aoi, args.floodplain_scale)
        if var not in ds_fp.data_vars:
            var = first_var_name(ds_fp)
        save_netcdf_backup(ds_fp, "gfplain250.nc")
        save_netcdf(ds_fp, out_dir / "gfplain250.nc")
        save_geotiff(ds_fp, var, out_dir / "gfplain250.tif")
        try:
            data = ds_fp[var].values
            data = data[~np.isnan(data)]
            if data.size and np.all(data == 0):
                print("Warning: GFPLAIN250 contains only 0s in this AOI. Try a larger --buffer-km.")
        except Exception:
            pass
        print("Saved GFPLAIN250 floodplain outputs")

    print(f"Run complete: {datetime.now().isoformat(timespec='seconds')}")
    if LOG_PATH is not None:
        print(f"Log saved: {LOG_PATH}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Audit and repair pipeline for exporting xarray data to QGIS-safe GeoTIFF.
"""

from __future__ import annotations

import argparse
import logging
import pickle
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import array_bounds
import xarray as xr

import rioxarray  # noqa: F401 - registers rio accessor


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.export_geotiff import ensure_dataarray, export_geotiff, normalize_spatial_da


LOGGER = logging.getLogger("geotiff_audit")
WORLD_X_RANGE = (-180.5, 180.5)
WORLD_Y_RANGE = (-90.5, 90.5)
PERCENTILES = [1, 5, 50, 95, 99]


@dataclass
class AuditState:
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.failures.append(message)
        LOGGER.error("FAIL: %s", message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)
        LOGGER.warning("WARN: %s", message)

    def note(self, message: str) -> None:
        self.notes.append(message)
        LOGGER.info("NOTE: %s", message)


def configure_logging() -> Path:
    logs_dir = REPO_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"geotiff_audit_{ts}.log"

    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    LOGGER.addHandler(stream_handler)
    LOGGER.addHandler(file_handler)
    LOGGER.propagate = False
    return log_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit xarray -> GeoTIFF export for QGIS compatibility."
    )
    parser.add_argument("--input", type=Path, help="Input path (.nc/.zarr/.pkl/.tif/.tiff)")
    parser.add_argument("--var", help="Data variable name when input is a Dataset")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("output/geotiff_audit_output.tif"),
        help="Output GeoTIFF path",
    )
    parser.add_argument("--crs", help="Explicit CRS, e.g. EPSG:4326 or EPSG:32720")
    parser.add_argument(
        "--nodata",
        type=float,
        default=None,
        help="Optional explicit nodata value",
    )
    parser.add_argument("--dtype", default=None, help="Optional output dtype")
    parser.add_argument("--compress", default="deflate", help="Compression codec")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run on synthetic lat/lon demo data (ignores --input)",
    )
    args = parser.parse_args()

    if not args.demo and args.input is None:
        parser.error("Use --input <path> or --demo")
    return args


def _as_1d_float(values: Any) -> np.ndarray:
    arr = np.asarray(values, dtype="float64")
    if arr.ndim != 1:
        raise ValueError("Expected 1D coordinate array")
    return arr


def _monotonic_direction(values: np.ndarray) -> str:
    diffs = np.diff(values)
    if diffs.size == 0:
        return "single"
    if np.all(diffs > 0):
        return "ascending"
    if np.all(diffs < 0):
        return "descending"
    if np.all(diffs == 0):
        return "constant"
    return "non-monotonic"


def _is_regular_spacing(values: np.ndarray) -> tuple[bool, float | None]:
    if values.size < 2:
        return (False, None)
    diffs = np.diff(values)
    step = float(np.median(diffs))
    atol = max(1e-12, abs(step) * 1e-6)
    ok = np.allclose(diffs, step, rtol=1e-6, atol=atol)
    return bool(ok), step


def _coords_look_like_degrees(x_values: np.ndarray, y_values: np.ndarray) -> bool:
    return (
        np.nanmin(x_values) >= WORLD_X_RANGE[0]
        and np.nanmax(x_values) <= WORLD_X_RANGE[1]
        and np.nanmin(y_values) >= WORLD_Y_RANGE[0]
        and np.nanmax(y_values) <= WORLD_Y_RANGE[1]
    )


def _extract_crs_from_attrs(da: xr.DataArray) -> str | None:
    for key in ("crs", "spatial_ref", "crs_wkt", "wkt"):
        value = da.attrs.get(key)
        if value:
            return str(value)
    for key in ("epsg", "epsg_code"):
        value = da.attrs.get(key)
        if value is not None:
            return f"EPSG:{int(value)}"
    grid_mapping = da.attrs.get("grid_mapping")
    if grid_mapping and grid_mapping in da.coords:
        mapping_attrs = da.coords[grid_mapping].attrs
        for key in ("crs", "spatial_ref", "crs_wkt", "wkt"):
            value = mapping_attrs.get(key)
            if value:
                return str(value)
    return None


def _compute_data_stats(da: xr.DataArray) -> dict[str, Any]:
    arr = np.asanyarray(da.values)

    if np.ma.isMaskedArray(arr):
        mask = np.ma.getmaskarray(arr)
        raw = np.asarray(arr.data)
    else:
        mask = np.zeros(arr.shape, dtype=bool)
        raw = np.asarray(arr)

    total_count = raw.size
    if total_count == 0:
        return {
            "total_count": 0,
            "nan_count": 0,
            "nan_ratio": 0.0,
            "finite_count": 0,
            "is_constant": False,
            "min": None,
            "max": None,
            "percentiles": {},
        }

    if raw.dtype.kind in ("f", "c"):
        float_values = raw.astype("float64", copy=False)
        invalid = mask | ~np.isfinite(float_values)
        finite_values = float_values[~invalid]
    else:
        invalid = mask
        finite_values = raw[~invalid].astype("float64", copy=False)

    nan_count = int(np.count_nonzero(invalid))
    finite_count = int(finite_values.size)

    if finite_count == 0:
        return {
            "total_count": total_count,
            "nan_count": nan_count,
            "nan_ratio": nan_count / total_count,
            "finite_count": 0,
            "is_constant": False,
            "min": None,
            "max": None,
            "percentiles": {},
        }

    min_val = float(np.min(finite_values))
    max_val = float(np.max(finite_values))
    percentiles = {
        int(p): float(v)
        for p, v in zip(PERCENTILES, np.percentile(finite_values, PERCENTILES), strict=False)
    }

    return {
        "total_count": total_count,
        "nan_count": nan_count,
        "nan_ratio": nan_count / total_count,
        "finite_count": finite_count,
        "is_constant": bool(np.isclose(min_val, max_val)),
        "min": min_val,
        "max": max_val,
        "percentiles": percentiles,
    }


def _safe_rio_transform(da: xr.DataArray) -> Any:
    try:
        return da.rio.transform(recalc=False)
    except Exception:
        return None


def _coord_bounds(values: np.ndarray) -> tuple[float, float] | None:
    if values.size == 0:
        return None
    if values.size == 1:
        v = float(values[0])
        return (v, v)
    diffs = np.diff(values)
    step = float(np.median(diffs))
    lo = float(values[0] - (step / 2.0))
    hi = float(values[-1] + (step / 2.0))
    return (min(lo, hi), max(lo, hi))


def _canonical_bounds(bounds: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    left, bottom, right, top = bounds
    return (min(left, right), min(bottom, top), max(left, right), max(bottom, top))


def _candidate_spatial_dims(da: xr.DataArray) -> tuple[str | None, str | None]:
    dims = list(da.dims)

    x_dim = None
    y_dim = None
    if "x" in dims and "y" in dims:
        return ("x", "y")

    for name in ("lon", "longitude"):
        if name in dims:
            x_dim = name
            break
    for name in ("lat", "latitude"):
        if name in dims:
            y_dim = name
            break

    if x_dim is None:
        for name in ("x", "lon", "longitude"):
            if name in da.coords and da[name].ndim == 1:
                x_dim = da[name].dims[0]
                break
    if y_dim is None:
        for name in ("y", "lat", "latitude"):
            if name in da.coords and da[name].ndim == 1:
                y_dim = da[name].dims[0]
                break

    return (x_dim, y_dim)


def _check_spatial_metadata(da: xr.DataArray, state: AuditState) -> dict[str, Any]:
    LOGGER.info("=== B) Spatial Reference / Transform ===")

    x_dim, y_dim = _candidate_spatial_dims(da)
    LOGGER.info("Candidate spatial dims: x=%s y=%s", x_dim, y_dim)

    if not x_dim or not y_dim:
        state.fail(
            "Coordinates not recognized as spatial dims (expected x/y or lon/lat style names)."
        )
        return {"x_dim": x_dim, "y_dim": y_dim, "coord_bounds": None}

    x_coord_name = "x" if "x" in da.coords else x_dim
    y_coord_name = "y" if "y" in da.coords else y_dim

    if x_coord_name not in da.coords or y_coord_name not in da.coords:
        state.fail(
            "Spatial dims were found but coordinate arrays are missing. Export cannot build a transform."
        )
        return {"x_dim": x_dim, "y_dim": y_dim, "coord_bounds": None}

    x_values = _as_1d_float(da[x_coord_name].values)
    y_values = _as_1d_float(da[y_coord_name].values)

    x_dir = _monotonic_direction(x_values)
    y_dir = _monotonic_direction(y_values)
    x_regular, x_step = _is_regular_spacing(x_values)
    y_regular, y_step = _is_regular_spacing(y_values)

    LOGGER.info(
        "x coord: count=%d range=[%.8f, %.8f] monotonic=%s regular=%s step=%s",
        x_values.size,
        float(np.min(x_values)),
        float(np.max(x_values)),
        x_dir,
        x_regular,
        None if x_step is None else f"{x_step:.8g}",
    )
    LOGGER.info(
        "y coord: count=%d range=[%.8f, %.8f] monotonic=%s regular=%s step=%s",
        y_values.size,
        float(np.min(y_values)),
        float(np.max(y_values)),
        y_dir,
        y_regular,
        None if y_step is None else f"{y_step:.8g}",
    )

    if x_dir in ("constant", "non-monotonic") or y_dir in ("constant", "non-monotonic"):
        state.fail("Spatial coordinates are not monotonic. Transform inference will fail.")
    if not x_regular or not y_regular:
        state.fail("Spatial coordinates are not regularly spaced. GeoTIFF affine may be invalid.")

    rio_crs = da.rio.crs
    attr_crs = _extract_crs_from_attrs(da)
    LOGGER.info("CRS check: da.rio.crs=%s | attr-derived=%s", rio_crs, attr_crs)

    if rio_crs is None and attr_crs is None:
        if _coords_look_like_degrees(x_values, y_values):
            state.warn(
                "No CRS metadata found, but coordinates look like lon/lat degrees; "
                "normalizer can infer EPSG:4326."
            )
        else:
            state.fail("No CRS found in rio metadata/attrs and coords do not look like degrees.")

    transform = _safe_rio_transform(da)
    LOGGER.info("Existing transform: %s", transform)

    coord_x_bounds = _coord_bounds(x_values)
    coord_y_bounds = _coord_bounds(y_values)
    coord_bounds = None
    if coord_x_bounds and coord_y_bounds:
        coord_bounds = (
            coord_x_bounds[0],
            coord_y_bounds[0],
            coord_x_bounds[1],
            coord_y_bounds[1],
        )
        LOGGER.info(
            "Bounds from coords (left,bottom,right,top): (%.8f, %.8f, %.8f, %.8f)",
            coord_bounds[0],
            coord_bounds[1],
            coord_bounds[2],
            coord_bounds[3],
        )

    return {
        "x_dim": x_dim,
        "y_dim": y_dim,
        "x_dir": x_dir,
        "y_dir": y_dir,
        "coord_bounds": coord_bounds,
        "rio_crs": rio_crs,
        "attr_crs": attr_crs,
        "transform": transform,
    }


def _check_data_validity(da: xr.DataArray, state: AuditState) -> dict[str, Any]:
    LOGGER.info("=== A) Data Validity ===")
    LOGGER.info("dims=%s", da.dims)
    LOGGER.info("shape=%s", da.shape)
    LOGGER.info("dtype=%s", da.dtype)

    stats = _compute_data_stats(da)
    LOGGER.info(
        "counts: total=%d finite=%d nan=%d nan_ratio=%.4f",
        stats["total_count"],
        stats["finite_count"],
        stats["nan_count"],
        stats["nan_ratio"],
    )
    LOGGER.info("min=%s max=%s", stats["min"], stats["max"])
    LOGGER.info("percentiles=%s", stats["percentiles"])

    if stats["finite_count"] == 0:
        state.fail("All pixels are nodata/NaN; QGIS will appear empty.")
    elif stats["is_constant"]:
        state.warn("All finite pixels have the same value; layer can look flat/empty.")

    return stats


def _check_nodata(da: xr.DataArray, state: AuditState) -> dict[str, Any]:
    LOGGER.info("=== C) Nodata Handling ===")
    rio_nodata = da.rio.nodata
    attr_nodata = None
    for key in ("_FillValue", "missing_value", "fill_value", "nodata"):
        if key in da.attrs:
            attr_nodata = da.attrs[key]
            break

    LOGGER.info("rio.nodata=%r", rio_nodata)
    LOGGER.info("attr nodata=%r", attr_nodata)

    if rio_nodata is None and attr_nodata is None:
        state.warn("No nodata metadata found; exporter will assign default nodata.")

    dtype = np.dtype(da.dtype)
    chosen = rio_nodata if rio_nodata is not None else attr_nodata
    if chosen is not None:
        try:
            chosen_float = float(chosen)
            if np.isnan(chosen_float):
                state.warn("Input nodata is NaN. Exporter should replace with numeric nodata.")
                if dtype.kind in ("i", "u"):
                    state.fail("NaN nodata with integer dtype is invalid for GeoTIFF metadata.")
        except (TypeError, ValueError):
            state.warn("Input nodata is non-numeric; exporter will replace it.")

    return {"rio_nodata": rio_nodata, "attr_nodata": attr_nodata}


def _check_dim_handling(da: xr.DataArray, state: AuditState) -> dict[str, Any]:
    LOGGER.info("=== D) Band/Time Dimension Correctness ===")
    non_spatial = [d for d in da.dims if d not in ("x", "y", "lon", "lat", "longitude", "latitude")]
    LOGGER.info("non-spatial dims on input=%s", non_spatial)

    time_like = [d for d in non_spatial if d.lower() in ("time", "month", "date", "datetime", "bnds", "bounds")]
    if time_like:
        state.note(
            "Time-like dims detected (%s). Export should slice to one frame unless explicit multiband."
            % ",".join(time_like)
        )

    unknown = [d for d in non_spatial if d.lower() not in ("time", "month", "date", "datetime", "bnds", "bounds", "band")]
    if unknown:
        state.warn(
            "Non-spatial dims %s are not time-like/band. Slice explicitly before export if needed."
            % unknown
        )

    return {"non_spatial": non_spatial, "time_like": time_like, "unknown": unknown}


def _check_bounds_consistency(
    normalized: xr.DataArray,
    pre_coord_bounds: tuple[float, float, float, float] | None,
    state: AuditState,
) -> None:
    transform = _safe_rio_transform(normalized)
    if transform is None:
        state.fail("Normalized raster has no affine transform.")
        return

    height = int(normalized.sizes["y"])
    width = int(normalized.sizes["x"])
    tf_bounds_raw = array_bounds(height, width, transform)
    tf_bounds = _canonical_bounds(tf_bounds_raw)
    LOGGER.info(
        "Bounds from transform (canonical left,bottom,right,top): (%.8f, %.8f, %.8f, %.8f)",
        tf_bounds[0],
        tf_bounds[1],
        tf_bounds[2],
        tf_bounds[3],
    )

    if pre_coord_bounds:
        canonical_coord_bounds = _canonical_bounds(pre_coord_bounds)
        deltas = [
            abs(a - b) for a, b in zip(tf_bounds, canonical_coord_bounds, strict=False)
        ]
        LOGGER.info("Bounds delta |transform - coords| = %s", [round(v, 10) for v in deltas])
        if any(v > 1e-6 for v in deltas):
            state.warn(
                "Transform bounds differ from coordinate-derived bounds. Check spatial dim mapping and spacing."
            )

    if tf_bounds[0] == tf_bounds[2] or tf_bounds[1] == tf_bounds[3]:
        state.fail("Transform produced zero-area bounds.")


def _sample_band_stats(src: rasterio.io.DatasetReader, band: int) -> dict[str, Any]:
    sample_h = min(src.height, 1024)
    sample_w = min(src.width, 1024)
    data = src.read(
        band,
        masked=True,
        out_shape=(sample_h, sample_w),
        resampling=Resampling.nearest,
    )

    values = data.compressed() if np.ma.isMaskedArray(data) else np.asarray(data).reshape(-1)
    if values.size == 0:
        return {"finite_count": 0, "min": None, "max": None}
    finite = np.asarray(values, dtype="float64")
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return {"finite_count": 0, "min": None, "max": None}
    return {
        "finite_count": int(finite.size),
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
    }


def _check_written_geotiff(path: Path, state: AuditState) -> dict[str, Any]:
    LOGGER.info("=== E) GeoTIFF Integrity Post-Write ===")
    with rasterio.open(path) as src:
        LOGGER.info("path=%s", path)
        LOGGER.info("width=%d height=%d bands=%d", src.width, src.height, src.count)
        LOGGER.info("crs=%s", src.crs)
        LOGGER.info("transform=%s", src.transform)
        LOGGER.info("bounds=%s", src.bounds)
        LOGGER.info("nodata=%r", src.nodata)

        canonical_bounds = _canonical_bounds(
            (src.bounds.left, src.bounds.bottom, src.bounds.right, src.bounds.top)
        )

        if src.crs is None:
            state.fail("Written GeoTIFF has no CRS.")
        if canonical_bounds[0] == canonical_bounds[2] or canonical_bounds[1] == canonical_bounds[3]:
            state.fail("Written GeoTIFF has zero-area bounds.")
        if (
            canonical_bounds[0] == 0
            and canonical_bounds[1] == 0
            and canonical_bounds[2] == 0
            and canonical_bounds[3] == 0
        ):
            state.fail("Written GeoTIFF bounds are all zero.")

        if src.crs and src.crs.to_epsg() == 4326:
            out_of_world = (
                canonical_bounds[0] < WORLD_X_RANGE[0]
                or canonical_bounds[2] > WORLD_X_RANGE[1]
                or canonical_bounds[1] < WORLD_Y_RANGE[0]
                or canonical_bounds[3] > WORLD_Y_RANGE[1]
            )
            if out_of_world:
                state.fail("EPSG:4326 bounds are outside world degree ranges (units mismatch suspected).")

        band_stats: dict[int, dict[str, Any]] = {}
        all_empty = True
        for idx in range(1, src.count + 1):
            stats = _sample_band_stats(src, idx)
            band_stats[idx] = stats
            LOGGER.info(
                "band %d stats (sampled): finite_count=%d min=%s max=%s",
                idx,
                stats["finite_count"],
                stats["min"],
                stats["max"],
            )
            if stats["finite_count"] > 0:
                all_empty = False

        if all_empty:
            state.fail("All output bands are nodata-only in sampled read.")

        return {
            "width": src.width,
            "height": src.height,
            "count": src.count,
            "crs": src.crs,
            "transform": src.transform,
            "bounds": src.bounds,
            "nodata": src.nodata,
            "band_stats": band_stats,
        }


def _load_input(path: Path, var: str | None) -> xr.DataArray:
    suffix = path.suffix.lower()

    if suffix in (".tif", ".tiff"):
        da = rioxarray.open_rasterio(path, masked=True)
        return ensure_dataarray(da)

    if suffix in (".nc", ".netcdf"):
        ds = xr.open_dataset(path)
        return ensure_dataarray(ds, var=var)

    if suffix == ".zarr":
        ds = xr.open_zarr(path)
        return ensure_dataarray(ds, var=var)

    if suffix in (".pkl", ".pickle"):
        with path.open("rb") as f:
            obj = pickle.load(f)
        return ensure_dataarray(obj, var=var)

    try:
        ds = xr.open_dataset(path)
        return ensure_dataarray(ds, var=var)
    except Exception:
        pass

    try:
        with path.open("rb") as f:
            obj = pickle.load(f)
        return ensure_dataarray(obj, var=var)
    except Exception as exc:
        raise ValueError(
            f"Unsupported input format for '{path}'. Use .nc/.zarr/.pkl/.tif or --demo."
        ) from exc


def _make_demo_dataarray() -> xr.DataArray:
    y = np.linspace(-13.9, -13.5, 64)
    x = np.linspace(-63.95, -63.55, 80)
    yy, xx = np.meshgrid(y, x, indexing="ij")

    # Synthetic flood proxy with noise and nodata patch.
    values = np.exp(-(((xx + 63.74) ** 2) + ((yy + 13.74) ** 2)) / 0.003)
    values = values + np.random.default_rng(42).normal(0.0, 0.02, size=values.shape)
    values = values.astype("float32")
    values[values < 0.01] = np.nan

    da = xr.DataArray(
        values,
        dims=("lat", "lon"),
        coords={"lat": y, "lon": x},
        name="demo_flood_diff",
        attrs={"long_name": "Synthetic flood index"},
    )
    return da


def _evaluate_hypotheses(
    state: AuditState,
    spatial_info: dict[str, Any],
    data_stats: dict[str, Any],
    output_info: dict[str, Any] | None,
    nodata_info: dict[str, Any],
    dim_info: dict[str, Any],
) -> None:
    LOGGER.info("=== Root-Cause Hypotheses ===")

    if spatial_info.get("rio_crs") is None and spatial_info.get("attr_crs") is None:
        state.note("Hypothesis 1 matched: no CRS present in input metadata.")

    if not spatial_info.get("x_dim") or not spatial_info.get("y_dim"):
        state.note("Hypothesis 2 matched: coords/dims not recognized as spatial.")

    if spatial_info.get("y_dir") == "descending":
        state.note("Hypothesis 3 possible: latitude was descending before normalization.")

    if data_stats.get("finite_count", 0) == 0:
        state.note("Hypothesis 4 matched: data is all NaN/nodata.")

    if dim_info.get("time_like") and output_info is not None and output_info.get("count", 0) > 1:
        state.note("Hypothesis 5 possible: multiple time slices exported as bands.")

    nodata = nodata_info.get("rio_nodata")
    if nodata is None:
        nodata = nodata_info.get("attr_nodata")
    if nodata is None:
        state.note("Hypothesis 6 possible: nodata missing in input metadata.")
    else:
        try:
            if np.isnan(float(nodata)):
                state.note("Hypothesis 6 matched: nodata=NaN detected in input.")
        except Exception:
            state.note("Hypothesis 6 possible: nodata is non-numeric.")

    if output_info is not None:
        crs = output_info.get("crs")
        bounds = output_info.get("bounds")
        if crs and crs.to_epsg() == 4326 and bounds is not None:
            canonical_bounds = _canonical_bounds(
                (bounds.left, bounds.bottom, bounds.right, bounds.top)
            )
            out_of_world = (
                canonical_bounds[0] < WORLD_X_RANGE[0]
                or canonical_bounds[2] > WORLD_X_RANGE[1]
                or canonical_bounds[1] < WORLD_Y_RANGE[0]
                or canonical_bounds[3] > WORLD_Y_RANGE[1]
            )
            if out_of_world:
                state.note("Hypothesis 7 matched: bounds look out-of-range for EPSG:4326.")


def main() -> int:
    args = parse_args()
    log_path = configure_logging()
    state = AuditState()

    LOGGER.info("GeoTIFF audit started")
    LOGGER.info("log_file=%s", log_path)

    if args.demo:
        da = _make_demo_dataarray()
        LOGGER.info("Input mode: demo synthetic raster")
    else:
        input_path: Path = args.input.resolve()
        LOGGER.info("Input mode: file")
        LOGGER.info("input=%s", input_path)
        if not input_path.exists():
            state.fail(f"Input not found: {input_path}")
            LOGGER.info("Audit finished with failures")
            LOGGER.info("log_file=%s", log_path)
            return 2
        da = _load_input(input_path, args.var)

    if args.crs and da.rio.crs is None:
        LOGGER.info("Applying explicit CRS before export: %s", args.crs)
        da = da.rio.write_crs(args.crs, inplace=False)

    LOGGER.info("Variable name=%s", da.name)

    data_stats = _check_data_validity(da, state)
    spatial_info = _check_spatial_metadata(da, state)
    nodata_info = _check_nodata(da, state)
    dim_info = _check_dim_handling(da, state)

    normalized: xr.DataArray | None = None
    try:
        normalized = normalize_spatial_da(da, crs=args.crs)
        LOGGER.info("normalize_spatial_da: success")
        _check_bounds_consistency(normalized, spatial_info.get("coord_bounds"), state)
    except Exception as exc:
        state.fail(f"normalize_spatial_da failed: {exc}")

    output_info: dict[str, Any] | None = None
    output_path = args.out.resolve()
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        export_input = normalized if normalized is not None else da
        written = export_geotiff(
            export_input,
            output_path,
            nodata=args.nodata,
            dtype=args.dtype,
            compress=args.compress,
        )
        LOGGER.info("GeoTIFF written: %s", written)
        output_info = _check_written_geotiff(written, state)
    except Exception as exc:
        state.fail(f"export_geotiff failed: {exc}")

    _evaluate_hypotheses(state, spatial_info, data_stats, output_info, nodata_info, dim_info)

    LOGGER.info("=== Summary ===")
    LOGGER.info("failures=%d warnings=%d notes=%d", len(state.failures), len(state.warnings), len(state.notes))

    if state.failures:
        LOGGER.info("Status: FAILED")
        LOGGER.info("log_file=%s", log_path)
        return 1

    LOGGER.info("Status: OK")
    LOGGER.info("log_file=%s", log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

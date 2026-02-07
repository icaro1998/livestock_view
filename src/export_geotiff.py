#!/usr/bin/env python3
"""
Robust GeoTIFF export utilities for xarray/rioxarray objects.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import xarray as xr
from affine import Affine

import rioxarray  # noqa: F401 - enables .rio accessors


_X_NAME_CANDIDATES: tuple[str, ...] = ("x", "lon", "longitude")
_Y_NAME_CANDIDATES: tuple[str, ...] = ("y", "lat", "latitude")
_SLICE_LAST_DIMS: tuple[str, ...] = ("time", "month", "date", "datetime", "bnds", "bounds")


def ensure_dataarray(data: xr.DataArray | xr.Dataset, var: str | None = None) -> xr.DataArray:
    """
    Normalize input into a DataArray.
    """
    if isinstance(data, xr.DataArray):
        return data
    if not isinstance(data, xr.Dataset):
        raise TypeError(f"Expected DataArray or Dataset, got: {type(data)!r}")
    if var:
        if var not in data.data_vars:
            raise KeyError(f"Variable '{var}' not found. Available: {list(data.data_vars)}")
        return data[var]

    numeric_vars = [name for name, da in data.data_vars.items() if np.issubdtype(da.dtype, np.number)]
    if not numeric_vars:
        raise ValueError("Dataset has no numeric data variables. Provide --var explicitly.")
    return data[numeric_vars[0]]


def _find_dim_from_candidates(dims: Sequence[str], candidates: Sequence[str]) -> str | None:
    for name in candidates:
        if name in dims:
            return name
    return None


def _find_spatial_dims(da: xr.DataArray) -> tuple[str, str]:
    dims = list(da.dims)

    # Priority 1: direct x/y dims.
    if "x" in dims and "y" in dims:
        return "x", "y"

    # Priority 2: lon/lat (or longitude/latitude) as dims.
    x_dim = _find_dim_from_candidates(dims, _X_NAME_CANDIDATES[1:])
    y_dim = _find_dim_from_candidates(dims, _Y_NAME_CANDIDATES[1:])
    if x_dim and y_dim:
        return x_dim, y_dim

    # Priority 3: 1D coordinate vars pointing to dims.
    if not x_dim:
        for name in _X_NAME_CANDIDATES:
            if name in da.coords and da[name].ndim == 1:
                x_dim = da[name].dims[0]
                break
    if not y_dim:
        for name in _Y_NAME_CANDIDATES:
            if name in da.coords and da[name].ndim == 1:
                y_dim = da[name].dims[0]
                break

    if not x_dim or not y_dim:
        raise ValueError(
            "Could not identify spatial dims. Expected dims like (x,y), (lon,lat), "
            "or coordinate vars lon/lat."
        )
    if x_dim == y_dim:
        raise ValueError(f"Invalid spatial mapping: x and y resolved to same dim '{x_dim}'.")
    return x_dim, y_dim


def _as_float_array(values: xr.DataArray | np.ndarray) -> np.ndarray:
    arr = np.asarray(values)
    if arr.ndim != 1:
        raise ValueError("Spatial coordinates must be 1D.")
    return arr.astype("float64")


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


def _is_regular_spacing(values: np.ndarray) -> tuple[bool, float]:
    if values.size < 2:
        return False, np.nan
    diffs = np.diff(values)
    step = float(np.median(diffs))
    atol = max(1e-12, abs(step) * 1e-6)
    ok = np.allclose(diffs, step, rtol=1e-6, atol=atol)
    return bool(ok), step


def _coords_look_like_degrees(x_vals: np.ndarray, y_vals: np.ndarray) -> bool:
    return (
        np.nanmin(x_vals) >= -180.5
        and np.nanmax(x_vals) <= 180.5
        and np.nanmin(y_vals) >= -90.5
        and np.nanmax(y_vals) <= 90.5
    )


def _extract_crs_candidate(da: xr.DataArray) -> str | None:
    if da.rio.crs is not None:
        return str(da.rio.crs)

    def _from_attrs(attrs: dict[str, Any]) -> str | None:
        for key in ("crs", "spatial_ref", "crs_wkt", "wkt"):
            value = attrs.get(key)
            if value:
                return str(value)
        for key in ("epsg", "epsg_code"):
            value = attrs.get(key)
            if value is not None:
                return f"EPSG:{int(value)}"
        return None

    direct = _from_attrs(dict(da.attrs))
    if direct:
        return direct

    grid_mapping = da.attrs.get("grid_mapping")
    if grid_mapping and grid_mapping in da.coords:
        mapped = _from_attrs(dict(da.coords[grid_mapping].attrs))
        if mapped:
            return mapped

    return None


def _infer_transform_from_coords(da_xy: xr.DataArray) -> Affine:
    x_vals = _as_float_array(da_xy["x"].values)
    y_vals = _as_float_array(da_xy["y"].values)

    x_regular, x_step = _is_regular_spacing(x_vals)
    y_regular, y_step = _is_regular_spacing(y_vals)
    if not x_regular or not y_regular:
        raise ValueError(
            "Could not infer affine transform: x/y coordinates are not regular grids. "
            "Resample/reproject to a regular grid before export."
        )
    if np.isclose(x_step, 0.0) or np.isclose(y_step, 0.0):
        raise ValueError("Coordinate steps must be non-zero.")

    x0 = float(x_vals[0] - (x_step / 2.0))
    y0 = float(y_vals[0] - (y_step / 2.0))
    return Affine(float(x_step), 0.0, x0, 0.0, float(y_step), y0)


def normalize_spatial_da(da: xr.DataArray, crs: str | None = None) -> xr.DataArray:
    """
    Normalize DataArray to QGIS-safe spatial layout:
    - dims renamed to x/y
    - monotonic ascending x/y coordinates
    - CRS present
    - affine transform written
    """
    if not isinstance(da, xr.DataArray):
        raise TypeError(f"normalize_spatial_da expects DataArray, got {type(da)!r}")

    x_dim, y_dim = _find_spatial_dims(da)
    rename_map: dict[str, str] = {}
    if x_dim != "x":
        rename_map[x_dim] = "x"
    if y_dim != "y":
        rename_map[y_dim] = "y"
    if rename_map:
        da = da.rename(rename_map)

    if "x" not in da.coords or "y" not in da.coords:
        raise ValueError(
            "Spatial coordinates missing after dim normalization. "
            "Expected coordinate arrays for both 'x' and 'y'."
        )

    x_vals = _as_float_array(da["x"].values)
    y_vals = _as_float_array(da["y"].values)
    if not np.all(np.isfinite(x_vals)) or not np.all(np.isfinite(y_vals)):
        raise ValueError("Spatial coordinates contain NaN/Inf values.")

    x_dir = _monotonic_direction(x_vals)
    y_dir = _monotonic_direction(y_vals)
    if x_dir == "descending":
        da = da.sortby("x")
    elif x_dir not in ("ascending", "single"):
        raise ValueError(f"x coordinate is {x_dir}; expected monotonic coordinates.")
    if y_dir == "descending":
        da = da.sortby("y")
    elif y_dir not in ("ascending", "single"):
        raise ValueError(f"y coordinate is {y_dir}; expected monotonic coordinates.")

    x_vals = _as_float_array(da["x"].values)
    y_vals = _as_float_array(da["y"].values)
    if x_vals.size > 1 and np.any(np.diff(x_vals) <= 0):
        raise ValueError("x coordinate has duplicates/non-increasing values after sorting.")
    if y_vals.size > 1 and np.any(np.diff(y_vals) <= 0):
        raise ValueError("y coordinate has duplicates/non-increasing values after sorting.")

    da = da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)

    crs_value = _extract_crs_candidate(da)
    if not crs_value:
        if crs:
            crs_value = crs
        elif _coords_look_like_degrees(x_vals, y_vals):
            crs_value = "EPSG:4326"
        else:
            raise ValueError(
                "CRS is missing and could not be inferred from metadata/coords. "
                "Pass a CRS explicitly, e.g. normalize_spatial_da(da, crs='EPSG:32720')."
            )
    da = da.rio.write_crs(crs_value, inplace=False)

    transform = _infer_transform_from_coords(da)
    da = da.rio.write_transform(transform, inplace=False)
    return da


def _normalize_band_layout(da_xy: xr.DataArray) -> xr.DataArray:
    working = da_xy
    for dim in list(working.dims):
        if dim not in ("x", "y") and working.sizes[dim] == 1:
            working = working.isel({dim: 0}, drop=True)

    non_spatial = [d for d in working.dims if d not in ("x", "y")]

    for dim in list(non_spatial):
        if dim.lower() in _SLICE_LAST_DIMS and working.sizes[dim] > 1:
            working = working.isel({dim: -1}, drop=True)

    non_spatial = [d for d in working.dims if d not in ("x", "y")]
    if not non_spatial:
        return working.transpose("y", "x")

    if len(non_spatial) == 1 and non_spatial[0] == "band":
        return working.transpose("band", "y", "x")

    raise ValueError(
        f"Cannot export dims {non_spatial}. Slice to a single 2D layer "
        "(e.g. da.isel(time=-1)) or provide a single 'band' dimension."
    )


def _coerce_numeric_dtype(
    da: xr.DataArray, dtype: str | np.dtype | None
) -> tuple[xr.DataArray, np.dtype]:
    if dtype is not None:
        target = np.dtype(dtype)
    else:
        target = np.dtype(da.dtype)
        if target.kind == "b":
            target = np.dtype("uint8")
    if target.kind not in ("i", "u", "f"):
        raise TypeError(f"Unsupported dtype '{target}'. Use integer or float dtype.")
    if np.dtype(da.dtype) != target:
        da = da.astype(target)
    return da, target


def _from_attrs_nodata(da: xr.DataArray) -> Any:
    for key in ("_FillValue", "missing_value", "fill_value", "nodata"):
        if key in da.attrs:
            return da.attrs[key]
    return None


def _sanitize_nodata(value: Any, dtype: np.dtype) -> Any:
    if value is None:
        return None
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return None
        value = value.reshape(-1)[0]
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return None
    try:
        value_float = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(value_float):
        return None
    return np.array(value_float, dtype=dtype).item()


def _pick_nodata(da: xr.DataArray, dtype: np.dtype, explicit_nodata: Any) -> Any:
    value = explicit_nodata
    if value is None:
        value = da.rio.nodata
    if value is None:
        value = _from_attrs_nodata(da)
    value = _sanitize_nodata(value, dtype)
    if value is not None:
        return value

    if dtype.kind == "f":
        return -9999.0
    if dtype.kind == "u":
        return 0
    if dtype.kind == "i":
        return -9999
    raise TypeError(f"Unsupported dtype kind for nodata selection: {dtype.kind!r}")


def _drop_conflicting_fillvalue_metadata(da: xr.DataArray) -> xr.DataArray:
    cleaned = da.copy(deep=False)
    attrs = dict(cleaned.attrs)
    encoding = dict(cleaned.encoding)

    for key in ("_FillValue", "missing_value", "fill_value", "nodata"):
        attrs.pop(key, None)
        encoding.pop(key, None)

    cleaned.attrs = attrs
    cleaned.encoding = encoding
    return cleaned


def export_geotiff(
    da: xr.DataArray,
    path: str | Path,
    *,
    nodata: Any = None,
    dtype: str | np.dtype | None = None,
    compress: str = "deflate",
) -> Path:
    """
    Export a DataArray to GeoTIFF with robust spatial metadata and nodata handling.
    """
    normalized = normalize_spatial_da(da)
    normalized = _normalize_band_layout(normalized)
    normalized, target_dtype = _coerce_numeric_dtype(normalized, dtype)
    normalized = _drop_conflicting_fillvalue_metadata(normalized)

    nodata_value = _pick_nodata(normalized, target_dtype, nodata)
    normalized = normalized.rio.write_nodata(nodata_value, inplace=False)
    if normalized.rio.crs is None:
        raise ValueError("CRS missing before write. Call normalize_spatial_da with explicit CRS.")
    normalized = normalized.rio.write_crs(normalized.rio.crs, inplace=False)

    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    compress_name = (compress or "").upper()
    write_kwargs: dict[str, Any] = {
        "dtype": str(target_dtype),
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "IF_SAFER",
    }
    if compress_name:
        write_kwargs["compress"] = compress_name
        if compress_name in {"DEFLATE", "LZW", "ZSTD"}:
            write_kwargs["predictor"] = 3 if target_dtype.kind == "f" else 2

    normalized.rio.to_raster(out_path, **write_kwargs)
    return out_path

from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path

from qgis.core import (
    QgsField,
    QgsProject,
    QgsRasterLayer,
    QgsVectorLayer,
    QgsWkbTypes,
    edit,
)

try:
    from qgis.analysis import QgsZonalStatistics
except Exception as exc:
    raise RuntimeError("QGIS analysis module is required (QgsZonalStatistics).") from exc


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
EVOLUTION_DIR = Path(
    globals().get("EVOLUTION_DIR", str(BASE / "output" / "flood" / "water_evolution_2025_full_s1_10m_consistent"))
)
MASK_KIND = str(globals().get("MASK_KIND", "water")).strip().lower()  # water | overflow
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "01/2025")).strip()
TO_MMYYYY = str(globals().get("TO_MMYYYY", "12/2025")).strip()
PARCEL_LAYER_NAME = str(globals().get("PARCEL_LAYER_NAME", "")).strip()
ID_FIELD = str(globals().get("ID_FIELD", "")).strip()
OUT_DIR = Path(globals().get("OUT_DIR", str(BASE / "output" / "flood" / "parcel_stats_2025")))
OUT_CSV_LONG = str(globals().get("OUT_CSV_LONG", "parcel_monthly_water_stats_long.csv"))
OUT_CSV_WIDE = str(globals().get("OUT_CSV_WIDE", "parcel_monthly_water_stats_wide.csv"))
LOG_FILE = str(globals().get("LOG_FILE", "parcel_monthly_water_stats.log"))
LOAD_OUTPUT_LAYER = bool(globals().get("LOAD_OUTPUT_LAYER", False))


MASK_RE = re.compile(r"water_mask_(\d{4}-\d{2}-\d{2})\.tif$")
OVERFLOW_RE = re.compile(r"overflow_mask_(\d{4}-\d{2}-\d{2})\.tif$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _month_key(value: str) -> int:
    mm, yyyy = value.split("/")
    m = int(mm)
    y = int(yyyy)
    if m < 1 or m > 12:
        raise ValueError(f"Invalid MM/YYYY value: {value}")
    return y * 12 + m


def _date_to_month_key(yyyy_mm_dd: str) -> int:
    return int(yyyy_mm_dd[0:4]) * 12 + int(yyyy_mm_dd[5:7])


def _date_to_month_label(yyyy_mm_dd: str) -> str:
    return yyyy_mm_dd[0:7]


def _find_parcel_layer(name: str) -> QgsVectorLayer:
    layers = [lyr for lyr in QgsProject.instance().mapLayers().values() if isinstance(lyr, QgsVectorLayer)]
    if not layers:
        raise RuntimeError("No vector layers loaded. Load your parcel/KML polygon layer first.")

    if name:
        for lyr in layers:
            if lyr.name() == name:
                return lyr
        raise RuntimeError(f"Vector layer not found by name: {name}")

    active = iface.activeLayer()
    if isinstance(active, QgsVectorLayer):
        return active

    # Fallback: first polygon vector layer.
    for lyr in layers:
        if QgsWkbTypes.geometryType(lyr.wkbType()) == QgsWkbTypes.PolygonGeometry:
            return lyr
    raise RuntimeError("No polygon vector layer found. Select a polygon layer and try again.")


def _ensure_polygon_layer(layer: QgsVectorLayer) -> None:
    if QgsWkbTypes.geometryType(layer.wkbType()) != QgsWkbTypes.PolygonGeometry:
        raise RuntimeError(f"Layer '{layer.name()}' is not polygon geometry.")


def _choose_id_field(layer: QgsVectorLayer, requested: str) -> str:
    field_names = [f.name() for f in layer.fields()]
    if requested:
        if requested not in field_names:
            raise RuntimeError(f"ID_FIELD '{requested}' not found in layer '{layer.name()}'.")
        return requested

    preferred = [
        "id",
        "ID",
        "fid",
        "FID",
        "name",
        "Name",
        "nombre",
        "NOMBRE",
        "lote",
        "Lote",
        "parcel",
        "PARCEL_ID",
    ]
    for p in preferred:
        if p in field_names:
            return p

    # Fallback: first non-geometry field.
    if field_names:
        return field_names[0]
    raise RuntimeError(f"Layer '{layer.name()}' has no attributes. Add an ID field and retry.")


def _gather_masks(evolution_dir: Path, kind: str, from_mm: str, to_mm: str) -> list[tuple[str, Path]]:
    if kind == "overflow":
        masks_dir = evolution_dir / "overflow"
        rex = OVERFLOW_RE
    else:
        masks_dir = evolution_dir / "masks"
        rex = MASK_RE

    if not masks_dir.exists():
        raise RuntimeError(f"Masks directory not found: {masks_dir}")

    items: list[tuple[str, Path]] = []
    for p in sorted(masks_dir.glob("*.tif")):
        m = rex.match(p.name)
        if m:
            items.append((m.group(1), p))
    if not items:
        raise RuntimeError(f"No mask files found in {masks_dir}")

    start_key = _month_key(from_mm)
    end_key = _month_key(to_mm)
    if end_key < start_key:
        raise RuntimeError("TO_MMYYYY must be >= FROM_MMYYYY.")

    selected = []
    for date_label, p in items:
        mk = _date_to_month_key(date_label)
        if mk < start_key or mk > end_key:
            continue
        selected.append((date_label, p))
    if not selected:
        raise RuntimeError("No mask files after month filtering.")
    return sorted(selected, key=lambda t: t[0])


def _ensure_field(layer: QgsVectorLayer, field_name: str, field_type) -> None:
    if layer.fields().indexFromName(field_name) >= 0:
        return
    with edit(layer):
        layer.addAttribute(QgsField(field_name, field_type))
    layer.updateFields()


def _log_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    parcel = _find_parcel_layer(PARCEL_LAYER_NAME)
    _ensure_polygon_layer(parcel)
    id_field = _choose_id_field(parcel, ID_FIELD)

    masks = _gather_masks(EVOLUTION_DIR, MASK_KIND, FROM_MMYYYY, TO_MMYYYY)

    # Work on a temporary in-memory copy to avoid polluting your original layer schema.
    temp = QgsVectorLayer(parcel.source(), f"{parcel.name()}__stats_tmp", parcel.providerType())
    if not temp.isValid():
        # Fallback: clone selected features into memory provider if source-based clone fails.
        temp = QgsVectorLayer(f"{QgsWkbTypes.displayString(parcel.wkbType())}?crs={parcel.crs().authid()}", "stats_tmp", "memory")
        if not temp.isValid():
            raise RuntimeError("Could not create temporary layer for zonal statistics.")
        prov = temp.dataProvider()
        prov.addAttributes(parcel.fields())
        temp.updateFields()
        prov.addFeatures(list(parcel.getFeatures()))
        temp.updateExtents()

    # Ensure ID field exists in temp layer.
    if temp.fields().indexFromName(id_field) < 0:
        raise RuntimeError(f"ID field '{id_field}' not available in temporary layer.")

    # Zonal stats per month (binary masks: mean = wet_fraction, sum = wet_pixels, count = valid_pixels)
    month_to_fields: dict[str, dict[str, str]] = {}
    for date_label, raster_path in masks:
        month_label = _date_to_month_label(date_label)
        prefix = f"m{month_label.replace('-', '')}_"
        month_to_fields[month_label] = {
            "mean": f"{prefix}mean",
            "sum": f"{prefix}sum",
            "count": f"{prefix}count",
        }

        rlyr = QgsRasterLayer(str(raster_path), f"mask_{date_label}", "gdal")
        if not rlyr.isValid():
            raise RuntimeError(f"Invalid raster mask: {raster_path}")

        zs = QgsZonalStatistics(
            temp,
            rlyr,
            prefix,
            1,
            QgsZonalStatistics.Count | QgsZonalStatistics.Sum | QgsZonalStatistics.Mean,
        )
        rc = zs.calculateStatistics(None)
        if rc != 0:
            raise RuntimeError(f"Zonal statistics failed for {raster_path} (code={rc}).")

    # Build long-format output (one row = parcel x month).
    long_rows: list[dict[str, str]] = []
    for ft in temp.getFeatures():
        parcel_id = ft[id_field]
        for month_label in sorted(month_to_fields.keys()):
            flds = month_to_fields[month_label]
            wet_fraction = ft[flds["mean"]]
            wet_pixels = ft[flds["sum"]]
            valid_pixels = ft[flds["count"]]
            long_rows.append(
                {
                    "parcel_id": "" if parcel_id is None else str(parcel_id),
                    "month": month_label,
                    "wet_fraction": "" if wet_fraction is None else f"{float(wet_fraction):.6f}",
                    "wet_pixels": "" if wet_pixels is None else str(int(round(float(wet_pixels)))),
                    "valid_pixels": "" if valid_pixels is None else str(int(round(float(valid_pixels)))),
                }
            )

    # Wide-format output (one row = parcel, one column per month wet_fraction).
    wide_rows: list[dict[str, str]] = []
    month_cols = [f"wet_fraction_{m.replace('-', '_')}" for m in sorted(month_to_fields.keys())]
    for ft in temp.getFeatures():
        parcel_id = ft[id_field]
        row = {"parcel_id": "" if parcel_id is None else str(parcel_id)}
        for month_label in sorted(month_to_fields.keys()):
            flds = month_to_fields[month_label]
            wet_fraction = ft[flds["mean"]]
            row[f"wet_fraction_{month_label.replace('-', '_')}"] = (
                "" if wet_fraction is None else f"{float(wet_fraction):.6f}"
            )
        wide_rows.append(row)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_long = OUT_DIR / OUT_CSV_LONG
    csv_wide = OUT_DIR / OUT_CSV_WIDE
    log_path = OUT_DIR / LOG_FILE

    _write_csv(csv_long, long_rows, ["parcel_id", "month", "wet_fraction", "wet_pixels", "valid_pixels"])
    _write_csv(csv_wide, wide_rows, ["parcel_id", *month_cols])

    lines = []
    lines.append("Parcel monthly water statistics export")
    lines.append(f"UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"parcel_layer: {parcel.name()}")
    lines.append(f"id_field: {id_field}")
    lines.append(f"evolution_dir: {EVOLUTION_DIR}")
    lines.append(f"mask_kind: {MASK_KIND}")
    lines.append(f"month_range: {FROM_MMYYYY} -> {TO_MMYYYY}")
    lines.append(f"months_processed: {len(month_to_fields)}")
    lines.append("months_list: " + ", ".join(sorted(month_to_fields.keys())))
    lines.append(f"rows_long: {len(long_rows)}")
    lines.append(f"rows_wide: {len(wide_rows)}")
    lines.append(f"csv_long: {csv_long}")
    lines.append(f"csv_wide: {csv_wide}")
    _log_lines(log_path, lines)

    if LOAD_OUTPUT_LAYER:
        uri = f"file:///{csv_long.as_posix()}?type=csv&detectTypes=yes&geomType=none"
        csv_layer = QgsVectorLayer(uri, "parcel_monthly_water_stats_long", "delimitedtext")
        if csv_layer.isValid():
            QgsProject.instance().addMapLayer(csv_layer)

    print(f"Done. Long CSV: {csv_long}")
    print(f"Done. Wide CSV: {csv_wide}")
    print(f"Log: {log_path}")


main()

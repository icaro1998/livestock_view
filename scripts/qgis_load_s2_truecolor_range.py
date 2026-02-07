from pathlib import Path

from qgis.core import (
    QgsContrastEnhancement,
    QgsCoordinateReferenceSystem,
    QgsMultiBandColorRenderer,
    QgsProject,
    QgsRasterLayer,
    QgsRasterRange,
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "01/2025"))  # MM/YYYY
TO_MMYYYY = str(globals().get("TO_MMYYYY", "12/2025"))  # MM/YYYY
S2_DIR = Path(globals().get("S2_DIR", str(BASE / "output" / "sentinel2_truecolor_best_10km_2025")))
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
SHOW_ONLY_LAST_MONTH = bool(globals().get("SHOW_ONLY_LAST_MONTH", True))
ZOOM_TO_RESULT = bool(globals().get("ZOOM_TO_RESULT", True))
GROUP_NAME = str(globals().get("GROUP_NAME", f"S2 TrueColor {FROM_MMYYYY} to {TO_MMYYYY}"))


def _parse_month(value: str) -> tuple[int, int]:
    parts = value.strip().split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid MM/YYYY value: {value!r}")
    month = int(parts[0])
    year = int(parts[1])
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in MM/YYYY value: {value!r}")
    return year, month


def _month_key(year: int, month: int) -> int:
    return year * 12 + month


def _iter_months(start_y: int, start_m: int, end_y: int, end_m: int):
    y, m = start_y, start_m
    end_key = _month_key(end_y, end_m)
    while _month_key(y, m) <= end_key:
        yield y, m
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def _style_truecolor(layer: QgsRasterLayer) -> None:
    provider = layer.dataProvider()
    try:
        for band in (1, 2, 3):
            provider.setUserNoDataValue(band, [QgsRasterRange(0.0, 0.0)])
    except Exception:
        pass

    renderer = QgsMultiBandColorRenderer(provider, 1, 2, 3)
    for band, set_ce in (
        (1, renderer.setRedContrastEnhancement),
        (2, renderer.setGreenContrastEnhancement),
        (3, renderer.setBlueContrastEnhancement),
    ):
        try:
            dtype = provider.dataType(band)
            ce = QgsContrastEnhancement(dtype)
            ce.setContrastEnhancementAlgorithm(QgsContrastEnhancement.StretchToMinimumMaximum, True)
            ce.setMinimumValue(300.0)
            ce.setMaximumValue(3500.0)
            set_ce(ce)
        except Exception:
            pass

    layer.setRenderer(renderer)
    renderer.setOpacity(1.0)
    layer.triggerRepaint()


def main() -> None:
    fy, fm = _parse_month(FROM_MMYYYY)
    ty, tm = _parse_month(TO_MMYYYY)
    if _month_key(ty, tm) < _month_key(fy, fm):
        raise ValueError("TO_MMYYYY must be after FROM_MMYYYY")
    if not S2_DIR.exists():
        raise FileNotFoundError(f"S2_DIR does not exist: {S2_DIR}")

    project = QgsProject.instance()
    if CLEAR_PROJECT:
        project.removeAllMapLayers()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

    root = project.layerTreeRoot()
    existing = root.findGroup(GROUP_NAME)
    if existing is not None:
        root.removeChildNode(existing)
    group = root.addGroup(GROUP_NAME)

    canvas = iface.mapCanvas()
    prev_render_flag = canvas.renderFlag()
    canvas.setRenderFlag(False)

    loaded_groups = []
    failures = []
    last_extent = None
    try:
        for year, month in _iter_months(fy, fm, ty, tm):
            ym = f"{year:04d}-{month:02d}"
            tif = S2_DIR / f"s2_truecolor_{ym}.tif"
            if not tif.exists():
                failures.append(f"{ym}: missing {tif.name}")
                continue

            mg = group.addGroup(ym)
            try:
                layer = QgsRasterLayer(str(tif), f"S2 TrueColor {ym}", "gdal")
                if not layer.isValid():
                    raise RuntimeError("Invalid raster layer")
                project.addMapLayer(layer, False)
                mg.addLayer(layer)
                _style_truecolor(layer)
                last_extent = layer.extent()
                loaded_groups.append(mg)
                print(f"Loaded {ym}")
            except Exception as exc:
                failures.append(f"{ym}: {exc}")
                group.removeChildNode(mg)

        if not loaded_groups:
            raise RuntimeError("No month could be loaded.")

        if SHOW_ONLY_LAST_MONTH:
            for mg in loaded_groups[:-1]:
                mg.setItemVisibilityChecked(False)
            loaded_groups[-1].setItemVisibilityChecked(True)

        if ZOOM_TO_RESULT and last_extent is not None:
            canvas.setExtent(last_extent)
    finally:
        canvas.setRenderFlag(prev_render_flag)
        canvas.refresh()

    print(f"Loaded month groups: {len(loaded_groups)}")
    print(f"Group: {GROUP_NAME}")
    if failures:
        print("Missing/failed:")
        for line in failures:
            print(f"  - {line}")


main()

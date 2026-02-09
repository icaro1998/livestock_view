from __future__ import annotations

from pathlib import Path

from qgis.PyQt.QtCore import QTimer
from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsColorRampShader,
    QgsContrastEnhancement,
    QgsCoordinateReferenceSystem,
    QgsMultiBandColorRenderer,
    QgsProject,
    QgsRasterLayer,
    QgsRasterRange,
    QgsRasterShader,
    QgsSingleBandGrayRenderer,
    QgsSingleBandPseudoColorRenderer,
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")

# Time window
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "01/2025"))  # MM/YYYY
TO_MMYYYY = str(globals().get("TO_MMYYYY", "12/2025"))  # MM/YYYY

# General behavior
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
ZOOM_TO_RESULT = bool(globals().get("ZOOM_TO_RESULT", True))
SHOW_ONLY_LAST_MONTH = bool(globals().get("SHOW_ONLY_LAST_MONTH", True))
GROUP_NAME = str(globals().get("GROUP_NAME", "Hydrology Realistic 30km"))
AUTO_START_ANIMATION = bool(globals().get("AUTO_START_ANIMATION", False))
ANIMATION_MS = int(globals().get("ANIMATION_MS", 800))

# Data roots
TOPO_ROOT = Path(globals().get("TOPO_ROOT", str(BASE / "output" / "flood_30km")))
ADDITIONAL_ROOT = Path(globals().get("ADDITIONAL_ROOT", str(BASE / "output" / "flood" / "additional_30km_2025")))
S2_TRUECOLOR_DIR = Path(
    globals().get("S2_TRUECOLOR_DIR", str(BASE / "output" / "sentinel2_truecolor_best_30km_2025"))
)

# Optional layers
INCLUDE_S3_NDWI = bool(globals().get("INCLUDE_S3_NDWI", False))
INCLUDE_DW_MONTHLY = bool(globals().get("INCLUDE_DW_MONTHLY", True))

# Visual settings
TOPO_OPACITY = float(globals().get("TOPO_OPACITY", 1.0))
S2_OPACITY = float(globals().get("S2_OPACITY", 0.95))
PERMANENT_OPACITY = float(globals().get("PERMANENT_OPACITY", 0.72))
STREAMS_OPACITY = float(globals().get("STREAMS_OPACITY", 0.62))
DW_MONTHLY_OPACITY = float(globals().get("DW_MONTHLY_OPACITY", 0.45))
S1_DIFF_OPACITY = float(globals().get("S1_DIFF_OPACITY", 0.72))
S3_OPACITY = float(globals().get("S3_OPACITY", 0.45))
PERMANENT_OCC_MIN = float(globals().get("PERMANENT_OCC_MIN", 80.0))
STREAM_OCC_MIN = float(globals().get("STREAM_OCC_MIN", 5.0))
STREAM_OCC_MAX = float(globals().get("STREAM_OCC_MAX", 60.0))


def _get_canvas():
    try:
        from qgis.utils import iface as qgis_iface
    except Exception:
        return None
    if qgis_iface is None:
        return None
    try:
        return qgis_iface.mapCanvas()
    except Exception:
        return None


def _parse_month(value: str) -> tuple[int, int]:
    parts = value.strip().split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid MM/YYYY value: {value!r}")
    month = int(parts[0])
    year = int(parts[1])
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in MM/YYYY: {value!r}")
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


def _pick_latest(glob_pattern: str) -> Path | None:
    candidates = sorted(BASE.glob(glob_pattern))
    if not candidates:
        return None
    return candidates[-1]


def _set_singleband_style(layer: QgsRasterLayer, items: list[QgsColorRampShader.ColorRampItem], opacity: float) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(items)
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _style_hillshade_bw(layer: QgsRasterLayer, opacity: float) -> None:
    renderer = QgsSingleBandGrayRenderer(layer.dataProvider(), 1)
    renderer.setOpacity(opacity)
    dtype = layer.dataProvider().dataType(1)
    ce = QgsContrastEnhancement(dtype)
    ce.setContrastEnhancementAlgorithm(QgsContrastEnhancement.StretchToMinimumMaximum, True)
    ce.setMinimumValue(0.0)
    ce.setMaximumValue(255.0)
    renderer.setContrastEnhancement(ce)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _style_truecolor(layer: QgsRasterLayer, opacity: float) -> None:
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
    renderer.setOpacity(opacity)
    layer.triggerRepaint()


def _style_permanent_water_occurrence(layer: QgsRasterLayer, opacity: float) -> None:
    # Emphasize stable/permanent water: high occurrence values.
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "0"),
            QgsColorRampShader.ColorRampItem(max(0.0, PERMANENT_OCC_MIN - 1.0), QColor(255, 255, 255, 0), "low"),
            QgsColorRampShader.ColorRampItem(PERMANENT_OCC_MIN, QColor("#4fa3ff"), "perm_min"),
            QgsColorRampShader.ColorRampItem(100.0, QColor("#08306b"), "100"),
        ],
        opacity=opacity,
    )


def _style_stream_emphasis_occurrence(layer: QgsRasterLayer, opacity: float) -> None:
    # Emphasize lower-to-mid occurrence where minor channels are often visible.
    lo = max(0.0, min(100.0, STREAM_OCC_MIN))
    hi = max(lo, min(100.0, STREAM_OCC_MAX))
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "0"),
            QgsColorRampShader.ColorRampItem(max(0.0, lo - 1.0), QColor(255, 255, 255, 0), "below"),
            QgsColorRampShader.ColorRampItem(lo, QColor("#c7f9ff"), "stream_min"),
            QgsColorRampShader.ColorRampItem(min(hi, lo + 15.0), QColor("#7dd3fc"), "mid"),
            QgsColorRampShader.ColorRampItem(max(lo, hi - 10.0), QColor("#38bdf8"), "high"),
            QgsColorRampShader.ColorRampItem(hi, QColor(255, 255, 255, 0), "stream_max"),
            QgsColorRampShader.ColorRampItem(100.0, QColor(255, 255, 255, 0), "100"),
        ],
        opacity=opacity,
    )


def _style_dw_monthly(layer: QgsRasterLayer, opacity: float) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(0.00, QColor(255, 255, 255, 0), "0"),
            QgsColorRampShader.ColorRampItem(0.05, QColor(198, 242, 255, 40), "0.05"),
            QgsColorRampShader.ColorRampItem(0.20, QColor(125, 211, 252, 95), "0.20"),
            QgsColorRampShader.ColorRampItem(0.40, QColor(56, 189, 248, 145), "0.40"),
            QgsColorRampShader.ColorRampItem(0.70, QColor(14, 116, 144, 205), "0.70"),
            QgsColorRampShader.ColorRampItem(1.00, QColor(8, 69, 148, 255), "1.00"),
        ],
        opacity=opacity,
    )


def _style_s3_ndwi(layer: QgsRasterLayer, opacity: float) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(-1.0, QColor(255, 255, 255, 0), "-1"),
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "0"),
            QgsColorRampShader.ColorRampItem(0.05, QColor("#d7f7f2"), "0.05"),
            QgsColorRampShader.ColorRampItem(0.20, QColor("#7ddfd3"), "0.20"),
            QgsColorRampShader.ColorRampItem(0.50, QColor("#006d63"), "0.50"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#004d40"), "1"),
        ],
        opacity=opacity,
    )


def _style_s1_flood_diff(layer: QgsRasterLayer, opacity: float) -> None:
    # Increase water (positive) = cyan/blue. Decrease water (negative) = orange/red.
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(-3.0, QColor("#f46d43"), "-3"),
            QgsColorRampShader.ColorRampItem(-1.0, QColor("#fdae61"), "-1"),
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 15), "0"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#7fd3ff"), "1"),
            QgsColorRampShader.ColorRampItem(3.0, QColor("#00e5ff"), "3"),
        ],
        opacity=opacity,
    )


def _add_raster(project: QgsProject, group, path: Path, name: str) -> QgsRasterLayer:
    layer = QgsRasterLayer(str(path), name, "gdal")
    if not layer.isValid():
        raise RuntimeError(f"Invalid raster: {path}")
    project.addMapLayer(layer, False)
    group.addLayer(layer)
    return layer


def stop_realistic_animation() -> None:
    state = globals().get("_REALISTIC_30KM_ANIM_STATE")
    if not state:
        return
    timer = state.get("timer")
    if timer is not None:
        timer.stop()
        timer.deleteLater()
    state["timer"] = None
    print("Realistic 30km animation stopped.")


def start_realistic_animation(interval_ms: int = 800, loop: bool = True) -> None:
    state = globals().get("_REALISTIC_30KM_ANIM_STATE")
    if not state or not state.get("months"):
        print("No monthly layers loaded for animation.")
        return

    stop_realistic_animation()
    root = QgsProject.instance().layerTreeRoot()
    canvas = _get_canvas()
    iface_obj = None
    try:
        from qgis.utils import iface as iface_obj  # type: ignore
    except Exception:
        iface_obj = None

    def _step() -> None:
        months = state["months"]
        idx = int(state.get("idx", 0)) % len(months)
        ym = months[idx]

        for m in months:
            for lyr in state["color_layers"].get(m, []):
                node = root.findLayer(lyr.id())
                if node is not None:
                    node.setItemVisibilityChecked(m == ym)
            for lyr in state["temporal_layers"].get(m, []):
                node = root.findLayer(lyr.id())
                if node is not None:
                    node.setItemVisibilityChecked(m == ym)

        if iface_obj is not None:
            try:
                iface_obj.mainWindow().statusBar().showMessage(f"Hydrology month: {ym}", 900)
            except Exception:
                pass
        if canvas is not None:
            canvas.refresh()

        if idx >= len(months) - 1:
            if loop:
                state["idx"] = 0
            else:
                stop_realistic_animation()
        else:
            state["idx"] = idx + 1

    timer = QTimer()
    timer.timeout.connect(_step)
    state["timer"] = timer
    _step()
    timer.start(max(120, int(interval_ms)))
    print(f"Realistic 30km animation started ({len(state['months'])} months, {interval_ms} ms/frame).")


def main() -> None:
    fy, fm = _parse_month(FROM_MMYYYY)
    ty, tm = _parse_month(TO_MMYYYY)
    if _month_key(ty, tm) < _month_key(fy, fm):
        raise ValueError("TO_MMYYYY must be after FROM_MMYYYY")

    project = QgsProject.instance()
    if CLEAR_PROJECT:
        project.removeAllMapLayers()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

    root = project.layerTreeRoot()
    old = root.findGroup(GROUP_NAME)
    if old is not None:
        root.removeChildNode(old)
    master = root.addGroup(GROUP_NAME)

    g_topo = master.addGroup("01 Topography")
    g_color = master.addGroup("02 Color")
    g_water = master.addGroup("03 Water")
    g_perm = g_water.addGroup("03.1 Permanent Water + Streams")
    g_temp = g_water.addGroup("03.2 Temporal Rise/Fall")

    last_extent = None

    hillshade = TOPO_ROOT / "terrain_hillshade.tif"
    if hillshade.exists():
        lyr_h = _add_raster(project, g_topo, hillshade, "Topography hillshade")
        _style_hillshade_bw(lyr_h, TOPO_OPACITY)
        last_extent = lyr_h.extent()
    else:
        print(f"Warning: missing hillshade: {hillshade}")

    occurrence = TOPO_ROOT / "surface_water_occurrence.tif"
    if occurrence.exists():
        lyr_perm = _add_raster(project, g_perm, occurrence, "Permanent water (JRC occurrence >=80%)")
        _style_permanent_water_occurrence(lyr_perm, PERMANENT_OPACITY)
        lyr_stream = _add_raster(project, g_perm, occurrence, "Minor channels emphasis (JRC occurrence 5-60)")
        _style_stream_emphasis_occurrence(lyr_stream, STREAMS_OPACITY)
        last_extent = last_extent or lyr_perm.extent()
    else:
        print(f"Warning: missing surface water occurrence: {occurrence}")

    color_layers: dict[str, list[QgsRasterLayer]] = {}
    temporal_layers: dict[str, list[QgsRasterLayer]] = {}
    loaded_months: list[str] = []
    failures: list[str] = []

    for y, m in _iter_months(fy, fm, ty, tm):
        ym = f"{y:04d}-{m:02d}"
        per_month_color: list[QgsRasterLayer] = []
        per_month_temp: list[QgsRasterLayer] = []

        s2_path = S2_TRUECOLOR_DIR / f"s2_truecolor_{ym}.tif"
        s1_path = _pick_latest(f"output/flood_30km/s1_flood_diff_{ym}-*.tif")
        dw_path = ADDITIONAL_ROOT / "dynamicworld" / f"dw_water_prob_{ym}.tif"
        s3_path = ADDITIONAL_ROOT / "s3_olci" / f"s3_ndwi_{ym}.tif"

        if s2_path.exists():
            lyr_s2 = _add_raster(project, g_color, s2_path, f"S2 TrueColor {ym}")
            _style_truecolor(lyr_s2, S2_OPACITY)
            per_month_color.append(lyr_s2)
            last_extent = last_extent or lyr_s2.extent()

        if INCLUDE_S3_NDWI and s3_path.exists():
            lyr_s3 = _add_raster(project, g_color, s3_path, f"S3 NDWI {ym}")
            _style_s3_ndwi(lyr_s3, S3_OPACITY)
            per_month_color.append(lyr_s3)
            last_extent = last_extent or lyr_s3.extent()

        if INCLUDE_DW_MONTHLY and dw_path.exists():
            lyr_dw = _add_raster(project, g_temp, dw_path, f"DW water prob {ym}")
            _style_dw_monthly(lyr_dw, DW_MONTHLY_OPACITY)
            per_month_temp.append(lyr_dw)
            last_extent = last_extent or lyr_dw.extent()

        if s1_path is not None:
            lyr_s1 = _add_raster(project, g_temp, s1_path, f"S1 flood diff {ym}")
            _style_s1_flood_diff(lyr_s1, S1_DIFF_OPACITY)
            per_month_temp.append(lyr_s1)
            last_extent = last_extent or lyr_s1.extent()

        if per_month_color or per_month_temp:
            loaded_months.append(ym)
            color_layers[ym] = per_month_color
            temporal_layers[ym] = per_month_temp
        else:
            failures.append(f"{ym}: no monthly layers found")

    if not loaded_months:
        raise RuntimeError("No monthly layers loaded for selected range.")

    show_month = loaded_months[-1] if SHOW_ONLY_LAST_MONTH else loaded_months[0]
    for ym in loaded_months:
        visible = ym == show_month
        for lyr in color_layers.get(ym, []):
            node = master.findLayer(lyr.id())
            if node is not None:
                node.setItemVisibilityChecked(visible)
        for lyr in temporal_layers.get(ym, []):
            node = master.findLayer(lyr.id())
            if node is not None:
                node.setItemVisibilityChecked(visible)

    canvas = _get_canvas()
    if canvas is not None and ZOOM_TO_RESULT and last_extent is not None:
        canvas.setExtent(last_extent)
        canvas.refresh()
    elif canvas is None:
        print("Info: no interactive canvas (iface). Layers added without map zoom.")

    globals()["_REALISTIC_30KM_ANIM_STATE"] = {
        "months": loaded_months,
        "color_layers": color_layers,
        "temporal_layers": temporal_layers,
        "idx": max(0, loaded_months.index(show_month)),
        "timer": None,
    }

    print(f"Group: {GROUP_NAME}")
    print(f"Topography root: {TOPO_ROOT}")
    print(f"Additional root: {ADDITIONAL_ROOT}")
    print(f"S2 truecolor dir: {S2_TRUECOLOR_DIR}")
    print(f"Loaded months: {len(loaded_months)} ({loaded_months[0]} -> {loaded_months[-1]})")
    print(f"Permanent occurrence min: {PERMANENT_OCC_MIN}")
    print(f"Stream occurrence window: {STREAM_OCC_MIN}..{STREAM_OCC_MAX}")
    if failures:
        print("Missing months/layers:")
        for line in failures:
            print(f"  - {line}")
    print("Temporal controls:")
    print("  - start_realistic_animation(interval_ms=800, loop=True)")
    print("  - stop_realistic_animation()")
    if AUTO_START_ANIMATION and canvas is not None:
        start_realistic_animation(interval_ms=ANIMATION_MS, loop=True)


main()

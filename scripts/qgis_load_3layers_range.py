from pathlib import Path

from qgis.core import (
    QgsContrastEnhancement,
    QgsColorRampShader,
    QgsCoordinateReferenceSystem,
    QgsMultiBandColorRenderer,
    QgsProject,
    QgsRasterLayer,
    QgsRasterRange,
    QgsRasterShader,
    QgsSingleBandPseudoColorRenderer,
)
from qgis.PyQt.QtCore import QTimer
from qgis.PyQt.QtGui import QColor


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "01/2025"))  # MM/YYYY
TO_MMYYYY = str(globals().get("TO_MMYYYY", "12/2025"))  # MM/YYYY
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
ADD_TRUECOLOR_BASE = bool(globals().get("ADD_TRUECOLOR_BASE", True))
SHOW_ONLY_LAST_MONTH = bool(globals().get("SHOW_ONLY_LAST_MONTH", True))
ZOOM_TO_RESULT = bool(globals().get("ZOOM_TO_RESULT", True))
GROUP_NAME = str(globals().get("GROUP_NAME", f"Flood 3-layer {FROM_MMYYYY} to {TO_MMYYYY}"))
ADDITIONAL_DIR = str(globals().get("ADDITIONAL_DIR", "output/flood/additional_30km_2025"))
INCLUDE_S3_NDWI = bool(globals().get("INCLUDE_S3_NDWI", True))
SAR_RENDER_MODE = str(globals().get("SAR_RENDER_MODE", "auto")).strip().lower()
AUTO_START_ANIMATION = bool(globals().get("AUTO_START_ANIMATION", False))
ANIMATION_MS = int(globals().get("ANIMATION_MS", 800))
SAR_MASK_GLOB_EXPRS = globals().get(
    "SAR_MASK_GLOB_EXPRS",
    [
        "output/flood_30km/s1_flood_diff_{ym}-*.tif",
        "output/flood/snapshots/s1_flood_diff_{ym}-*.tif",
        "output/flood/water_evolution_2025_full_s1_10m_consistent/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_2025_jan_mar_s1_10m/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_2025_apr_jun_s1_10m/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_2025_jul_s1_10m/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_2025_aug_oct_s1_10m/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_2025_nov_dec_s1_10m/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_10km_2025/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_2024_2025/masks/water_mask_{ym}-*.tif",
        "output/flood/water_evolution_wide_2024_2025/masks/water_mask_{ym}-*.tif",
    ],
)


def _resolve_additional_dir(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else BASE / path


DATA_ROOT = _resolve_additional_dir(ADDITIONAL_DIR)


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


def _pick_one(glob_exprs: list[str]) -> Path | None:
    for expr in glob_exprs:
        candidates = sorted(BASE.glob(expr))
        if candidates:
            return candidates[-1]
    return None


def _make_color(hex_code: str, alpha: int = 255) -> QColor:
    c = QColor(hex_code)
    c.setAlpha(alpha)
    return c


def _set_singleband_style(layer: QgsRasterLayer, items: list[QgsColorRampShader.ColorRampItem], opacity: float) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(items)
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


def _style_sar_mask(layer: QgsRasterLayer) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(0.0, _make_color("#000000", 0), "dry"),
            QgsColorRampShader.ColorRampItem(0.49, _make_color("#000000", 0), "dry"),
            QgsColorRampShader.ColorRampItem(0.50, _make_color("#6dd3ff", 170), "wet"),
            QgsColorRampShader.ColorRampItem(1.00, _make_color("#005f99", 255), "wet"),
        ],
        opacity=0.88,
    )


def _style_sar_flood_diff(layer: QgsRasterLayer) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(-3.0, _make_color("#f46d43", 235), "loss"),
            QgsColorRampShader.ColorRampItem(-1.0, _make_color("#fdae61", 200), "loss"),
            QgsColorRampShader.ColorRampItem(0.0, _make_color("#f7f7f7", 35), "stable"),
            QgsColorRampShader.ColorRampItem(1.0, _make_color("#7fd3ff", 200), "gain"),
            QgsColorRampShader.ColorRampItem(3.0, _make_color("#00e5ff", 235), "gain"),
        ],
        opacity=0.80,
    )


def _resolve_sar_mode(path: Path) -> str:
    mode = SAR_RENDER_MODE
    if mode in {"mask", "flood_diff"}:
        return mode
    if "s1_flood_diff" in path.name.lower():
        return "flood_diff"
    return "mask"


def _style_dw_prob(layer: QgsRasterLayer) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(0.00, _make_color("#deebf7", 0), "0"),
            QgsColorRampShader.ColorRampItem(0.10, _make_color("#c6dbef", 45), "0.1"),
            QgsColorRampShader.ColorRampItem(0.20, _make_color("#9ecae1", 80), "0.2"),
            QgsColorRampShader.ColorRampItem(0.40, _make_color("#6baed6", 130), "0.4"),
            QgsColorRampShader.ColorRampItem(0.60, _make_color("#4292c6", 185), "0.6"),
            QgsColorRampShader.ColorRampItem(0.80, _make_color("#2171b5", 220), "0.8"),
            QgsColorRampShader.ColorRampItem(1.00, _make_color("#084594", 255), "1"),
        ],
        opacity=0.72,
    )


def _style_s2_ndwi(layer: QgsRasterLayer) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(-1.00, _make_color("#000000", 0), "-1"),
            QgsColorRampShader.ColorRampItem(0.00, _make_color("#000000", 0), "0"),
            QgsColorRampShader.ColorRampItem(0.05, _make_color("#d0f0ff", 60), "0.05"),
            QgsColorRampShader.ColorRampItem(0.15, _make_color("#7fc8f8", 115), "0.15"),
            QgsColorRampShader.ColorRampItem(0.30, _make_color("#2b8cbe", 175), "0.3"),
            QgsColorRampShader.ColorRampItem(0.50, _make_color("#045a8d", 230), "0.5"),
            QgsColorRampShader.ColorRampItem(1.00, _make_color("#023858", 255), "1"),
        ],
        opacity=0.58,
    )


def _style_s3_ndwi(layer: QgsRasterLayer) -> None:
    _set_singleband_style(
        layer,
        [
            QgsColorRampShader.ColorRampItem(-1.00, _make_color("#000000", 0), "-1"),
            QgsColorRampShader.ColorRampItem(0.00, _make_color("#000000", 0), "0"),
            QgsColorRampShader.ColorRampItem(0.05, _make_color("#d7f7f2", 70), "0.05"),
            QgsColorRampShader.ColorRampItem(0.15, _make_color("#7ddfd3", 130), "0.15"),
            QgsColorRampShader.ColorRampItem(0.30, _make_color("#2aa198", 180), "0.3"),
            QgsColorRampShader.ColorRampItem(0.50, _make_color("#006d63", 230), "0.5"),
            QgsColorRampShader.ColorRampItem(1.00, _make_color("#004d40", 255), "1"),
        ],
        opacity=0.50,
    )


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


def _add_layer(project: QgsProject, group, path: Path, name: str, styler):
    layer = QgsRasterLayer(str(path), name, "gdal")
    if not layer.isValid():
        raise RuntimeError(f"Invalid raster: {path}")
    project.addMapLayer(layer, False)
    group.addLayer(layer)
    styler(layer)
    layer.triggerRepaint()
    return layer


def stop_flood3_range_animation() -> None:
    state = globals().get("_FLOOD3_RANGE_ANIM_STATE")
    if not state:
        return
    timer = state.get("timer")
    if timer is not None:
        timer.stop()
        timer.deleteLater()
    state["timer"] = None
    print("Flood 3-layer animation stopped.")


def start_flood3_range_animation(interval_ms: int = 800, loop: bool = True) -> None:
    state = globals().get("_FLOOD3_RANGE_ANIM_STATE")
    if not state or not state.get("groups"):
        print("No month groups loaded for animation.")
        return

    stop_flood3_range_animation()
    root = QgsProject.instance().layerTreeRoot()
    canvas = _get_canvas()
    iface_obj = None
    try:
        from qgis.utils import iface as iface_obj  # type: ignore
    except Exception:
        iface_obj = None

    def _step() -> None:
        groups = state.get("groups", [])
        labels = state.get("labels", [])
        alive_groups = []
        alive_labels = []
        for i, grp in enumerate(groups):
            try:
                node = root.findGroup(grp.name())
            except Exception:
                node = None
            if node is None:
                continue
            alive_groups.append(node)
            alive_labels.append(labels[i] if i < len(labels) else grp.name())

        if not alive_groups:
            stop_flood3_range_animation()
            return

        state["groups"] = alive_groups
        state["labels"] = alive_labels
        idx = int(state.get("idx", 0)) % len(alive_groups)
        for i, grp in enumerate(alive_groups):
            grp.setItemVisibilityChecked(i == idx)

        if iface_obj is not None:
            try:
                iface_obj.mainWindow().statusBar().showMessage(f"Flood month: {alive_labels[idx]}", 900)
            except Exception:
                pass
        if canvas is not None:
            canvas.refresh()

        if idx >= len(alive_groups) - 1:
            if loop:
                state["idx"] = 0
            else:
                stop_flood3_range_animation()
        else:
            state["idx"] = idx + 1

    timer = QTimer()
    timer.timeout.connect(_step)
    state["timer"] = timer
    _step()
    timer.start(max(120, int(interval_ms)))
    print(f"Flood 3-layer animation started ({len(state['groups'])} months, {interval_ms} ms/frame).")


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
    existing = root.findGroup(GROUP_NAME)
    if existing is not None:
        root.removeChildNode(existing)
    group = root.addGroup(GROUP_NAME)

    canvas = _get_canvas()
    prev_render_flag = None
    if canvas is not None:
        prev_render_flag = canvas.renderFlag()
        canvas.setRenderFlag(False)

    loaded_month_groups = []
    failures = []
    last_extent = None
    try:
        for year, month in _iter_months(fy, fm, ty, tm):
            ym = f"{year:04d}-{month:02d}"

            sar = _pick_one([expr.format(ym=ym) for expr in SAR_MASK_GLOB_EXPRS])
            dw = DATA_ROOT / "dynamicworld" / f"dw_water_prob_{ym}.tif"
            ndwi = DATA_ROOT / "sentinel2_sr_harmonized" / f"s2_ndwi_{ym}.tif"
            s3_ndwi = DATA_ROOT / "s3_olci" / f"s3_ndwi_{ym}.tif"
            rgb = DATA_ROOT / "sentinel2_truecolor" / f"s2_truecolor_{ym}.tif"

            has_any_core = (sar is not None) or dw.exists() or ndwi.exists() or (INCLUDE_S3_NDWI and s3_ndwi.exists())
            if not has_any_core:
                failures.append(f"{ym}: missing all core layers (SAR, DW, S2, S3)")
                continue

            mg = group.addGroup(ym)
            try:
                loaded_this_month = 0
                if ADD_TRUECOLOR_BASE and rgb.exists():
                    try:
                        _add_layer(project, mg, rgb, f"S2 TrueColor {ym}", _style_truecolor)
                        loaded_this_month += 1
                    except Exception as exc:
                        print(f"Warning [{ym}]: TrueColor style/load failed ({exc}). Continuing.")

                if ndwi.exists():
                    lyr_s2 = _add_layer(project, mg, ndwi, f"S2 NDWI {ym}", _style_s2_ndwi)
                    loaded_this_month += 1
                    last_extent = lyr_s2.extent()
                if INCLUDE_S3_NDWI and s3_ndwi.exists():
                    lyr_s3 = _add_layer(project, mg, s3_ndwi, f"S3 NDWI {ym}", _style_s3_ndwi)
                    loaded_this_month += 1
                    last_extent = lyr_s3.extent()
                if dw.exists():
                    lyr_dw = _add_layer(project, mg, dw, f"DW Water Prob {ym}", _style_dw_prob)
                    loaded_this_month += 1
                    last_extent = lyr_dw.extent()
                if sar is not None:
                    sar_mode = _resolve_sar_mode(sar)
                    sar_styler = _style_sar_flood_diff if sar_mode == "flood_diff" else _style_sar_mask
                    sar_label = "S1 Flood Diff" if sar_mode == "flood_diff" else "SAR Water Mask"
                    lyr_sar = _add_layer(project, mg, sar, f"{sar_label} {ym}", sar_styler)
                    loaded_this_month += 1
                    last_extent = lyr_sar.extent()

                if loaded_this_month == 0:
                    failures.append(f"{ym}: no usable layers")
                    group.removeChildNode(mg)
                    continue

                loaded_month_groups.append(mg)
                missing_parts = []
                if sar is None:
                    missing_parts.append("SAR")
                if not dw.exists():
                    missing_parts.append("DW")
                if not ndwi.exists():
                    missing_parts.append("S2")
                if INCLUDE_S3_NDWI and not s3_ndwi.exists():
                    missing_parts.append("S3")
                if missing_parts:
                    print(f"Loaded month {ym} (missing: {', '.join(missing_parts)})")
                else:
                    print(f"Loaded month {ym}")
            except Exception as exc:
                failures.append(f"{ym}: {exc}")
                group.removeChildNode(mg)

        if not loaded_month_groups:
            raise RuntimeError("No month could be loaded.")

        if SHOW_ONLY_LAST_MONTH:
            for mg in loaded_month_groups[:-1]:
                mg.setItemVisibilityChecked(False)
            loaded_month_groups[-1].setItemVisibilityChecked(True)

        if canvas is not None and ZOOM_TO_RESULT and last_extent is not None:
            canvas.setExtent(last_extent)
    finally:
        if canvas is not None and prev_render_flag is not None:
            canvas.setRenderFlag(prev_render_flag)
            canvas.refresh()

    print(f"Loaded month groups: {len(loaded_month_groups)}")
    print(f"Group: {GROUP_NAME}")
    print(f"Data root: {DATA_ROOT}")
    print(f"SAR mode: {SAR_RENDER_MODE} (auto detects per file)")
    print(f"Include S3 NDWI: {INCLUDE_S3_NDWI}")
    if canvas is None:
        print("Info: no interactive canvas (iface). Layers were added to project without map zoom/refresh.")
    if failures:
        print("Missing/failed:")
        for line in failures:
            print(f"  - {line}")

    labels = []
    for mg in loaded_month_groups:
        try:
            labels.append(mg.name())
        except Exception:
            labels.append("month")
    globals()["_FLOOD3_RANGE_ANIM_STATE"] = {
        "groups": loaded_month_groups,
        "labels": labels,
        "idx": 0,
        "timer": None,
    }
    print("To animate manually: start_flood3_range_animation(interval_ms=800, loop=True)")
    if AUTO_START_ANIMATION and canvas is not None:
        start_flood3_range_animation(interval_ms=ANIMATION_MS, loop=True)


main()

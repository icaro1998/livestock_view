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
from qgis.PyQt.QtGui import QColor


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "01/2025"))  # MM/YYYY
TO_MMYYYY = str(globals().get("TO_MMYYYY", "12/2025"))  # MM/YYYY
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
ADD_TRUECOLOR_BASE = bool(globals().get("ADD_TRUECOLOR_BASE", True))
SHOW_ONLY_LAST_MONTH = bool(globals().get("SHOW_ONLY_LAST_MONTH", True))
ZOOM_TO_RESULT = bool(globals().get("ZOOM_TO_RESULT", True))
GROUP_NAME = str(globals().get("GROUP_NAME", f"Flood 3-layer {FROM_MMYYYY} to {TO_MMYYYY}"))
SAR_MASK_GLOB_EXPRS = globals().get(
    "SAR_MASK_GLOB_EXPRS",
    [
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

    canvas = iface.mapCanvas()
    prev_render_flag = canvas.renderFlag()
    canvas.setRenderFlag(False)

    loaded_month_groups = []
    failures = []
    last_extent = None
    try:
        for year, month in _iter_months(fy, fm, ty, tm):
            ym = f"{year:04d}-{month:02d}"

            sar = _pick_one([expr.format(ym=ym) for expr in SAR_MASK_GLOB_EXPRS])
            dw = BASE / "output" / "flood" / "additional_10km_2025" / "dynamicworld" / f"dw_water_prob_{ym}.tif"
            ndwi = (
                BASE / "output" / "flood" / "additional_10km_2025" / "sentinel2_sr_harmonized" / f"s2_ndwi_{ym}.tif"
            )
            rgb = BASE / "output" / "flood" / "additional_10km_2025" / "sentinel2_truecolor" / f"s2_truecolor_{ym}.tif"

            if sar is None or not dw.exists() or not ndwi.exists():
                failures.append(f"{ym}: missing SAR/DW/NDWI")
                continue

            mg = group.addGroup(ym)
            try:
                if ADD_TRUECOLOR_BASE and rgb.exists():
                    try:
                        _add_layer(project, mg, rgb, f"S2 TrueColor {ym}", _style_truecolor)
                    except Exception as exc:
                        print(f"Warning [{ym}]: TrueColor style/load failed ({exc}). Continuing.")
                _add_layer(project, mg, ndwi, f"S2 NDWI {ym}", _style_s2_ndwi)
                _add_layer(project, mg, dw, f"DW Water Prob {ym}", _style_dw_prob)
                lyr_sar = _add_layer(project, mg, sar, f"SAR Water Mask {ym}", _style_sar_mask)
                last_extent = lyr_sar.extent()
                loaded_month_groups.append(mg)
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

        if ZOOM_TO_RESULT and last_extent is not None:
            canvas.setExtent(last_extent)
    finally:
        canvas.setRenderFlag(prev_render_flag)
        canvas.refresh()

    print(f"Loaded month groups: {len(loaded_month_groups)}")
    print(f"Group: {GROUP_NAME}")
    if failures:
        print("Missing/failed:")
        for line in failures:
            print(f"  - {line}")


main()

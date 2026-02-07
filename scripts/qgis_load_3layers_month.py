from pathlib import Path

from qgis.core import (
    QgsContrastEnhancement,
    QgsColorRampShader,
    QgsCoordinateReferenceSystem,
    QgsMultiBandColorRenderer,
    QgsProcessingFeedback,
    QgsProject,
    QgsRasterLayer,
    QgsRasterRange,
    QgsRasterShader,
    QgsSingleBandPseudoColorRenderer,
)
from qgis.PyQt.QtGui import QColor


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
MONTH = str(globals().get("MONTH", "03/2025"))  # MM/YYYY
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
ZOOM_TO_RESULT = bool(globals().get("ZOOM_TO_RESULT", True))
ADD_TRUECOLOR_BASE = bool(globals().get("ADD_TRUECOLOR_BASE", True))
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
        raise ValueError(f"MONTH must be MM/YYYY. Got: {value!r}")
    month = int(parts[0])
    year = int(parts[1])
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in {value!r}")
    return year, month


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


def _add_layer(project: QgsProject, group, path: Path, name: str, styler) -> QgsRasterLayer:
    layer = QgsRasterLayer(str(path), name, "gdal")
    if not layer.isValid():
        raise RuntimeError(f"Invalid raster layer: {path}")
    project.addMapLayer(layer, False)
    group.addLayer(layer)
    styler(layer)
    layer.triggerRepaint()
    return layer


def _set_truecolor_style(layer: QgsRasterLayer) -> None:
    provider = layer.dataProvider()
    try:
        # Common in masked exports: 0 values are effectively nodata.
        for band in (1, 2, 3):
            provider.setUserNoDataValue(band, [QgsRasterRange(0.0, 0.0)])
    except Exception:
        pass

    renderer = QgsMultiBandColorRenderer(provider, 1, 2, 3)

    # Sentinel-2 SR scaled values are typically in 0..10000 (reflectance * 10000).
    # Clamp to a display range that looks natural and avoids washed-out whites.
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
            # Keep default contrast if enhancement cannot be applied in this QGIS build.
            pass

    layer.setRenderer(renderer)
    renderer.setOpacity(1.0)


def _build_s2_truecolor_vrt(year: int, month: int) -> Path:
    ym = f"{year:04d}-{month:02d}"
    s2_dir = BASE / "output" / "flood" / "additional_10km_2025" / "sentinel2_sr_harmonized"
    b4 = s2_dir / f"s2_B4_{ym}.tif"  # red
    b3 = s2_dir / f"s2_B3_{ym}.tif"  # green
    b2 = s2_dir / f"s2_B2_{ym}.tif"  # blue
    for p in (b4, b3, b2):
        if not p.exists():
            raise FileNotFoundError(f"Sentinel-2 band missing for true color: {p}")

    vrt_dir = BASE / "qgis" / "cache"
    vrt_dir.mkdir(parents=True, exist_ok=True)
    vrt_path = vrt_dir / f"s2_truecolor_{ym}.vrt"

    errors: list[str] = []

    # Attempt 1: gdalbuildvrt command line (PATH or OSGeo4W full path).
    import subprocess

    candidates = ["gdalbuildvrt", r"C:\OSGeo4W\bin\gdalbuildvrt.exe"]
    for exe in candidates:
        try:
            subprocess.run(
                [exe, "-overwrite", "-separate", str(vrt_path), str(b4), str(b3), str(b2)],
                check=False,
                capture_output=True,
                text=True,
            )
            if vrt_path.exists():
                return vrt_path
        except Exception as exc:
            errors.append(f"{exe}: {exc}")

    # Attempt 2: GDAL Python API.
    try:
        from osgeo import gdal

        ds = gdal.BuildVRT(str(vrt_path), [str(b4), str(b3), str(b2)], separate=True)
        if ds is not None:
            ds.FlushCache()
            ds = None
        if vrt_path.exists():
            return vrt_path
        errors.append("osgeo.gdal.BuildVRT returned no dataset")
    except Exception as exc:
        errors.append(f"osgeo.gdal.BuildVRT: {exc}")

    # Attempt 3: QGIS processing (gdal:buildvirtualraster).
    try:
        import processing

        params = {
            "INPUT": [str(b4), str(b3), str(b2)],
            "RESOLUTION": 0,  # average
            "SEPARATE": True,
            "PROJ_DIFFERENCE": False,
            "ADD_ALPHA": False,
            "ASSIGN_CRS": None,
            "RESAMPLING": 0,  # nearest
            "SRC_NODATA": None,
            "OUTPUT": str(vrt_path),
        }
        processing.run("gdal:buildvirtualraster", params, feedback=QgsProcessingFeedback())
        if vrt_path.exists():
            return vrt_path
        errors.append("processing gdal:buildvirtualraster did not create output")
    except Exception as exc:
        errors.append(f"processing gdal:buildvirtualraster: {exc}")

    raise RuntimeError(f"Could not build true color VRT: {vrt_path}. Attempts: {' | '.join(errors)}")
    return vrt_path


def main() -> None:
    year, month = _parse_month(MONTH)
    ym = f"{year:04d}-{month:02d}"

    sar_mask = _pick_one([expr.format(ym=ym) for expr in SAR_MASK_GLOB_EXPRS])
    if sar_mask is None:
        raise FileNotFoundError(f"No SAR water_mask file found for {ym}.")

    dw_prob = BASE / "output" / "flood" / "additional_10km_2025" / "dynamicworld" / f"dw_water_prob_{ym}.tif"
    if not dw_prob.exists():
        raise FileNotFoundError(f"Dynamic World file not found: {dw_prob}")

    s2_ndwi = (
        BASE
        / "output"
        / "flood"
        / "additional_10km_2025"
        / "sentinel2_sr_harmonized"
        / f"s2_ndwi_{ym}.tif"
    )
    if not s2_ndwi.exists():
        raise FileNotFoundError(f"Sentinel-2 NDWI file not found: {s2_ndwi}")
    s2_truecolor_tif = (
        BASE / "output" / "flood" / "additional_10km_2025" / "sentinel2_truecolor" / f"s2_truecolor_{ym}.tif"
    )

    project = QgsProject.instance()
    if CLEAR_PROJECT:
        project.removeAllMapLayers()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

    group_name = f"Flood 3-layer {ym}"
    root = project.layerTreeRoot()
    previous = root.findGroup(group_name)
    if previous is not None:
        root.removeChildNode(previous)
    group = root.addGroup(group_name)

    # Draw order (bottom -> top): S2 True Color, S2 NDWI, Dynamic World probability, SAR mask.
    s2_vrt = None
    if ADD_TRUECOLOR_BASE:
        try:
            rgb_source = s2_truecolor_tif if s2_truecolor_tif.exists() else _build_s2_truecolor_vrt(year, month)
            s2_vrt = rgb_source
            lyr_rgb = QgsRasterLayer(str(rgb_source), f"S2 TrueColor {ym}", "gdal")
            if not lyr_rgb.isValid():
                raise RuntimeError(f"Invalid true color layer: {rgb_source}")
            project.addMapLayer(lyr_rgb, False)
            group.addLayer(lyr_rgb)
            _set_truecolor_style(lyr_rgb)
            lyr_rgb.triggerRepaint()
        except Exception as exc:
            print(f"Warning: TrueColor base was not added ({exc}). Continuing with water layers.")

    lyr_s2 = _add_layer(project, group, s2_ndwi, f"S2 NDWI {ym}", _style_s2_ndwi)
    lyr_dw = _add_layer(project, group, dw_prob, f"DW Water Prob {ym}", _style_dw_prob)
    lyr_sar = _add_layer(project, group, sar_mask, f"SAR Water Mask {ym}", _style_sar_mask)

    if ZOOM_TO_RESULT:
        iface.mapCanvas().setExtent(lyr_sar.extent())
    iface.mapCanvas().refresh()

    print(f"Loaded month: {ym}")
    print(f"  SAR mask: {sar_mask}")
    print(f"  DW prob:  {dw_prob}")
    print(f"  S2 NDWI:  {s2_ndwi}")
    if ADD_TRUECOLOR_BASE and s2_vrt is not None:
        print(f"  S2 RGB:   {s2_vrt}")
    print(f"Group: {group_name}")


main()

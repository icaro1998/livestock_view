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
ADDITIONAL_DIR = str(globals().get("ADDITIONAL_DIR", "output/flood/additional_30km_2025"))
INCLUDE_S3_NDWI = bool(globals().get("INCLUDE_S3_NDWI", True))
SAR_RENDER_MODE = str(globals().get("SAR_RENDER_MODE", "auto")).strip().lower()
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
    s2_dir = DATA_ROOT / "sentinel2_sr_harmonized"
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
    dw_prob = DATA_ROOT / "dynamicworld" / f"dw_water_prob_{ym}.tif"
    s2_ndwi = DATA_ROOT / "sentinel2_sr_harmonized" / f"s2_ndwi_{ym}.tif"
    s3_ndwi = DATA_ROOT / "s3_olci" / f"s3_ndwi_{ym}.tif"
    s2_truecolor_tif = DATA_ROOT / "sentinel2_truecolor" / f"s2_truecolor_{ym}.tif"

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

    # Draw order (bottom -> top): S2 True Color, S2 NDWI, S3 NDWI, Dynamic World probability, SAR.
    s2_vrt = None
    loaded_count = 0
    zoom_layer = None
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
            loaded_count += 1
            zoom_layer = zoom_layer or lyr_rgb
        except Exception as exc:
            print(f"Warning: TrueColor base was not added ({exc}). Continuing with water layers.")

    if s2_ndwi.exists():
        lyr_s2 = _add_layer(project, group, s2_ndwi, f"S2 NDWI {ym}", _style_s2_ndwi)
        loaded_count += 1
        zoom_layer = zoom_layer or lyr_s2
    else:
        print(f"Warning: missing S2 NDWI for {ym}: {s2_ndwi}")

    if INCLUDE_S3_NDWI:
        if s3_ndwi.exists():
            lyr_s3 = _add_layer(project, group, s3_ndwi, f"S3 NDWI {ym}", _style_s3_ndwi)
            loaded_count += 1
            zoom_layer = zoom_layer or lyr_s3
        else:
            print(f"Warning: missing S3 NDWI for {ym}: {s3_ndwi}")

    if dw_prob.exists():
        lyr_dw = _add_layer(project, group, dw_prob, f"DW Water Prob {ym}", _style_dw_prob)
        loaded_count += 1
        zoom_layer = zoom_layer or lyr_dw
    else:
        print(f"Warning: missing DynamicWorld water prob for {ym}: {dw_prob}")

    sar_mode = None
    if sar_mask is not None:
        sar_mode = _resolve_sar_mode(sar_mask)
        sar_styler = _style_sar_flood_diff if sar_mode == "flood_diff" else _style_sar_mask
        sar_label = "S1 Flood Diff" if sar_mode == "flood_diff" else "SAR Water Mask"
        lyr_sar = _add_layer(project, group, sar_mask, f"{sar_label} {ym}", sar_styler)
        loaded_count += 1
        zoom_layer = zoom_layer or lyr_sar
    else:
        print(f"Warning: missing SAR file for {ym} (checked SAR_MASK_GLOB_EXPRS).")

    if loaded_count == 0:
        raise RuntimeError(f"No raster layers could be loaded for {ym}.")

    canvas = _get_canvas()
    if canvas is not None:
        if ZOOM_TO_RESULT and zoom_layer is not None:
            canvas.setExtent(zoom_layer.extent())
        canvas.refresh()
    else:
        print("Info: no interactive canvas (iface). Layers were added to project without map zoom/refresh.")

    print(f"Loaded month: {ym}")
    print(f"  SAR mask: {sar_mask}")
    print(f"  SAR mode: {sar_mode}")
    print(f"  Data root:{DATA_ROOT}")
    print(f"  DW prob:  {dw_prob}")
    print(f"  S2 NDWI:  {s2_ndwi}")
    if INCLUDE_S3_NDWI:
        print(f"  S3 NDWI:  {s3_ndwi}")
    if ADD_TRUECOLOR_BASE and s2_vrt is not None:
        print(f"  S2 RGB:   {s2_vrt}")
    print(f"  Layers:   {loaded_count}")
    print(f"Group: {group_name}")


main()

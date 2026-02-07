from __future__ import annotations

import math
import re
from pathlib import Path

from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsColorRampShader,
    QgsCoordinateReferenceSystem,
    QgsGraduatedSymbolRenderer,
    QgsLayerTreeGroup,
    QgsLineSymbol,
    QgsPalLayerSettings,
    QgsProject,
    QgsRasterBandStats,
    QgsRasterLayer,
    QgsRasterShader,
    QgsRendererRange,
    QgsSingleBandGrayRenderer,
    QgsSingleBandPseudoColorRenderer,
    QgsStyle,
    QgsTextBufferSettings,
    QgsTextFormat,
    QgsVectorLayer,
    QgsVectorLayerSimpleLabeling,
)

try:
    import processing  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("Processing plugin is required in QGIS to build contours.") from exc


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
DEM_PATH = Path(
    globals().get("DEM_PATH", str(BASE / "output" / "terrain_context" / "terrain_elevation.tif"))
)
SLOPE_PATH = Path(
    globals().get("SLOPE_PATH", str(BASE / "output" / "terrain_context" / "terrain_slope.tif"))
)
HILLSHADE_PATH = Path(
    globals().get("HILLSHADE_PATH", str(BASE / "output" / "terrain_context" / "terrain_hillshade.tif"))
)
OUT_DIR = Path(
    globals().get("OUT_DIR", str(BASE / "output" / "terrain_context" / "derived_qgis"))
)
CONTOUR_INTERVAL = float(globals().get("CONTOUR_INTERVAL", 5.0))
FORCE_REBUILD_CONTOURS = bool(globals().get("FORCE_REBUILD_CONTOURS", False))
ENABLE_CONTOURS = bool(globals().get("ENABLE_CONTOURS", True))
GROUP_NAME = str(globals().get("GROUP_NAME", "Terrain stack"))
SHOW_SLOPE = bool(globals().get("SHOW_SLOPE", True))
CONTOUR_LABEL_EVERY = int(globals().get("CONTOUR_LABEL_EVERY", 10))
REQUIRE_CONTOURS = bool(globals().get("REQUIRE_CONTOURS", False))
PREFER_SHAPEFILE_CONTOURS = bool(globals().get("PREFER_SHAPEFILE_CONTOURS", True))
HILLSHADE_OPACITY = float(globals().get("HILLSHADE_OPACITY", 0.35))
SLOPE_OPACITY = float(globals().get("SLOPE_OPACITY", 0.40))
ELEVATION_OPACITY = float(globals().get("ELEVATION_OPACITY", 0.92))
ELEVATION_SIGMA_STRETCH = float(globals().get("ELEVATION_SIGMA_STRETCH", 2.2))
KML_GROUP_NAME = str(globals().get("KML_GROUP_NAME", "Higuerones KML"))
KML_PATH = str(globals().get("KML_PATH", "")).strip()
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))


def _pick_default_kml() -> Path | None:
    candidates = [
        Path(r"C:\Users\orlan\Desktop\Higuerones.KML"),
        Path(r"C:\Users\orlan\Desktop\Mapa Higuerones.kml"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def _ensure_group(root, name: str) -> QgsLayerTreeGroup:
    old = root.findGroup(name)
    if old is not None:
        root.removeChildNode(old)
    return root.addGroup(name)


def _add_layer_to_group(layer, group: QgsLayerTreeGroup, visible: bool = True) -> None:
    QgsProject.instance().addMapLayer(layer, False)
    group.addLayer(layer)
    node = QgsProject.instance().layerTreeRoot().findLayer(layer.id())
    if node is not None:
        node.setItemVisibilityChecked(visible)


def _band_stats(layer: QgsRasterLayer):
    return layer.dataProvider().bandStatistics(
        1, QgsRasterBandStats.All, layer.extent(), 0
    )


def _band_min_max(layer: QgsRasterLayer) -> tuple[float, float]:
    stats = _band_stats(layer)
    return float(stats.minimumValue), float(stats.maximumValue)


def _stretch_min_max(layer: QgsRasterLayer, sigma: float) -> tuple[float, float]:
    stats = layer.dataProvider().bandStatistics(
        1, QgsRasterBandStats.All, layer.extent(), 0
    )
    mn = float(stats.minimumValue)
    mx = float(stats.maximumValue)
    mean = float(stats.mean)
    std = float(stats.stdDev)
    if (not math.isfinite(std)) or std <= 0 or sigma <= 0:
        return mn, mx
    lo = max(mn, mean - sigma * std)
    hi = min(mx, mean + sigma * std)
    if not math.isfinite(lo) or not math.isfinite(hi) or lo >= hi:
        return mn, mx
    return lo, hi


def _style_elevation(layer: QgsRasterLayer) -> None:
    mn, mx = _stretch_min_max(layer, ELEVATION_SIGMA_STRETCH)
    if not math.isfinite(mn) or not math.isfinite(mx) or mn >= mx:
        mn, mx = 0.0, 100.0

    stops = [
        (0.00, "#0c2c84"),
        (0.08, "#225ea8"),
        (0.16, "#1d91c0"),
        (0.26, "#41b6c4"),
        (0.36, "#7fcdbb"),
        (0.46, "#c7e9b4"),
        (0.56, "#ffffcc"),
        (0.68, "#fdd49e"),
        (0.78, "#fdae6b"),
        (0.88, "#f16913"),
        (1.00, "#a63603"),
    ]

    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    items = []
    for p, hex_color in stops:
        v = mn + p * (mx - mn)
        items.append(QgsColorRampShader.ColorRampItem(v, QColor(hex_color), f"{v:.1f} m"))
    ramp.setColorRampItemList(items)
    shader.setRasterShaderFunction(ramp)

    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(ELEVATION_OPACITY)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _style_hillshade(layer: QgsRasterLayer) -> None:
    renderer = QgsSingleBandGrayRenderer(layer.dataProvider(), 1)
    renderer.setOpacity(HILLSHADE_OPACITY)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _style_slope(layer: QgsRasterLayer) -> None:
    mn, mx = _band_min_max(layer)
    mn = max(0.0, mn)
    if not math.isfinite(mx) or mx <= mn:
        mx = 45.0

    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(mn, QColor("#ffffcc"), f"{mn:.1f}"),
            QgsColorRampShader.ColorRampItem(mn + 0.33 * (mx - mn), QColor("#a1dab4"), ""),
            QgsColorRampShader.ColorRampItem(mn + 0.66 * (mx - mn), QColor("#41b6c4"), ""),
            QgsColorRampShader.ColorRampItem(mx, QColor("#225ea8"), f"{mx:.1f}"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(SLOPE_OPACITY)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _build_contours(dem_path: Path, out_path: Path, interval: float) -> Path:
    if interval <= 0:
        raise ValueError("CONTOUR_INTERVAL must be > 0.")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if FORCE_REBUILD_CONTOURS:
        if out_path.suffix.lower() == ".shp":
            for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg", ".qpj"):
                candidate = out_path.with_suffix(ext)
                if candidate.exists():
                    candidate.unlink()
        elif out_path.exists():
            out_path.unlink()
    if out_path.exists():
        return out_path

    params = {
        "INPUT": str(dem_path),
        "BAND": 1,
        "INTERVAL": float(interval),
        "FIELD_NAME": "elev_m",
        "CREATE_3D": False,
        "IGNORE_NODATA": True,
        "NODATA": -9999.0,
        "OFFSET": 0.0,
        "OUTPUT": str(out_path),
    }
    processing.run("gdal:contour", params)
    return out_path


def _delete_vector_sidecars(path: Path) -> None:
    if path.suffix.lower() == ".shp":
        for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg", ".qpj"):
            candidate = path.with_suffix(ext)
            if candidate.exists():
                candidate.unlink()
    elif path.exists():
        path.unlink()
    journal = path.with_name(path.name + "-journal")
    if journal.exists():
        journal.unlink()
    tmp_rtree = path.with_name(path.name + ".tmp_rtree_contour.db")
    if tmp_rtree.exists():
        tmp_rtree.unlink()


def _open_vector_layer(path: Path, display_name: str) -> QgsVectorLayer | None:
    layer = QgsVectorLayer(str(path), display_name, "ogr")
    if layer.isValid():
        return layer

    probe = QgsVectorLayer(str(path), path.stem, "ogr")
    if not probe.isValid():
        return None

    sublayers = probe.dataProvider().subLayers()
    for sub in sublayers:
        sub_id, sub_name = _parse_sublayer_descriptor(sub)
        uri = f"{path}|layername={sub_name}"
        layer = QgsVectorLayer(uri, display_name, "ogr")
        if (not layer.isValid()) and sub_id is not None:
            uri = f"{path}|layerid={sub_id}"
            layer = QgsVectorLayer(uri, display_name, "ogr")
        if layer.isValid():
            return layer
    return None


def _style_contours(layer: QgsVectorLayer, interval: float) -> None:
    field = "elev_m"
    idx = layer.fields().indexFromName(field)
    if idx < 0:
        numeric = [f.name() for f in layer.fields() if f.isNumeric()]
        if not numeric:
            return
        field = numeric[0]
        idx = layer.fields().indexFromName(field)

    min_val = layer.minimumValue(idx)
    max_val = layer.maximumValue(idx)
    if min_val is None or max_val is None or float(max_val) <= float(min_val):
        symbol = QgsLineSymbol.createSimple({"color": "#1f2937", "width": "0.35"})
        layer.renderer().setSymbol(symbol)  # type: ignore[attr-defined]
        return

    min_val = float(min_val)
    max_val = float(max_val)
    classes = 7
    step = (max_val - min_val) / classes
    ranges = []
    style = QgsStyle().defaultStyle()
    color_ramp = style.colorRamp("Viridis")
    if color_ramp is None:
        color_ramp = style.colorRamp("Spectral")
    if color_ramp is None:
        color_ramp = style.colorRamp("Turbo")

    for i in range(classes):
        lo = min_val + i * step
        hi = max_val if i == classes - 1 else min_val + (i + 1) * step
        if color_ramp is not None:
            color = color_ramp.color(i / max(1, classes - 1))
        else:
            color = QColor("#1f2937")
        sym = QgsLineSymbol.createSimple({"color": color.name(), "width": "0.30"})
        ranges.append(QgsRendererRange(lo, hi, sym, f"{lo:.1f} - {hi:.1f} m"))

    renderer = QgsGraduatedSymbolRenderer(field, ranges)
    layer.setRenderer(renderer)

    # Label only major contours (e.g., every 10 m)
    settings = QgsPalLayerSettings()
    settings.enabled = True
    settings.fieldName = f"CASE WHEN \"{field}\" % {CONTOUR_LABEL_EVERY} = 0 THEN round(\"{field}\",1) END"
    tf = QgsTextFormat()
    tf.setSize(8)
    tf.setColor(QColor("#111827"))
    buf = QgsTextBufferSettings()
    buf.setEnabled(True)
    buf.setSize(0.8)
    buf.setColor(QColor("#ffffff"))
    tf.setBuffer(buf)
    settings.setFormat(tf)
    layer.setLabelsEnabled(True)
    layer.setLabeling(QgsVectorLayerSimpleLabeling(settings))
    layer.triggerRepaint()


def _parse_sublayer_descriptor(descriptor: str) -> tuple[str | None, str]:
    if "!!::!!" in descriptor:
        parts = descriptor.split("!!::!!")
    elif "!!::" in descriptor:
        parts = descriptor.split("!!::")
    else:
        return (None, descriptor)
    layer_id = None
    layer_name = descriptor
    if len(parts) >= 2:
        if parts[0].isdigit():
            layer_id = parts[0]
            layer_name = parts[1]
        else:
            layer_name = parts[0]
    return layer_id, layer_name


def _load_kml_to_top(kml_path: Path, group_name: str) -> list[QgsVectorLayer]:
    root = QgsProject.instance().layerTreeRoot()
    old = root.findGroup(group_name)
    if old is not None:
        root.removeChildNode(old)
    if not kml_path.exists():
        print(f"KML not found: {kml_path}")
        return []

    group = root.insertGroup(0, group_name)
    loaded = []
    probe = QgsVectorLayer(str(kml_path), kml_path.stem, "ogr")
    if not probe.isValid():
        print(f"KML invalid: {kml_path}")
        return loaded

    sublayers = probe.dataProvider().subLayers()
    if not sublayers:
        layer = QgsVectorLayer(str(kml_path), kml_path.stem, "ogr")
        if layer.isValid():
            QgsProject.instance().addMapLayer(layer, False)
            group.addLayer(layer)
            loaded.append(layer)
        return loaded

    for sub in sublayers:
        sub_id, sub_name = _parse_sublayer_descriptor(sub)
        uri = f"{kml_path}|layername={sub_name}"
        layer = QgsVectorLayer(uri, sub_name, "ogr")
        if (not layer.isValid()) and sub_id is not None:
            uri = f"{kml_path}|layerid={sub_id}"
            layer = QgsVectorLayer(uri, sub_name, "ogr")
        if not layer.isValid():
            continue
        QgsProject.instance().addMapLayer(layer, False)
        group.addLayer(layer)
        loaded.append(layer)
    return loaded


def main() -> None:
    if CLEAR_PROJECT:
        QgsProject.instance().removeAllMapLayers()

    if not DEM_PATH.exists():
        raise FileNotFoundError(
            f"DEM not found: {DEM_PATH}\n"
            "Generate first with:\n"
            r".\.venv\Scripts\python.exe .\scripts\flood_pipeline.py --mode context --context-scale 30 --out-dir output\terrain_context --log"
        )

    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
    root = project.layerTreeRoot()
    terrain_group = _ensure_group(root, GROUP_NAME)

    contour_path = None
    if ENABLE_CONTOURS:
        contour_suffix = ".shp" if PREFER_SHAPEFILE_CONTOURS else ".gpkg"
        contour_name = f"contours_{str(CONTOUR_INTERVAL).replace('.', 'p')}m{contour_suffix}"
        contour_path = _build_contours(DEM_PATH, OUT_DIR / contour_name, CONTOUR_INTERVAL)

    dem_layer = QgsRasterLayer(str(DEM_PATH), "Elevation (m)", "gdal")
    if not dem_layer.isValid():
        raise RuntimeError(f"Invalid DEM layer: {DEM_PATH}")
    _style_elevation(dem_layer)
    _add_layer_to_group(dem_layer, terrain_group, visible=True)

    hillshade_layer = None
    if HILLSHADE_PATH.exists():
        hillshade_layer = QgsRasterLayer(str(HILLSHADE_PATH), "Hillshade", "gdal")
        if hillshade_layer.isValid():
            _style_hillshade(hillshade_layer)
            _add_layer_to_group(hillshade_layer, terrain_group, visible=True)

    slope_layer = None
    if SLOPE_PATH.exists():
        slope_layer = QgsRasterLayer(str(SLOPE_PATH), "Slope (deg)", "gdal")
        if slope_layer.isValid():
            _style_slope(slope_layer)
            _add_layer_to_group(slope_layer, terrain_group, visible=SHOW_SLOPE)

    if contour_path is not None:
        contour_layer = _open_vector_layer(contour_path, "Contours")
        if contour_layer is None:
            _delete_vector_sidecars(contour_path)
            contour_path = _build_contours(DEM_PATH, contour_path, CONTOUR_INTERVAL)
            contour_layer = _open_vector_layer(contour_path, "Contours")
        if contour_layer is None and contour_path.suffix.lower() != ".shp":
            fallback_path = OUT_DIR / f"contours_{str(CONTOUR_INTERVAL).replace('.', 'p')}m.shp"
            _delete_vector_sidecars(fallback_path)
            contour_path = _build_contours(DEM_PATH, fallback_path, CONTOUR_INTERVAL)
            contour_layer = _open_vector_layer(contour_path, "Contours")
        if contour_layer is not None:
            _style_contours(contour_layer, CONTOUR_INTERVAL)
            _add_layer_to_group(contour_layer, terrain_group, visible=True)
        elif REQUIRE_CONTOURS:
            raise RuntimeError(f"Invalid contour layer: {contour_path}")
        else:
            print(f"Warning: contour layer skipped (failed to open): {contour_path}")

    kml_candidates = []
    if KML_PATH:
        kml_candidates.append(Path(KML_PATH))
    default_kml = _pick_default_kml()
    if default_kml is not None and default_kml not in kml_candidates:
        kml_candidates.append(default_kml)

    loaded_kml_layers = []
    for kml in kml_candidates:
        loaded_kml_layers = _load_kml_to_top(kml, KML_GROUP_NAME)
        if loaded_kml_layers:
            break

    if dem_layer.extent().isFinite():
        iface.mapCanvas().setExtent(dem_layer.extent())
        iface.mapCanvas().refresh()

    print("Terrain stack ready.")
    print(f"DEM: {DEM_PATH}")
    if contour_path is not None:
        print(f"Contours: {contour_path}")
        print(f"Contour interval: {CONTOUR_INTERVAL} m")
    else:
        print("Contours: disabled")
    print(f"KML layers loaded: {len(loaded_kml_layers)}")


main()

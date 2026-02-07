from __future__ import annotations

from pathlib import Path

from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsColorRampShader,
    QgsCoordinateReferenceSystem,
    QgsProject,
    QgsRasterBandStats,
    QgsRasterLayer,
    QgsRasterShader,
    QgsSingleBandGrayRenderer,
    QgsSingleBandPseudoColorRenderer,
    QgsVectorLayer,
)


# One-script topography loader with colors compatible with water-motion layers.
# Palette intent:
# - Terrain base: muted earth tones (low visual noise)
# - Hillshade: subtle grayscale relief
# - Slope: amber/orange accents for steep transitions
# - Optional contours: light neutral lines

BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
CONTEXT_DIR = Path(globals().get("CONTEXT_DIR", str(BASE / "output" / "flood" / "context_10km")))
FALLBACK_CONTEXT_DIR = Path(globals().get("FALLBACK_CONTEXT_DIR", str(BASE / "output" / "terrain_context")))
CONTOURS_PATH = Path(
    globals().get(
        "CONTOURS_PATH",
        str(BASE / "output" / "terrain_context" / "derived_qgis" / "contours_2p0m.shp"),
    )
)

GROUP_NAME = str(globals().get("GROUP_NAME", "Topography Compatible"))
CLEAR_GROUP = bool(globals().get("CLEAR_GROUP", True))
LOAD_CONTOURS = bool(globals().get("LOAD_CONTOURS", True))

ELEVATION_OPACITY = float(globals().get("ELEVATION_OPACITY", 0.78))
HILLSHADE_OPACITY = float(globals().get("HILLSHADE_OPACITY", 0.28))
SLOPE_OPACITY = float(globals().get("SLOPE_OPACITY", 0.42))


def _pick_context_dir() -> Path:
    if CONTEXT_DIR.exists():
        return CONTEXT_DIR
    return FALLBACK_CONTEXT_DIR


def _remove_group(name: str) -> None:
    root = QgsProject.instance().layerTreeRoot()
    g = root.findGroup(name)
    if g is not None:
        root.removeChildNode(g)


def _add_layer(layer, group, visible: bool = True) -> None:
    QgsProject.instance().addMapLayer(layer, False)
    group.addLayer(layer)
    node = QgsProject.instance().layerTreeRoot().findLayer(layer.id())
    if node is not None:
        node.setItemVisibilityChecked(visible)


def _stats(layer: QgsRasterLayer) -> QgsRasterBandStats:
    return layer.dataProvider().bandStatistics(1, QgsRasterBandStats.All, layer.extent(), 0)


def _style_elevation(layer: QgsRasterLayer) -> None:
    s = _stats(layer)
    lo = float(s.minimumValue)
    hi = float(s.maximumValue)
    if hi <= lo:
        lo, hi = 0.0, 100.0

    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    # Muted terrain ramp; avoids clashing with cyan/orange water-change colors.
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.00, QColor("#2d3b29"), "Low"),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.25, QColor("#4b5f3f"), ""),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.50, QColor("#7a7b55"), ""),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.75, QColor("#a58b62"), ""),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 1.00, QColor("#d1b07f"), "High"),
        ]
    )
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
    s = _stats(layer)
    lo = max(0.0, float(s.minimumValue))
    hi = float(s.maximumValue)
    if hi <= lo:
        hi = 45.0

    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    # Amber/orange ramp to mark abrupt relief while staying compatible with change overlays.
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.00, QColor("#fff7ec"), "Flat"),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.35, QColor("#fdd49e"), ""),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 0.70, QColor("#f16913"), ""),
            QgsColorRampShader.ColorRampItem(lo + (hi - lo) * 1.00, QColor("#7f2704"), "Steep"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(SLOPE_OPACITY)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _style_contours(layer: QgsVectorLayer) -> None:
    sym = layer.renderer().symbol()
    if sym is not None:
        sym.setColor(QColor("#d9d9d9"))
        sym.setOpacity(0.65)
        sym.setWidth(0.22)
    layer.triggerRepaint()


def _load_raster(path: Path, name: str) -> QgsRasterLayer:
    layer = QgsRasterLayer(str(path), name, "gdal")
    if not layer.isValid():
        raise RuntimeError(f"Invalid raster layer: {path}")
    return layer


def main() -> None:
    context_dir = _pick_context_dir()
    elev_path = context_dir / "terrain_elevation.tif"
    hill_path = context_dir / "terrain_hillshade.tif"
    slope_path = context_dir / "terrain_slope.tif"

    for p in (elev_path, hill_path, slope_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing required topography raster: {p}")

    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
    if CLEAR_GROUP:
        _remove_group(GROUP_NAME)
    root = project.layerTreeRoot()
    group = root.addGroup(GROUP_NAME)

    elev = _load_raster(elev_path, "Topography Elevation")
    hill = _load_raster(hill_path, "Topography Hillshade")
    slope = _load_raster(slope_path, "Topography Slope")

    _style_elevation(elev)
    _style_hillshade(hill)
    _style_slope(slope)

    _add_layer(elev, group, True)
    _add_layer(hill, group, True)
    _add_layer(slope, group, True)

    if LOAD_CONTOURS and CONTOURS_PATH.exists():
        contours = QgsVectorLayer(str(CONTOURS_PATH), "Topography Contours", "ogr")
        if contours.isValid():
            _style_contours(contours)
            _add_layer(contours, group, True)

    iface.mapCanvas().setExtent(elev.extent())
    iface.mapCanvas().refresh()
    print(f"Loaded topography stack from: {context_dir}")


main()


from __future__ import annotations

import re
from pathlib import Path

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
    QgsVectorLayer,
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")

# General
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
ZOOM_TO_RESULT = bool(globals().get("ZOOM_TO_RESULT", True))
GROUP_NAME = str(globals().get("GROUP_NAME", "Ordered Flood Stack"))

# 1) Topography (single grayscale image)
TOPO_RASTER = Path(globals().get("TOPO_RASTER", str(BASE / "output" / "terrain_context" / "terrain_hillshade.tif")))
TOPO_OPACITY = float(globals().get("TOPO_OPACITY", 0.95))

# Optional contours overlay (disabled by default to keep "single image" workflow)
ADD_CONTOURS = bool(globals().get("ADD_CONTOURS", False))
CONTOUR_PATH = Path(
    globals().get("CONTOUR_PATH", str(BASE / "output" / "terrain_context" / "derived_qgis" / "contours_1p0m.shp"))
)

# 2) Satellite cloud-minimized true-color base (single image)
S2_DIR = Path(globals().get("S2_DIR", str(BASE / "output" / "sentinel2_truecolor_best_10km_2025")))
S2_YEAR = int(globals().get("S2_YEAR", 2025))
S2_MONTH = int(globals().get("S2_MONTH", 7))  # 2025-07 has very low cloud in summary.
S2_OPACITY = float(globals().get("S2_OPACITY", 1.0))

# 3) Water layers (chronological + permanent)
EVOLUTION_DIR = Path(
    globals().get(
        "EVOLUTION_DIR",
        str(BASE / "output" / "flood" / "water_evolution_2025_full_s1_10m_consistent"),
    )
)
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "01/2025"))
TO_MMYYYY = str(globals().get("TO_MMYYYY", "12/2025"))
SHOW_ONLY_LAST_MONTH = bool(globals().get("SHOW_ONLY_LAST_MONTH", False))
LOAD_MONTHLY_MASKS = bool(globals().get("LOAD_MONTHLY_MASKS", True))
WATER_MASK_OPACITY = float(globals().get("WATER_MASK_OPACITY", 0.32))
PERMANENT_OPACITY = float(globals().get("PERMANENT_OPACITY", 0.58))
FREQUENCY_OPACITY = float(globals().get("FREQUENCY_OPACITY", 0.38))
LOAD_OVERFLOW = bool(globals().get("LOAD_OVERFLOW", False))
OVERFLOW_OPACITY = float(globals().get("OVERFLOW_OPACITY", 0.28))

# 4) Recommended optional overlays
LOAD_SURFACE_WATER_OCCURRENCE = bool(globals().get("LOAD_SURFACE_WATER_OCCURRENCE", True))
SURFACE_WATER_OCCURRENCE_PATH = Path(
    globals().get(
        "SURFACE_WATER_OCCURRENCE_PATH",
        str(BASE / "output" / "terrain_context" / "surface_water_occurrence.tif"),
    )
)
SURFACE_WATER_OCCURRENCE_OPACITY = float(globals().get("SURFACE_WATER_OCCURRENCE_OPACITY", 0.24))

KML_PATH = str(globals().get("KML_PATH", "")).strip()
KML_GROUP_NAME = str(globals().get("KML_GROUP_NAME", "Study Area KML"))

MASK_RE = re.compile(r"water_mask_(\d{4}-\d{2}-\d{2})\.tif$")
OVERFLOW_RE = re.compile(r"overflow_mask_(\d{4}-\d{2}-\d{2})\.tif$")


def _month_key(value: str) -> int:
    mm, yyyy = value.split("/")
    month = int(mm)
    year = int(yyyy)
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in MM/YYYY: {value}")
    return year * 12 + month


def _date_to_month_key(date_yyyy_mm_dd: str) -> int:
    yyyy = int(date_yyyy_mm_dd[0:4])
    mm = int(date_yyyy_mm_dd[5:7])
    return yyyy * 12 + mm


def _sorted_masks(folder: Path, pattern: re.Pattern[str], from_key: int, to_key: int) -> list[tuple[str, Path]]:
    items: list[tuple[str, Path]] = []
    if not folder.exists():
        return items
    for p in sorted(folder.glob("*.tif")):
        m = pattern.match(p.name)
        if not m:
            continue
        date_key = _date_to_month_key(m.group(1))
        if from_key <= date_key <= to_key:
            items.append((m.group(1), p))
    return items


def _add_raster(group, path: Path, name: str) -> QgsRasterLayer:
    layer = QgsRasterLayer(str(path), name, "gdal")
    if not layer.isValid():
        raise RuntimeError(f"Invalid raster: {path}")
    QgsProject.instance().addMapLayer(layer, False)
    group.addLayer(layer)
    return layer


def _move_group_top(master, group_name: str) -> None:
    node = master.findGroup(group_name)
    if node is None:
        return
    clone = node.clone()
    master.insertChildNode(0, clone)
    master.removeChildNode(node)


def _move_layer_top(group, layer_id: str) -> None:
    node = group.findLayer(layer_id)
    if node is None:
        return
    clone = node.clone()
    group.insertChildNode(0, clone)
    group.removeChildNode(node)


def _style_grayscale(layer: QgsRasterLayer, opacity: float) -> None:
    renderer = QgsSingleBandGrayRenderer(layer.dataProvider(), 1)
    dtype = layer.dataProvider().dataType(1)
    ce = QgsContrastEnhancement(dtype)
    ce.setContrastEnhancementAlgorithm(QgsContrastEnhancement.StretchToMinimumMaximum, True)
    ce.setMinimumValue(0.0)
    ce.setMaximumValue(255.0)
    renderer.setContrastEnhancement(ce)
    layer.setRenderer(renderer)
    renderer.setOpacity(opacity)


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


def _style_binary_mask(layer: QgsRasterLayer, color_hex: str, opacity: float, label: str) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "No water"),
            QgsColorRampShader.ColorRampItem(0.49, QColor(255, 255, 255, 0), ""),
            QgsColorRampShader.ColorRampItem(0.50, QColor(color_hex), label),
            QgsColorRampShader.ColorRampItem(1.0, QColor(color_hex), label),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


def _style_frequency_fraction(layer: QgsRasterLayer, opacity: float) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(-1.0, QColor(255, 255, 255, 0), "No data"),
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "0"),
            QgsColorRampShader.ColorRampItem(0.10, QColor("#deebf7"), "0.10"),
            QgsColorRampShader.ColorRampItem(0.25, QColor("#9ecae1"), "0.25"),
            QgsColorRampShader.ColorRampItem(0.50, QColor("#4292c6"), "0.50"),
            QgsColorRampShader.ColorRampItem(0.75, QColor("#2171b5"), "0.75"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#08306b"), "1.00"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


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
    return (layer_id, layer_name)


def _load_kml_folders(kml_path: Path, group_name: str) -> list[QgsVectorLayer]:
    root = QgsProject.instance().layerTreeRoot()
    old = root.findGroup(group_name)
    if old is not None:
        root.removeChildNode(old)

    if not kml_path.exists():
        print(f"KML file not found: {kml_path}")
        return []

    group = root.insertGroup(0, group_name)
    loaded: list[QgsVectorLayer] = []

    probe = QgsVectorLayer(str(kml_path), kml_path.stem, "ogr")
    if not probe.isValid():
        print(f"KML could not be opened: {kml_path}")
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
    from_key = _month_key(FROM_MMYYYY)
    to_key = _month_key(TO_MMYYYY)
    if to_key < from_key:
        raise ValueError("TO_MMYYYY must be >= FROM_MMYYYY")

    project = QgsProject.instance()
    root = project.layerTreeRoot()

    if CLEAR_PROJECT:
        project.removeAllMapLayers()

    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

    old_group = root.findGroup(GROUP_NAME)
    if old_group is not None:
        root.removeChildNode(old_group)
    master = root.addGroup(GROUP_NAME)

    # 1) Topography
    g1 = master.addGroup("01 Topography")
    if not TOPO_RASTER.exists():
        raise FileNotFoundError(f"Topography raster not found: {TOPO_RASTER}")
    topo = _add_raster(g1, TOPO_RASTER, "Topography grayscale")
    _style_grayscale(topo, TOPO_OPACITY)

    if ADD_CONTOURS and CONTOUR_PATH.exists():
        contours = QgsVectorLayer(str(CONTOUR_PATH), "Contours 1m", "ogr")
        if contours.isValid():
            project.addMapLayer(contours, False)
            g1.addLayer(contours)

    # 2) Satellite base
    g2 = master.addGroup("02 Satellite Base")
    s2_path = S2_DIR / f"s2_truecolor_{S2_YEAR:04d}-{S2_MONTH:02d}.tif"
    if not s2_path.exists():
        raise FileNotFoundError(f"S2 base not found: {s2_path}")
    s2 = _add_raster(g2, s2_path, f"S2 TrueColor {S2_YEAR:04d}-{S2_MONTH:02d} (cloud-minimized)")
    _style_truecolor(s2, S2_OPACITY)

    # 3) Water
    g3 = master.addGroup("03 Water Permanent + Chronological")
    permanent = EVOLUTION_DIR / "derived" / "permanent_water_mask.tif"
    freq = EVOLUTION_DIR / "derived" / "water_frequency_fraction.tif"
    if permanent.exists():
        lyr = _add_raster(g3, permanent, "Permanent water (2025)")
        _style_binary_mask(lyr, "#08306b", PERMANENT_OPACITY, "Permanent")
    if freq.exists():
        lyr = _add_raster(g3, freq, "Water frequency fraction (2025)")
        _style_frequency_fraction(lyr, FREQUENCY_OPACITY)

    masks: list[tuple[str, Path]] = []
    if LOAD_MONTHLY_MASKS:
        month_group = g3.addGroup("Monthly water masks (oldest to newest)")
        masks = _sorted_masks(EVOLUTION_DIR / "masks", MASK_RE, from_key, to_key)
        if not masks:
            raise FileNotFoundError(f"No monthly masks found in {EVOLUTION_DIR / 'masks'} for {FROM_MMYYYY}..{TO_MMYYYY}")

    month_palette = [
        "#d8f3ff",
        "#c7ebff",
        "#b6e3ff",
        "#a5dbff",
        "#94d3ff",
        "#83cbff",
        "#72c3ff",
        "#61bbff",
        "#50b3ff",
        "#3fa7f5",
        "#2f95dd",
        "#1f7bbf",
    ]

    if LOAD_MONTHLY_MASKS:
        visible_only = masks[-1][0] if SHOW_ONLY_LAST_MONTH else None
        for idx, (date_str, path) in enumerate(masks):
            month_color = month_palette[min(idx, len(month_palette) - 1)]
            lyr = _add_raster(month_group, path, f"Water {date_str}")
            _style_binary_mask(lyr, month_color, WATER_MASK_OPACITY, date_str)
            node = month_group.findLayer(lyr.id())
            if node is not None and visible_only is not None:
                node.setItemVisibilityChecked(date_str == visible_only)

    if LOAD_OVERFLOW:
        overflow_group = g3.addGroup("Overflow masks")
        overflows = _sorted_masks(EVOLUTION_DIR / "overflow", OVERFLOW_RE, from_key, to_key)
        for date_str, path in overflows:
            lyr = _add_raster(overflow_group, path, f"Overflow {date_str}")
            _style_binary_mask(lyr, "#f46d43", OVERFLOW_OPACITY, f"Overflow {date_str}")

    # Force visual stack inside water group (top->bottom):
    # Monthly temporal masks, then permanent/frequency below.
    if LOAD_MONTHLY_MASKS:
        month_node = g3.findGroup("Monthly water masks (oldest to newest)")
        if month_node is not None:
            # Keep chronological order top->bottom: oldest first, newest last.
            # Move in reverse so final visual order remains oldest->newest.
            for _, path in reversed(masks):
                layer_name = f"Water {path.stem.replace('water_mask_', '')}"
                for child in month_node.children():
                    if getattr(child, "name", lambda: "")() == layer_name:
                        clone = child.clone()
                        month_node.insertChildNode(0, clone)
                        month_node.removeChildNode(child)
                        break

    # 4) Recommended overlays
    g4 = master.addGroup("04 Recommended overlays")
    if LOAD_SURFACE_WATER_OCCURRENCE and SURFACE_WATER_OCCURRENCE_PATH.exists():
        occ = _add_raster(g4, SURFACE_WATER_OCCURRENCE_PATH, "JRC surface water occurrence")
        _style_frequency_fraction(occ, SURFACE_WATER_OCCURRENCE_OPACITY)

    if KML_PATH:
        loaded = _load_kml_folders(Path(KML_PATH), KML_GROUP_NAME)
        print(f"KML folders loaded: {len(loaded)}")

    # Force global stack (top->bottom):
    # 04 overlays, 03 water, 02 satellite, 01 topography.
    _move_group_top(master, "01 Topography")
    _move_group_top(master, "02 Satellite Base")
    _move_group_top(master, "03 Water Permanent + Chronological")
    _move_group_top(master, "04 Recommended overlays")

    if ZOOM_TO_RESULT:
        iface.mapCanvas().setExtent(s2.extent())
        iface.mapCanvas().refresh()

    print(f"Group loaded: {GROUP_NAME}")
    print(f"Topography: {TOPO_RASTER}")
    print(f"Satellite base: {s2_path}")
    print(f"Water masks loaded: {len(masks)}")
    print(f"Evolution source: {EVOLUTION_DIR}")


main()

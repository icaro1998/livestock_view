from __future__ import annotations

import re
from pathlib import Path

from qgis.PyQt.QtCore import QTimer
from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsColorRampShader,
    QgsCoordinateReferenceSystem,
    QgsProject,
    QgsRasterLayer,
    QgsRasterShader,
    QgsSingleBandPseudoColorRenderer,
    QgsVectorLayer,
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
EVOLUTION_DIR = Path(
    globals().get("EVOLUTION_DIR", str(BASE / "output" / "flood_2025" / "water_evolution"))
)
VIEW_MODE = str(globals().get("VIEW_MODE", "single")).lower()  # single | stack
AUTO_START_ANIMATION = bool(globals().get("AUTO_START_ANIMATION", False))
ANIMATION_MS = int(globals().get("ANIMATION_MS", 900))
PLAYBACK_SPEED_MULT = float(globals().get("PLAYBACK_SPEED_MULT", 1.0))
MASK_OPACITY = float(globals().get("MASK_OPACITY", 0.65))
MASK_KIND = str(globals().get("MASK_KIND", "water")).lower()  # water | overflow
LOAD_FREQUENCY = bool(globals().get("LOAD_FREQUENCY", True))
FREQUENCY_MODE = str(globals().get("FREQUENCY_MODE", "permanent")).lower()  # permanent | gradient
PERMANENT_MIN_MONTHS = int(globals().get("PERMANENT_MIN_MONTHS", 10))
PERMANENT_COLOR = str(globals().get("PERMANENT_COLOR", "#084081"))
SEASONAL_COLOR = str(globals().get("SEASONAL_COLOR", "#41b6c4"))
OVERFLOW_COLOR = str(globals().get("OVERFLOW_COLOR", "#f46d43"))
KML_PATH = str(globals().get("KML_PATH", "")).strip()
KML_GROUP_NAME = str(globals().get("KML_GROUP_NAME", "Study area (KML folders)"))
FROM_MMYYYY = str(globals().get("FROM_MMYYYY", "")).strip()
TO_MMYYYY = str(globals().get("TO_MMYYYY", "")).strip()
CLEAR_PROJECT = bool(globals().get("CLEAR_PROJECT", False))
GROUP_NAME = str(globals().get("GROUP_NAME", "Water evolution"))

MASK_RE = re.compile(r"water_mask_(\d{4}-\d{2}-\d{2})\.tif$")
OVERFLOW_RE = re.compile(r"overflow_mask_(\d{4}-\d{2}-\d{2})\.tif$")


def _remove_group(name: str) -> None:
    root = QgsProject.instance().layerTreeRoot()
    group = root.findGroup(name)
    if group is not None:
        root.removeChildNode(group)


def _apply_mask_style(layer: QgsRasterLayer, opacity: float, color_hex: str, legend_label: str) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "No water"),
            QgsColorRampShader.ColorRampItem(0.5, QColor(255, 255, 255, 0), ""),
            QgsColorRampShader.ColorRampItem(1.0, QColor(color_hex), legend_label),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


def _apply_frequency_style(layer: QgsRasterLayer) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(-1.0, QColor(255, 255, 255, 0), "No data"),
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "0 months"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#deebf7"), "1"),
            QgsColorRampShader.ColorRampItem(3.0, QColor("#9ecae1"), "3"),
            QgsColorRampShader.ColorRampItem(6.0, QColor("#6baed6"), "6"),
            QgsColorRampShader.ColorRampItem(9.0, QColor("#3182bd"), "9"),
            QgsColorRampShader.ColorRampItem(12.0, QColor("#08519c"), "12"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(0.95)
    layer.setRenderer(renderer)


def _apply_permanent_style(layer: QgsRasterLayer, permanent_min_months: int, max_months: int) -> None:
    # Show only persistent water as solid color; everything else transparent.
    if permanent_min_months < 1:
        permanent_min_months = 1
    if max_months < permanent_min_months:
        max_months = permanent_min_months

    threshold_edge = float(permanent_min_months) - 0.01
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Discrete)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(-1.0, QColor(255, 255, 255, 0), "No data"),
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "No water"),
            QgsColorRampShader.ColorRampItem(threshold_edge, QColor(255, 255, 255, 0), "Seasonal"),
            QgsColorRampShader.ColorRampItem(
                float(permanent_min_months), QColor(PERMANENT_COLOR), "Permanent water"
            ),
            QgsColorRampShader.ColorRampItem(float(max_months), QColor(PERMANENT_COLOR), "Permanent water"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(0.95)
    layer.setRenderer(renderer)


def _apply_permanent_binary_style(layer: QgsRasterLayer) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Discrete)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "Non permanent"),
            QgsColorRampShader.ColorRampItem(1.0, QColor(PERMANENT_COLOR), "Permanent water"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(0.95)
    layer.setRenderer(renderer)


def _effective_interval_ms(interval_ms: int, speed_mult: float) -> int:
    if speed_mult <= 0:
        speed_mult = 1.0
    return max(80, int(interval_ms / speed_mult))


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


def _parse_sublayer_descriptor(descriptor: str) -> tuple[str | None, str]:
    # Common forms:
    # - "0!!::!!LayerName!!::!!0!!::!!Point"
    # - "0!!::LayerName"
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


def stop_water_animation() -> None:
    state = globals().get("_WATER_ANIM_STATE")
    if not state:
        return
    timer = state.get("timer")
    if timer is not None:
        timer.stop()
        timer.deleteLater()
    state["timer"] = None
    print("Water animation stopped.")


def start_water_animation(
    interval_ms: int = 900, loop: bool = True, speed_mult: float = 1.0
) -> None:
    state = globals().get("_WATER_ANIM_STATE")
    if not state or not state.get("mask_layers"):
        print("No mask layers loaded for animation.")
        return

    stop_water_animation()
    root = QgsProject.instance().layerTreeRoot()

    def _step() -> None:
        layers = state["mask_layers"]
        labels = state["labels"]
        alive_layers = []
        alive_labels = []
        for i, lyr in enumerate(layers):
            try:
                node = root.findLayer(lyr.id())
            except RuntimeError:
                node = None
            if node is None:
                continue
            alive_layers.append(lyr)
            alive_labels.append(labels[i] if i < len(labels) else f"{i+1}")

        if not alive_layers:
            stop_water_animation()
            print("Water animation stopped: layers were removed from project.")
            return

        state["mask_layers"] = alive_layers
        state["labels"] = alive_labels
        layers = alive_layers
        labels = alive_labels
        idx = int(state.get("idx", 0)) % len(layers)

        for i, lyr in enumerate(layers):
            node = root.findLayer(lyr.id())
            if node is not None:
                node.setItemVisibilityChecked(i == idx)

        iface.mainWindow().statusBar().showMessage(f"Water mask month: {labels[idx]}", 1000)
        iface.mapCanvas().refresh()

        if idx >= len(layers) - 1:
            if loop:
                state["idx"] = 0
            else:
                stop_water_animation()
                return
        else:
            state["idx"] = idx + 1

    timer = QTimer()
    timer.timeout.connect(_step)
    state["timer"] = timer
    _step()
    effective_ms = _effective_interval_ms(interval_ms, speed_mult)
    timer.start(effective_ms)
    print(
        f"Water animation started ({len(state['mask_layers'])} layers, "
        f"{effective_ms} ms/frame effective, speed x{speed_mult:.2f})."
    )


def main() -> None:
    # Defensive: if a previous timer is still active and layers were replaced,
    # stop it before rebuilding groups/layers.
    stop_water_animation()

    if VIEW_MODE not in {"single", "stack"}:
        raise ValueError("VIEW_MODE must be 'single' or 'stack'.")
    if MASK_KIND not in {"water", "overflow"}:
        raise ValueError("MASK_KIND must be 'water' or 'overflow'.")

    if MASK_KIND == "overflow":
        masks_dir = EVOLUTION_DIR / "overflow"
        mask_re = OVERFLOW_RE
        layer_prefix = "Overflow"
        mask_color = OVERFLOW_COLOR
    else:
        masks_dir = EVOLUTION_DIR / "masks"
        mask_re = MASK_RE
        layer_prefix = "Water mask"
        mask_color = SEASONAL_COLOR

    derived_dir = EVOLUTION_DIR / "derived"
    if not masks_dir.exists():
        raise FileNotFoundError(f"Missing folder for MASK_KIND={MASK_KIND}: {masks_dir}")

    mask_files = []
    for p in sorted(masks_dir.glob("*.tif")):
        m = mask_re.match(p.name)
        if m:
            mask_files.append((m.group(1), p))
    if not mask_files:
        raise FileNotFoundError(f"No mask files found in: {masks_dir}")
    mask_files = sorted(mask_files, key=lambda t: t[0])  # oldest -> newest

    if FROM_MMYYYY or TO_MMYYYY:
        start_key = _month_key(FROM_MMYYYY) if FROM_MMYYYY else None
        end_key = _month_key(TO_MMYYYY) if TO_MMYYYY else None
        selected = []
        for label, path in mask_files:
            mk = _date_to_month_key(label)
            if start_key is not None and mk < start_key:
                continue
            if end_key is not None and mk > end_key:
                continue
            selected.append((label, path))
        mask_files = selected
        if not mask_files:
            raise FileNotFoundError(
                f"No mask files after applying range filter FROM_MMYYYY={FROM_MMYYYY} TO_MMYYYY={TO_MMYYYY}"
            )

    if CLEAR_PROJECT:
        QgsProject.instance().removeAllMapLayers()

    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

    _remove_group(GROUP_NAME)
    root = project.layerTreeRoot()
    group = root.addGroup(GROUP_NAME)

    canvas = iface.mapCanvas()
    previous_render_state = canvas.renderFlag()
    canvas.setRenderFlag(False)

    freq_layer = None
    kml_layers: list[QgsVectorLayer] = []
    loaded_masks = []
    labels = []
    total_months = len(mask_files)
    try:
        if LOAD_FREQUENCY:
            if FREQUENCY_MODE == "permanent":
                perm_path = derived_dir / "permanent_water_mask.tif"
                if perm_path.exists():
                    freq_layer = QgsRasterLayer(str(perm_path), "Permanent water", "gdal")
                    if freq_layer.isValid():
                        _apply_permanent_binary_style(freq_layer)
                        project.addMapLayer(freq_layer, False)
                        group.insertLayer(0, freq_layer)
                        freq_layer.triggerRepaint()
                else:
                    freq_path = derived_dir / "water_frequency_months.tif"
                    if freq_path.exists():
                        freq_layer = QgsRasterLayer(str(freq_path), "Water frequency (months)", "gdal")
                        if freq_layer.isValid():
                            _apply_permanent_style(
                                freq_layer,
                                permanent_min_months=PERMANENT_MIN_MONTHS,
                                max_months=max(total_months, PERMANENT_MIN_MONTHS),
                            )
                            project.addMapLayer(freq_layer, False)
                            group.insertLayer(0, freq_layer)
                            freq_layer.triggerRepaint()
                    else:
                        print(f"Frequency layer not found: {freq_path}")
            else:
                freq_path = derived_dir / "water_frequency_months.tif"
                if freq_path.exists():
                    freq_layer = QgsRasterLayer(str(freq_path), "Water frequency (months)", "gdal")
                    if freq_layer.isValid():
                        _apply_frequency_style(freq_layer)
                        project.addMapLayer(freq_layer, False)
                        group.insertLayer(0, freq_layer)
                        freq_layer.triggerRepaint()
                else:
                    print(f"Frequency layer not found: {freq_path}")

        for label, path in mask_files:
            layer = QgsRasterLayer(str(path), f"{layer_prefix} {label}", "gdal")
            if not layer.isValid():
                print(f"Skipped invalid mask: {path}")
                continue
            _apply_mask_style(
                layer, MASK_OPACITY, color_hex=mask_color, legend_label=layer_prefix
            )
            project.addMapLayer(layer, False)
            group.addLayer(layer)
            layer.triggerRepaint()
            loaded_masks.append(layer)
            labels.append(label)

            node = root.findLayer(layer.id())
            if node is not None and VIEW_MODE == "single":
                node.setItemVisibilityChecked(False)

        if not loaded_masks:
            raise RuntimeError("No valid mask layers loaded.")

        if freq_layer is not None:
            freq_node = root.findLayer(freq_layer.id())
            if freq_node is not None:
                freq_node.setItemVisibilityChecked(True)

        if VIEW_MODE == "single":
            last = len(loaded_masks) - 1
            for i, lyr in enumerate(loaded_masks):
                node = root.findLayer(lyr.id())
                if node is not None:
                    node.setItemVisibilityChecked(i == last)
        canvas.setExtent(loaded_masks[-1].extent())

        if KML_PATH:
            kml_layers = _load_kml_folders(Path(KML_PATH), KML_GROUP_NAME)
    finally:
        canvas.setRenderFlag(previous_render_state)
        canvas.refresh()

    globals()["_WATER_ANIM_STATE"] = {
        "mask_layers": loaded_masks,
        "labels": labels,
        "idx": 0,
        "timer": None,
        "group_name": GROUP_NAME,
        "freq_layer": freq_layer,
        "kml_layers": kml_layers,
    }

    print(
        f"Loaded {len(loaded_masks)} water mask layers from {masks_dir}. "
        f"Mode={VIEW_MODE}. MASK_KIND={MASK_KIND}. Group='{GROUP_NAME}'."
    )
    if loaded_masks:
        print(f"Date order: {labels[0]} -> {labels[-1]}")
    if LOAD_FREQUENCY:
        if FREQUENCY_MODE == "permanent":
            print(
                f"Permanent-water layer loaded (if present): months >= {PERMANENT_MIN_MONTHS}"
            )
        else:
            print("Frequency layer loaded (if present).")
    if KML_PATH:
        print(f"KML layers loaded: {len(kml_layers)} from {KML_PATH}")
    if VIEW_MODE == "single":
        print(
            f"Animate with: start_water_animation(interval_ms=900, loop=True, speed_mult={PLAYBACK_SPEED_MULT})"
        )
        if AUTO_START_ANIMATION:
            start_water_animation(
                interval_ms=ANIMATION_MS,
                loop=True,
                speed_mult=PLAYBACK_SPEED_MULT,
            )


main()

from __future__ import annotations

import csv
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
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
MOTION_DIR = Path(globals().get("MOTION_DIR", str(BASE / "output" / "flood_motion" / "mvp")))
MANIFEST_CSV = Path(globals().get("MANIFEST_CSV", str(MOTION_DIR / "06_qgis" / "timelapse_manifest.csv")))
GROUP_NAME = str(globals().get("GROUP_NAME", "Water Motion MVP"))
VIEW_MODE = str(globals().get("VIEW_MODE", "single")).lower()  # single | stack
SHOW_CHANGE = bool(globals().get("SHOW_CHANGE", True))
MASK_OPACITY = float(globals().get("MASK_OPACITY", 0.72))
CHANGE_OPACITY = float(globals().get("CHANGE_OPACITY", 0.85))
AUTO_START_ANIMATION = bool(globals().get("AUTO_START_ANIMATION", False))
ANIMATION_MS = int(globals().get("ANIMATION_MS", 700))


def _apply_water_style(layer: QgsRasterLayer, opacity: float) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Discrete)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "No water"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#08306b"), "Water"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


def _apply_change_style(layer: QgsRasterLayer, opacity: float) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Discrete)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(-1.0, QColor("#f46d43"), "Water loss"),
            QgsColorRampShader.ColorRampItem(0.0, QColor(255, 255, 255, 0), "Stable"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#00e5ff"), "Water gain"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


def _remove_group(name: str) -> None:
    root = QgsProject.instance().layerTreeRoot()
    g = root.findGroup(name)
    if g is not None:
        root.removeChildNode(g)


def stop_motion_animation() -> None:
    state = globals().get("_MOTION_ANIM_STATE")
    if not state:
        return
    timer = state.get("timer")
    if timer is not None:
        timer.stop()
        timer.deleteLater()
    state["timer"] = None
    print("Motion animation stopped.")


def start_motion_animation(interval_ms: int = 700, loop: bool = True) -> None:
    state = globals().get("_MOTION_ANIM_STATE")
    if not state or not state.get("mask_layers"):
        print("No layers loaded for animation.")
        return

    stop_motion_animation()
    root = QgsProject.instance().layerTreeRoot()

    def _step() -> None:
        mask_layers = state["mask_layers"]
        change_layers = state["change_layers"]
        labels = state["labels"]

        alive_masks = []
        alive_labels = []
        for i, lyr in enumerate(mask_layers):
            try:
                node = root.findLayer(lyr.id())
            except RuntimeError:
                node = None
            if node is None:
                continue
            alive_masks.append(lyr)
            alive_labels.append(labels[i] if i < len(labels) else f"{i+1}")

        if not alive_masks:
            stop_motion_animation()
            print("Motion animation stopped: all water layers were removed.")
            return

        alive_changes = []
        for lyr in change_layers:
            try:
                node = root.findLayer(lyr.id())
            except RuntimeError:
                node = None
            if node is not None:
                alive_changes.append(lyr)

        state["mask_layers"] = alive_masks
        state["change_layers"] = alive_changes
        state["labels"] = alive_labels
        mask_layers = alive_masks
        change_layers = alive_changes
        labels = alive_labels

        idx = int(state.get("idx", 0)) % len(mask_layers)

        for i, lyr in enumerate(mask_layers):
            node = root.findLayer(lyr.id())
            if node is not None:
                node.setItemVisibilityChecked(i == idx)

        for i, lyr in enumerate(change_layers):
            node = root.findLayer(lyr.id())
            if node is not None:
                node.setItemVisibilityChecked(i == idx)

        iface.mainWindow().statusBar().showMessage(f"Water motion date: {labels[idx]}", 1000)
        iface.mapCanvas().refresh()

        if idx >= len(mask_layers) - 1:
            if loop:
                state["idx"] = 0
            else:
                stop_motion_animation()
                return
        else:
            state["idx"] = idx + 1

    timer = QTimer()
    timer.timeout.connect(_step)
    state["timer"] = timer
    _step()
    timer.start(max(120, int(interval_ms)))
    print(f"Motion animation started ({len(state['mask_layers'])} frames, {interval_ms} ms/frame).")


def main() -> None:
    if VIEW_MODE not in {"single", "stack"}:
        raise ValueError("VIEW_MODE must be 'single' or 'stack'.")
    if not MANIFEST_CSV.exists():
        raise FileNotFoundError(f"Manifest not found: {MANIFEST_CSV}")

    rows: list[dict[str, str]] = []
    with MANIFEST_CSV.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = [row for row in r if row.get("fused_path")]
    if not rows:
        raise RuntimeError("Manifest has no rows with fused_path.")

    rows.sort(key=lambda x: x.get("date", ""))

    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
    _remove_group(GROUP_NAME)
    root = project.layerTreeRoot()
    group = root.addGroup(GROUP_NAME)

    mask_layers: list[QgsRasterLayer] = []
    change_layers: list[QgsRasterLayer] = []
    labels: list[str] = []

    canvas = iface.mapCanvas()
    prev_flag = canvas.renderFlag()
    canvas.setRenderFlag(False)
    try:
        for row in rows:
            date = row.get("date", "")
            fused_path = row.get("fused_path", "")
            if not fused_path:
                continue
            lyr = QgsRasterLayer(fused_path, f"Water {date}", "gdal")
            if not lyr.isValid():
                continue
            _apply_water_style(lyr, MASK_OPACITY if VIEW_MODE == "single" else 0.24)
            project.addMapLayer(lyr, False)
            group.addLayer(lyr)
            mask_layers.append(lyr)
            labels.append(date)

            if SHOW_CHANGE and row.get("change_path"):
                ch = QgsRasterLayer(row["change_path"], f"Change {date}", "gdal")
                if ch.isValid():
                    _apply_change_style(ch, CHANGE_OPACITY if VIEW_MODE == "single" else 0.35)
                    project.addMapLayer(ch, False)
                    group.addLayer(ch)
                    change_layers.append(ch)

        if not mask_layers:
            raise RuntimeError("No valid fused layers were loaded.")

        if VIEW_MODE == "single":
            for i, lyr in enumerate(mask_layers):
                node = root.findLayer(lyr.id())
                if node is not None:
                    node.setItemVisibilityChecked(i == len(mask_layers) - 1)
            for lyr in change_layers:
                node = root.findLayer(lyr.id())
                if node is not None:
                    node.setItemVisibilityChecked(True)

        canvas.setExtent(mask_layers[-1].extent())
    finally:
        canvas.setRenderFlag(prev_flag)
        canvas.refresh()

    globals()["_MOTION_ANIM_STATE"] = {
        "mask_layers": mask_layers,
        "change_layers": change_layers,
        "labels": labels,
        "idx": 0,
        "timer": None,
    }
    print(f"Loaded {len(mask_layers)} water frames from {MANIFEST_CSV}")
    if AUTO_START_ANIMATION and VIEW_MODE == "single":
        start_motion_animation(interval_ms=ANIMATION_MS, loop=True)


main()

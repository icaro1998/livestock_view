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
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
FROM_MMYYYY = globals().get("FROM_MMYYYY", "01/2024")
TO_MMYYYY = globals().get("TO_MMYYYY", "12/2025")
VIEW_MODE = str(globals().get("VIEW_MODE", "single")).lower()  # "single" | "stack"
AUTO_START_ANIMATION = bool(globals().get("AUTO_START_ANIMATION", False))
ANIMATION_MS = int(globals().get("ANIMATION_MS", 900))
GROUP_NAME = str(
    globals().get("GROUP_NAME", f"Flood snapshots {FROM_MMYYYY} to {TO_MMYYYY}")
)
STACK_OPACITY = globals().get("STACK_OPACITY", None)

SNAP_RE = re.compile(r"s1_flood_diff_(\d{4})-(\d{2})-(\d{2})\.tif$")


class Snapshot:
    def __init__(self, path: Path, year: int, month: int, day: int, source_rank: int) -> None:
        self.path = path
        self.year = year
        self.month = month
        self.day = day
        self.source_rank = source_rank  # lower = preferred

    @property
    def label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def date_key(self) -> tuple[int, int, int]:
        return (self.year, self.month, self.day)

    @property
    def month_key(self) -> int:
        return self.year * 12 + self.month


def _parse_mm_yyyy(value: str) -> tuple[int, int]:
    parts = value.strip().split("/")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        raise ValueError(f"Invalid MM/YYYY value: {value!r}")
    month = int(parts[0])
    year = int(parts[1])
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month in MM/YYYY value: {value!r}")
    return year, month


def _month_key(year: int, month: int) -> int:
    return year * 12 + month


def _discover_snapshots() -> list[Snapshot]:
    preferred_dirs = [
        BASE / "output" / "flood_2025" / "snapshots",
        BASE / "output" / "flood" / "snapshots",
    ]

    best_by_date: dict[tuple[int, int, int], Snapshot] = {}
    for rank, folder in enumerate(preferred_dirs):
        if not folder.exists():
            continue
        for path in sorted(folder.glob("s1_flood_diff_*.tif")):
            m = SNAP_RE.match(path.name)
            if not m:
                continue
            snap = Snapshot(
                path=path,
                year=int(m.group(1)),
                month=int(m.group(2)),
                day=int(m.group(3)),
                source_rank=rank,
            )
            current = best_by_date.get(snap.date_key)
            if current is None or snap.source_rank < current.source_rank:
                best_by_date[snap.date_key] = snap
    return sorted(best_by_date.values(), key=lambda s: s.date_key)


def _recommended_stack_opacity(n: int) -> float:
    if n <= 0:
        return 0.2
    return max(0.08, min(0.30, round(1.2 / n, 2)))


def _apply_fallback_style(layer: QgsRasterLayer, opacity: float) -> None:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(-3.0, QColor("#08306b"), "-3"),
            QgsColorRampShader.ColorRampItem(-2.0, QColor("#2171b5"), "-2"),
            QgsColorRampShader.ColorRampItem(-1.0, QColor("#6baed6"), "-1"),
            QgsColorRampShader.ColorRampItem(-0.5, QColor("#c6dbef"), "-0.5"),
            QgsColorRampShader.ColorRampItem(0.0, QColor("#f7f7f7"), "0"),
            QgsColorRampShader.ColorRampItem(0.5, QColor("#fddbc7"), "0.5"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#f4a582"), "1"),
            QgsColorRampShader.ColorRampItem(2.0, QColor("#d6604d"), "2"),
            QgsColorRampShader.ColorRampItem(3.0, QColor("#b2182b"), "3"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)
    layer.setRenderer(renderer)


def _remove_existing_group(group_name: str) -> None:
    root = QgsProject.instance().layerTreeRoot()
    existing = root.findGroup(group_name)
    if existing is not None:
        root.removeChildNode(existing)


def stop_flood_animation() -> None:
    state = globals().get("_FLOOD_ANIM_STATE")
    if not state:
        return
    timer = state.get("timer")
    if timer is not None:
        timer.stop()
        timer.deleteLater()
    state["timer"] = None
    print("Flood animation stopped.")


def start_flood_animation(interval_ms: int = 900, loop: bool = True) -> None:
    state = globals().get("_FLOOD_ANIM_STATE")
    if not state or not state.get("layers"):
        print("No loaded layers available for animation.")
        return

    stop_flood_animation()
    root = QgsProject.instance().layerTreeRoot()

    def _step() -> None:
        idx = state["idx"]
        layers = state["layers"]
        labels = state["labels"]

        for i, lyr in enumerate(layers):
            node = root.findLayer(lyr.id())
            if node is not None:
                node.setItemVisibilityChecked(i == idx)

        iface.mainWindow().statusBar().showMessage(f"Flood month: {labels[idx]}", 1000)
        iface.mapCanvas().refresh()

        if idx >= len(layers) - 1:
            if loop:
                state["idx"] = 0
            else:
                stop_flood_animation()
                return
        else:
            state["idx"] = idx + 1

    timer = QTimer()
    timer.timeout.connect(_step)
    state["timer"] = timer
    _step()
    timer.start(max(120, int(interval_ms)))
    print(f"Flood animation started ({len(state['layers'])} layers, {interval_ms} ms/frame).")


def main() -> None:
    if VIEW_MODE not in {"single", "stack"}:
        raise ValueError("VIEW_MODE must be 'single' or 'stack'.")

    from_year, from_month = _parse_mm_yyyy(FROM_MMYYYY)
    to_year, to_month = _parse_mm_yyyy(TO_MMYYYY)
    start_key = _month_key(from_year, from_month)
    end_key = _month_key(to_year, to_month)
    if end_key < start_key:
        raise ValueError("TO_MMYYYY must be after or equal to FROM_MMYYYY.")

    all_snaps = _discover_snapshots()
    if not all_snaps:
        raise FileNotFoundError("No snapshot files found in output/flood*/snapshots.")

    selected = [s for s in all_snaps if start_key <= s.month_key <= end_key]
    if not selected:
        raise FileNotFoundError(
            f"No snapshots found in requested range {FROM_MMYYYY} -> {TO_MMYYYY}."
        )

    qml_path = BASE / "qgis" / "styles" / "flood_diff_diverging.qml"
    opacity_stack = (
        float(STACK_OPACITY) if STACK_OPACITY is not None else _recommended_stack_opacity(len(selected))
    )

    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
    _remove_existing_group(GROUP_NAME)
    root = project.layerTreeRoot()
    group = root.addGroup(GROUP_NAME)

    canvas = iface.mapCanvas()
    prev_render_flag = canvas.renderFlag()
    canvas.setRenderFlag(False)

    loaded_layers: list[QgsRasterLayer] = []
    loaded_labels: list[str] = []
    errors: list[str] = []
    try:
        for i, snap in enumerate(selected):
            name = f"Flood {snap.year:04d}-{snap.month:02d}"
            layer = QgsRasterLayer(str(snap.path), name, "gdal")
            if not layer.isValid():
                errors.append(str(snap.path))
                continue

            project.addMapLayer(layer, False)
            group.addLayer(layer)

            target_opacity = opacity_stack if VIEW_MODE == "stack" else 0.75

            styled_ok = False
            if qml_path.exists():
                styled_ok, msg = layer.loadNamedStyle(str(qml_path))
                if not styled_ok:
                    print(f"QML style load failed for {snap.path.name}: {msg}")
            if not styled_ok:
                _apply_fallback_style(layer, target_opacity)
            else:
                renderer = layer.renderer()
                if renderer is not None:
                    renderer.setOpacity(target_opacity)

            layer.triggerRepaint()
            loaded_layers.append(layer)
            loaded_labels.append(snap.label)

            node = root.findLayer(layer.id())
            if node is not None and VIEW_MODE == "single":
                node.setItemVisibilityChecked(False)

        if not loaded_layers:
            raise RuntimeError("No valid raster layers were loaded.")

        # In single mode, keep only the most recent month visible.
        if VIEW_MODE == "single":
            last_idx = len(loaded_layers) - 1
            for i, lyr in enumerate(loaded_layers):
                node = root.findLayer(lyr.id())
                if node is not None:
                    node.setItemVisibilityChecked(i == last_idx)
        canvas.setExtent(loaded_layers[-1].extent())
    finally:
        canvas.setRenderFlag(prev_render_flag)
        canvas.refresh()

    globals()["_FLOOD_ANIM_STATE"] = {
        "layers": loaded_layers,
        "labels": loaded_labels,
        "idx": 0,
        "timer": None,
        "group_name": GROUP_NAME,
    }

    print(
        f"Loaded {len(loaded_layers)} layers ({FROM_MMYYYY} -> {TO_MMYYYY}) in mode={VIEW_MODE}. "
        f"Group: {GROUP_NAME}"
    )
    if errors:
        print(f"Skipped invalid layers: {len(errors)}")
        for path in errors[:5]:
            print(f"  - {path}")
        if len(errors) > 5:
            print(f"  ... and {len(errors)-5} more")

    if VIEW_MODE == "single":
        print("You can animate now with: start_flood_animation(interval_ms=900, loop=True)")
    if AUTO_START_ANIMATION and VIEW_MODE == "single":
        start_flood_animation(interval_ms=ANIMATION_MS, loop=True)


main()

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from qgis.PyQt.QtGui import QColor, QPainter
from qgis.core import (
    QgsColorRampShader,
    QgsContrastEnhancement,
    QgsLayerTreeGroup,
    QgsLayerTreeLayer,
    QgsLineSymbol,
    QgsMultiBandColorRenderer,
    QgsProject,
    QgsRasterLayer,
    QgsRasterRange,
    QgsRasterShader,
    QgsSingleBandPseudoColorRenderer,
    QgsVectorLayer,
)


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
TARGET_GROUP = str(globals().get("TARGET_GROUP", "Flood 3-layer 2025 (Consistent)")).strip()
APPLY_TO_ALL = bool(globals().get("APPLY_TO_ALL", False))
LOG_DIR = Path(globals().get("LOG_DIR", str(BASE / "logs" / "qgis_style_changes")))
PROFILE_NAME = str(globals().get("PROFILE_NAME", "hologram_v1"))
DRY_RUN = bool(globals().get("DRY_RUN", False))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return _utc_now().strftime("%Y%m%d_%H%M%S")


def _record(
    rows: list[dict[str, str]],
    layer,
    dataset: str,
    setting: str,
    old_value: Any,
    new_value: Any,
    status: str = "ok",
    note: str = "",
) -> None:
    rows.append(
        {
            "time_utc": _utc_now().isoformat(),
            "profile": PROFILE_NAME,
            "layer_name": layer.name() if layer is not None else "",
            "layer_id": layer.id() if layer is not None else "",
            "dataset_type": dataset,
            "setting": setting,
            "old_value": "" if old_value is None else str(old_value),
            "new_value": "" if new_value is None else str(new_value),
            "status": status,
            "note": note,
        }
    )


def _collect_group_layers(group: QgsLayerTreeGroup) -> list:
    out = []
    for child in group.children():
        if isinstance(child, QgsLayerTreeLayer):
            lyr = child.layer()
            if lyr is not None:
                out.append(lyr)
        elif isinstance(child, QgsLayerTreeGroup):
            out.extend(_collect_group_layers(child))
    return out


def _classify(layer_name: str) -> str:
    n = layer_name.lower()
    if "sar water mask" in n or n.startswith("water mask"):
        return "sar_mask"
    if "dw water prob" in n or "dynamic world" in n:
        return "dynamic_world"
    if "s2 ndwi" in n or "ndwi" in n:
        return "s2_ndwi"
    if "truecolor" in n:
        return "s2_truecolor"
    if "permanent water" in n or "water frequency" in n:
        return "water_frequency"
    if "elevation" in n:
        return "terrain_elevation"
    if "slope" in n:
        return "terrain_slope"
    if "hillshade" in n:
        return "terrain_hillshade"
    if "contour" in n:
        return "terrain_contours"
    return "other"


def _blend_to_name(mode: QPainter.CompositionMode) -> str:
    names = {
        QPainter.CompositionMode_SourceOver: "normal",
        QPainter.CompositionMode_Screen: "screen",
        QPainter.CompositionMode_Lighten: "lighten",
        QPainter.CompositionMode_Multiply: "multiply",
        QPainter.CompositionMode_Overlay: "overlay",
    }
    return names.get(mode, f"mode_{int(mode)}")


def _set_blend(layer, mode: QPainter.CompositionMode, dataset: str, rows: list[dict[str, str]]) -> None:
    try:
        old_mode = layer.blendMode()
        if not DRY_RUN:
            layer.setBlendMode(mode)
        _record(
            rows,
            layer,
            dataset,
            "blend_mode",
            _blend_to_name(old_mode),
            _blend_to_name(mode),
        )
    except Exception as exc:
        _record(rows, layer, dataset, "blend_mode", None, _blend_to_name(mode), status="error", note=str(exc))


def _apply_singleband_style(
    layer: QgsRasterLayer,
    dataset: str,
    items: list[QgsColorRampShader.ColorRampItem],
    opacity: float,
    blend_mode: QPainter.CompositionMode,
    rows: list[dict[str, str]],
) -> None:
    old_renderer = None
    old_opacity = None
    try:
        if layer.renderer() is not None:
            old_renderer = layer.renderer().__class__.__name__
            old_opacity = layer.renderer().opacity()
    except Exception:
        pass

    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(items)
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(opacity)

    if not DRY_RUN:
        layer.setRenderer(renderer)
    _record(rows, layer, dataset, "renderer", old_renderer, renderer.__class__.__name__)
    _record(rows, layer, dataset, "renderer_opacity", old_opacity, opacity)
    _record(rows, layer, dataset, "color_ramp_items", len(items), len(items))
    _set_blend(layer, blend_mode, dataset, rows)

    if not DRY_RUN:
        layer.triggerRepaint()


def _style_sar_mask(layer: QgsRasterLayer, rows: list[dict[str, str]]) -> None:
    items = [
        QgsColorRampShader.ColorRampItem(0.00, QColor(0, 0, 0, 0), "dry"),
        QgsColorRampShader.ColorRampItem(0.49, QColor(0, 0, 0, 0), "dry"),
        QgsColorRampShader.ColorRampItem(0.55, QColor(186, 240, 255, 80), "low wet"),
        QgsColorRampShader.ColorRampItem(0.75, QColor(85, 213, 255, 150), "medium wet"),
        QgsColorRampShader.ColorRampItem(1.00, QColor(0, 84, 140, 235), "high wet"),
    ]
    _apply_singleband_style(
        layer,
        dataset="sar_mask",
        items=items,
        opacity=0.62,
        blend_mode=QPainter.CompositionMode_Screen,
        rows=rows,
    )


def _style_dynamic_world(layer: QgsRasterLayer, rows: list[dict[str, str]]) -> None:
    items = [
        QgsColorRampShader.ColorRampItem(0.00, QColor(0, 0, 0, 0), "0"),
        QgsColorRampShader.ColorRampItem(0.10, QColor(198, 231, 247, 30), "0.1"),
        QgsColorRampShader.ColorRampItem(0.25, QColor(159, 213, 239, 65), "0.25"),
        QgsColorRampShader.ColorRampItem(0.50, QColor(94, 174, 221, 125), "0.5"),
        QgsColorRampShader.ColorRampItem(0.75, QColor(33, 113, 181, 185), "0.75"),
        QgsColorRampShader.ColorRampItem(1.00, QColor(8, 69, 148, 245), "1.0"),
    ]
    _apply_singleband_style(
        layer,
        dataset="dynamic_world",
        items=items,
        opacity=0.54,
        blend_mode=QPainter.CompositionMode_Lighten,
        rows=rows,
    )


def _style_s2_ndwi(layer: QgsRasterLayer, rows: list[dict[str, str]]) -> None:
    items = [
        QgsColorRampShader.ColorRampItem(-1.00, QColor(0, 0, 0, 0), "-1"),
        QgsColorRampShader.ColorRampItem(0.00, QColor(0, 0, 0, 0), "0"),
        QgsColorRampShader.ColorRampItem(0.06, QColor(210, 240, 255, 45), "0.06"),
        QgsColorRampShader.ColorRampItem(0.15, QColor(125, 200, 248, 95), "0.15"),
        QgsColorRampShader.ColorRampItem(0.30, QColor(43, 140, 190, 165), "0.3"),
        QgsColorRampShader.ColorRampItem(0.50, QColor(4, 90, 141, 235), "0.5"),
        QgsColorRampShader.ColorRampItem(1.00, QColor(2, 56, 88, 255), "1"),
    ]
    _apply_singleband_style(
        layer,
        dataset="s2_ndwi",
        items=items,
        opacity=0.48,
        blend_mode=QPainter.CompositionMode_Screen,
        rows=rows,
    )


def _style_truecolor(layer: QgsRasterLayer, rows: list[dict[str, str]]) -> None:
    dataset = "s2_truecolor"
    provider = layer.dataProvider()

    for band in (1, 2, 3):
        try:
            if not DRY_RUN:
                provider.setUserNoDataValue(band, [QgsRasterRange(0.0, 0.0)])
            _record(rows, layer, dataset, f"band{band}_nodata", "as_is", "0..0")
        except Exception as exc:
            _record(rows, layer, dataset, f"band{band}_nodata", None, "0..0", status="error", note=str(exc))

    old_renderer = None
    try:
        if layer.renderer() is not None:
            old_renderer = layer.renderer().__class__.__name__
    except Exception:
        pass

    renderer = QgsMultiBandColorRenderer(provider, 1, 2, 3)
    for band, setter in (
        (1, renderer.setRedContrastEnhancement),
        (2, renderer.setGreenContrastEnhancement),
        (3, renderer.setBlueContrastEnhancement),
    ):
        try:
            ce = QgsContrastEnhancement(provider.dataType(band))
            ce.setContrastEnhancementAlgorithm(QgsContrastEnhancement.StretchToMinimumMaximum, True)
            ce.setMinimumValue(250.0)
            ce.setMaximumValue(3400.0)
            setter(ce)
            _record(rows, layer, dataset, f"band{band}_contrast", "as_is", "min=250,max=3400")
        except Exception as exc:
            _record(rows, layer, dataset, f"band{band}_contrast", None, "min=250,max=3400", status="error", note=str(exc))

    renderer.setOpacity(1.0)
    if not DRY_RUN:
        layer.setRenderer(renderer)
    _record(rows, layer, dataset, "renderer", old_renderer, renderer.__class__.__name__)
    _record(rows, layer, dataset, "renderer_opacity", "as_is", 1.0)
    _set_blend(layer, QPainter.CompositionMode_SourceOver, dataset, rows)

    if not DRY_RUN:
        layer.triggerRepaint()


def _style_water_frequency(layer: QgsRasterLayer, rows: list[dict[str, str]]) -> None:
    items = [
        QgsColorRampShader.ColorRampItem(0.0, QColor(0, 0, 0, 0), "0"),
        QgsColorRampShader.ColorRampItem(2.0, QColor(206, 226, 255, 80), "2"),
        QgsColorRampShader.ColorRampItem(5.0, QColor(127, 190, 255, 130), "5"),
        QgsColorRampShader.ColorRampItem(9.0, QColor(49, 130, 189, 190), "9"),
        QgsColorRampShader.ColorRampItem(12.0, QColor(8, 81, 156, 255), "12"),
    ]
    _apply_singleband_style(
        layer,
        dataset="water_frequency",
        items=items,
        opacity=0.72,
        blend_mode=QPainter.CompositionMode_Screen,
        rows=rows,
    )


def _style_contours(layer: QgsVectorLayer, rows: list[dict[str, str]]) -> None:
    dataset = "terrain_contours"
    try:
        symbol = QgsLineSymbol.createSimple(
            {
                "line_color": "#ffe082",
                "line_width": "0.35",
                "line_style": "solid",
            }
        )
        old_renderer = layer.renderer().__class__.__name__ if layer.renderer() is not None else None
        if not DRY_RUN:
            layer.renderer().setSymbol(symbol)
            layer.setOpacity(0.72)
        _record(rows, layer, dataset, "renderer", old_renderer, layer.renderer().__class__.__name__)
        _record(rows, layer, dataset, "layer_opacity", "as_is", 0.72)
        _set_blend(layer, QPainter.CompositionMode_Screen, dataset, rows)
        if not DRY_RUN:
            layer.triggerRepaint()
    except Exception as exc:
        _record(rows, layer, dataset, "style", None, "line_color=#ffe082,width=0.35", status="error", note=str(exc))


def _style_layer(layer, rows: list[dict[str, str]]) -> None:
    dataset = _classify(layer.name())
    if isinstance(layer, QgsRasterLayer):
        if dataset == "sar_mask":
            _style_sar_mask(layer, rows)
        elif dataset == "dynamic_world":
            _style_dynamic_world(layer, rows)
        elif dataset == "s2_ndwi":
            _style_s2_ndwi(layer, rows)
        elif dataset == "s2_truecolor":
            _style_truecolor(layer, rows)
        elif dataset == "water_frequency":
            _style_water_frequency(layer, rows)
        else:
            _record(rows, layer, dataset, "style", "unchanged", "unchanged", status="skip", note="no profile rule")
    elif isinstance(layer, QgsVectorLayer):
        if dataset == "terrain_contours":
            _style_contours(layer, rows)
        else:
            _record(rows, layer, dataset, "style", "unchanged", "unchanged", status="skip", note="no profile rule")
    else:
        _record(rows, layer, dataset, "style", "unchanged", "unchanged", status="skip", note="unsupported layer type")


def _write_logs(rows: list[dict[str, str]]) -> tuple[Path, Path]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _ts()
    csv_path = LOG_DIR / f"qgis_style_changes_{stamp}.csv"
    txt_path = LOG_DIR / f"qgis_style_changes_{stamp}.log"

    headers = [
        "time_utc",
        "profile",
        "layer_name",
        "layer_id",
        "dataset_type",
        "setting",
        "old_value",
        "new_value",
        "status",
        "note",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["dataset_type"]] = counts.get(r["dataset_type"], 0) + 1

    lines = []
    lines.append(f"QGIS style changes log ({PROFILE_NAME})")
    lines.append(f"UTC: {_utc_now().isoformat()}")
    lines.append(f"rows: {len(rows)}")
    lines.append("")
    lines.append("Rows by dataset_type:")
    for key in sorted(counts):
        lines.append(f"- {key}: {counts[key]}")
    lines.append("")
    lines.append(f"CSV: {csv_path}")
    lines.append(f"TXT: {txt_path}")
    txt_path.write_text("\n".join(lines), encoding="utf-8")
    return csv_path, txt_path


def main() -> None:
    project = QgsProject.instance()
    root = project.layerTreeRoot()

    if APPLY_TO_ALL:
        layers = list(project.mapLayers().values())
        source_desc = "all project layers"
    else:
        group = root.findGroup(TARGET_GROUP)
        if group is None:
            raise RuntimeError(f"Group not found: {TARGET_GROUP}")
        layers = _collect_group_layers(group)
        source_desc = f"group '{TARGET_GROUP}'"

    if not layers:
        raise RuntimeError(f"No layers found in {source_desc}.")

    rows: list[dict[str, str]] = []
    for lyr in layers:
        _style_layer(lyr, rows)

    csv_path, txt_path = _write_logs(rows)
    iface.mapCanvas().refresh()
    print(f"Hologram profile applied to {len(layers)} layers from {source_desc}.")
    print(f"Log CSV: {csv_path}")
    print(f"Log TXT: {txt_path}")
    if DRY_RUN:
        print("DRY_RUN=True: settings were logged but not applied.")


main()

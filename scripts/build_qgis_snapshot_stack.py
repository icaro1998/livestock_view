#!/usr/bin/env python3
"""
Build a QGIS project with one layer per monthly snapshot for a selected month range.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path


SNAPSHOT_RE = re.compile(r"s1_flood_diff_(\d{4})-(\d{2})-(\d{2})\.tif$")


@dataclass(frozen=True)
class SnapshotFile:
    path: Path
    year: int
    month: int
    day: int

    @property
    def month_label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def month_key(self) -> int:
        return self.year * 12 + (self.month - 1)


def _parse_mm_yyyy(raw: str) -> tuple[int, int]:
    value = raw.strip()
    parts = value.split("/")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        raise ValueError("Expected MM/YYYY format.")
    month = int(parts[0])
    year = int(parts[1])
    if month < 1 or month > 12:
        raise ValueError("Month must be 01-12.")
    return year, month


def _discover_snapshots(snapshot_dir: Path) -> list[SnapshotFile]:
    snapshots: list[SnapshotFile] = []
    if not snapshot_dir.exists():
        return snapshots
    for path in sorted(snapshot_dir.glob("s1_flood_diff_*.tif")):
        match = SNAPSHOT_RE.match(path.name)
        if not match:
            continue
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        snapshots.append(SnapshotFile(path=path, year=year, month=month, day=day))
    return snapshots


def _month_key(year: int, month: int) -> int:
    return year * 12 + (month - 1)


def _recommended_opacity(layer_count: int) -> float:
    # Keep cumulative overlay readable as month count changes.
    if layer_count <= 0:
        return 0.12
    value = round(1.2 / layer_count, 2)
    return max(0.08, min(0.30, value))


def _build_qgs(
    selected: list[SnapshotFile],
    output_path: Path,
    title: str,
    group_name: str,
) -> None:
    opacity = _recommended_opacity(len(selected))
    qgs_dir = output_path.parent.resolve()

    lines: list[str] = []
    lines.append('<qgis version="3.40.9" projectname="livestock_flood_range_stack">')
    lines.append(f"  <title>{title}</title>")
    lines.append("  <projectCrs>")
    lines.append("    <spatialrefsys>")
    lines.append("      <authid>OGC:CRS84</authid>")
    lines.append("      <description>WGS 84 (CRS84 lon/lat)</description>")
    lines.append("      <proj4>+proj=longlat +datum=WGS84 +no_defs</proj4>")
    lines.append("      <srsid>3452</srsid>")
    lines.append("    </spatialrefsys>")
    lines.append("  </projectCrs>")
    lines.append('  <mapcanvas annotationsVisible="1" name="theMapCanvas">')
    lines.append("    <units>degrees</units>")
    lines.append("    <extent>")
    lines.append("      <xmin>-64.11153603146758000</xmin>")
    lines.append("      <ymin>-13.88018610194003700</ymin>")
    lines.append("      <xmax>-63.74340811195626000</xmax>")
    lines.append("      <ymax>-13.52041247635613500</ymax>")
    lines.append("    </extent>")
    lines.append("    <rotation>0</rotation>")
    lines.append("  </mapcanvas>")
    lines.append("")
    lines.append(f'  <layer-tree-group name="{group_name}">')
    for snap in selected:
        layer_id = f"s1_flood_diff_{snap.year:04d}_{snap.month:02d}"
        layer_name = f"S1_Flood_Diff_{snap.month_label}"
        lines.append(
            f'    <layer-tree-layer id="{layer_id}" name="{layer_name}" checked="Qt::Checked"/>'
        )
    lines.append("  </layer-tree-group>")
    lines.append("")
    lines.append("  <projectlayers>")

    for snap in selected:
        layer_id = f"s1_flood_diff_{snap.year:04d}_{snap.month:02d}"
        layer_name = f"S1_Flood_Diff_{snap.month_label}"
        ds_path = snap.path.resolve()
        ds_rel_text = os.path.relpath(str(ds_path), str(qgs_dir)).replace("\\", "/")

        lines.append(f'    <maplayer type="raster" name="{layer_name}" id="{layer_id}">')
        lines.append(f"      <datasource>{ds_rel_text}</datasource>")
        lines.append("      <provider>gdal</provider>")
        lines.append(f"      <layername>{layer_name}</layername>")
        lines.append("      <extent>")
        lines.append("        <xmin>-64.11153603146758000</xmin>")
        lines.append("        <ymin>-13.88018610194003700</ymin>")
        lines.append("        <xmax>-63.74340811195626000</xmax>")
        lines.append("        <ymax>-13.52041247635613500</ymax>")
        lines.append("      </extent>")
        lines.append(
            f'      <rasterrenderer type="singlebandpseudocolor" opacity="{opacity:.2f}" alphaBand="-1" band="1">'
        )
        lines.append("        <rastershader>")
        lines.append('          <colorrampshader colorRampType="INTERPOLATED" classificationMode="1" clip="0">')
        lines.append('            <item color="#08306b" value="-4" label="-4" alpha="255"/>')
        lines.append('            <item color="#08519c" value="-3" label="-3" alpha="255"/>')
        lines.append('            <item color="#2171b5" value="-2" label="-2" alpha="255"/>')
        lines.append('            <item color="#6baed6" value="-1" label="-1" alpha="255"/>')
        lines.append('            <item color="#c6dbef" value="-0.5" label="-0.5" alpha="180"/>')
        lines.append('            <item color="#f7fbff" value="0" label="0" alpha="0"/>')
        lines.append('            <item color="#f7fbff" value="1" label="1" alpha="0"/>')
        lines.append("          </colorrampshader>")
        lines.append("        </rastershader>")
        lines.append("      </rasterrenderer>")
        lines.append("    </maplayer>")
        lines.append("")

    lines.append("  </projectlayers>")
    lines.append("")
    lines.append("  <layerorder>")
    for snap in selected:
        layer_id = f"s1_flood_diff_{snap.year:04d}_{snap.month:02d}"
        lines.append(f'    <layer id="{layer_id}"/>')
    lines.append("  </layerorder>")
    lines.append("</qgis>")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote QGIS project: {output_path}")
    print(f"Layers: {len(selected)}")
    print(f"Opacity per layer: {opacity:.2f}")
    print(f"Range: {selected[0].month_label} -> {selected[-1].month_label}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a QGIS project with static snapshot layers for a month range."
    )
    parser.add_argument("--from", dest="from_month", required=True, help='Start month in MM/YYYY.')
    parser.add_argument("--to", dest="to_month", required=True, help='End month in MM/YYYY.')
    parser.add_argument(
        "--snapshot-dir",
        default="output/flood_2025/snapshots",
        help="Directory containing s1_flood_diff_YYYY-MM-DD.tif files.",
    )
    parser.add_argument(
        "--output",
        default="qgis/livestock_flood_stack_range.qgs",
        help="Output QGIS project path.",
    )
    args = parser.parse_args()

    from_year, from_month = _parse_mm_yyyy(args.from_month)
    to_year, to_month = _parse_mm_yyyy(args.to_month)
    start_key = _month_key(from_year, from_month)
    end_key = _month_key(to_year, to_month)
    if end_key < start_key:
        raise SystemExit("Invalid range: --to must be after or equal to --from.")

    snapshot_dir = Path(args.snapshot_dir)
    all_snaps = _discover_snapshots(snapshot_dir)
    if not all_snaps:
        raise SystemExit(f"No snapshot files found in: {snapshot_dir}")

    selected = [s for s in all_snaps if start_key <= s.month_key <= end_key]
    if not selected:
        first = min(all_snaps, key=lambda s: s.month_key).month_label
        last = max(all_snaps, key=lambda s: s.month_key).month_label
        raise SystemExit(
            f"No snapshots in requested range {from_year:04d}-{from_month:02d} -> "
            f"{to_year:04d}-{to_month:02d}. Available: {first} -> {last}."
        )

    expected = end_key - start_key + 1
    if len(selected) < expected:
        print(
            f"Warning: selected {len(selected)} month layers, expected {expected}. "
            "Some months are missing in snapshot files."
        )

    output = Path(args.output)
    title = (
        f"Livestock Flood ({selected[0].month_label} to {selected[-1].month_label} Snapshot Stack)"
    )
    group = f"FloodDiff Stack {selected[0].month_label} to {selected[-1].month_label}"
    _build_qgs(selected, output, title=title, group_name=group)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Build and optionally launch a minimal QGIS project with one flood snapshot layer.
"""

from __future__ import annotations

import argparse
import calendar
import os
import re
import subprocess
import sys
from pathlib import Path


SNAPSHOT_RE = re.compile(r"s1_flood_diff_(\d{4})-(\d{2})-(\d{2})\.tif$")


def _parse_mm_yyyy(value: str) -> tuple[int, int]:
    raw = value.strip()
    parts = raw.split("/")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        raise ValueError("Expected MM/YYYY format.")
    month = int(parts[0])
    year = int(parts[1])
    if month < 1 or month > 12:
        raise ValueError("Month must be 01-12.")
    return year, month


def _find_snapshot(snapshot_dir: Path, year: int, month: int) -> Path:
    if not snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot directory not found: {snapshot_dir}")

    month_prefix = f"s1_flood_diff_{year:04d}-{month:02d}-"
    candidates: list[Path] = []
    for path in sorted(snapshot_dir.glob("s1_flood_diff_*.tif")):
        if not path.name.startswith(month_prefix):
            continue
        match = SNAPSHOT_RE.match(path.name)
        if match is None:
            continue
        candidates.append(path)

    if not candidates:
        raise FileNotFoundError(
            f"No snapshot found for {year:04d}-{month:02d} in {snapshot_dir}"
        )
    return candidates[-1]


def _write_qgs(snapshot_path: Path, output_qgs: Path, title: str, opacity: float) -> None:
    qgs_dir = output_qgs.parent.resolve()
    ds_rel = os.path.relpath(str(snapshot_path.resolve()), str(qgs_dir)).replace("\\", "/")
    layer_id = "s1_flood_diff_single"
    layer_name = snapshot_path.stem

    content = f"""<qgis version="3.40.9" projectname="livestock_flood_single_snapshot">
  <title>{title}</title>
  <projectCrs>
    <spatialrefsys>
      <authid>OGC:CRS84</authid>
      <description>WGS 84 (CRS84 lon/lat)</description>
      <proj4>+proj=longlat +datum=WGS84 +no_defs</proj4>
      <srsid>3452</srsid>
    </spatialrefsys>
  </projectCrs>
  <mapcanvas annotationsVisible="1" name="theMapCanvas">
    <units>degrees</units>
    <extent>
      <xmin>-64.11153603146758000</xmin>
      <ymin>-13.88018610194003700</ymin>
      <xmax>-63.74340811195626000</xmax>
      <ymax>-13.52041247635613500</ymax>
    </extent>
    <rotation>0</rotation>
  </mapcanvas>

  <layer-tree-group name="Single Snapshot">
    <layer-tree-layer id="{layer_id}" name="{layer_name}" checked="Qt::Checked"/>
  </layer-tree-group>

  <projectlayers>
    <maplayer type="raster" name="{layer_name}" id="{layer_id}">
      <datasource>{ds_rel}</datasource>
      <provider>gdal</provider>
      <layername>{layer_name}</layername>
      <extent>
        <xmin>-64.11153603146758000</xmin>
        <ymin>-13.88018610194003700</ymin>
        <xmax>-63.74340811195626000</xmax>
        <ymax>-13.52041247635613500</ymax>
      </extent>
      <rasterrenderer type="singlebandpseudocolor" opacity="{opacity:.2f}" alphaBand="-1" band="1">
        <rastershader>
          <colorrampshader colorRampType="INTERPOLATED" classificationMode="1" clip="0">
            <item color="#08306b" value="-4" label="-4" alpha="255"/>
            <item color="#08519c" value="-3" label="-3" alpha="255"/>
            <item color="#2171b5" value="-2" label="-2" alpha="255"/>
            <item color="#6baed6" value="-1" label="-1" alpha="255"/>
            <item color="#f7f7f7" value="0" label="0" alpha="255"/>
            <item color="#fddbc7" value="0.5" label="0.5" alpha="255"/>
            <item color="#f4a582" value="1" label="1" alpha="255"/>
            <item color="#d6604d" value="2" label="2" alpha="255"/>
            <item color="#b2182b" value="3" label="3" alpha="255"/>
          </colorrampshader>
        </rastershader>
      </rasterrenderer>
    </maplayer>
  </projectlayers>

  <layerorder>
    <layer id="{layer_id}"/>
  </layerorder>
</qgis>
"""
    output_qgs.parent.mkdir(parents=True, exist_ok=True)
    output_qgs.write_text(content, encoding="utf-8")


def _resolve_qgis_bin(provided: str) -> Path:
    if provided:
        path = Path(provided)
        if path.exists():
            return path
        raise FileNotFoundError(f"QGIS binary not found: {path}")
    ltr = Path(r"C:\OSGeo4W\bin\qgis-ltr-bin.exe")
    if ltr.exists():
        return ltr
    std = Path(r"C:\OSGeo4W\bin\qgis-bin.exe")
    if std.exists():
        return std
    raise FileNotFoundError("Could not find qgis-ltr-bin.exe or qgis-bin.exe in C:\\OSGeo4W\\bin")


def _default_snapshot_dir(repo_root: Path) -> Path:
    preferred = repo_root / "output" / "flood_2025" / "snapshots"
    fallback = repo_root / "output" / "flood" / "snapshots"
    return preferred if preferred.exists() else fallback


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build (and optionally launch) a minimal QGIS project with one snapshot."
    )
    parser.add_argument("--month", required=True, help='Target month in MM/YYYY, e.g. "03/2025".')
    parser.add_argument("--snapshot-dir", default="", help="Snapshot directory (optional).")
    parser.add_argument(
        "--output",
        default="qgis/livestock_flood_single_snapshot.qgs",
        help="Output QGIS project path.",
    )
    parser.add_argument("--opacity", type=float, default=1.0, help="Layer opacity [0..1].")
    parser.add_argument("--launch", action="store_true", help="Launch QGIS after writing project.")
    parser.add_argument("--qgis-bin", default="", help="Path to qgis-ltr-bin.exe or qgis-bin.exe.")
    parser.add_argument("--profile", default="livestock_single_snapshot", help="QGIS profile name.")
    parser.add_argument(
        "--profiles-path",
        default="qgis/profiles",
        help="QGIS profiles path (directory).",
    )
    parser.add_argument("--opengl", default="software", choices=["software", "desktop"], help="QT_OPENGL mode.")
    args = parser.parse_args()

    if args.opacity < 0 or args.opacity > 1:
        raise SystemExit("--opacity must be between 0 and 1.")

    repo_root = Path(__file__).resolve().parent.parent
    snapshot_dir = Path(args.snapshot_dir) if args.snapshot_dir else _default_snapshot_dir(repo_root)
    output_qgs = Path(args.output)
    if not output_qgs.is_absolute():
        output_qgs = repo_root / output_qgs
    profiles_path = Path(args.profiles_path)
    if not profiles_path.is_absolute():
        profiles_path = repo_root / profiles_path

    try:
        year, month = _parse_mm_yyyy(args.month)
    except ValueError as exc:
        raise SystemExit(str(exc))

    try:
        snapshot = _find_snapshot(snapshot_dir, year, month)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc))

    month_name = calendar.month_name[month]
    title = f"Livestock Flood Single Snapshot ({month_name} {year})"
    _write_qgs(snapshot, output_qgs, title=title, opacity=args.opacity)

    print(f"Wrote: {output_qgs}")
    print(f"Layer: {snapshot}")
    print(f"Style: flood_diff blue ramp, opacity={args.opacity:.2f}")

    if not args.launch:
        return 0

    qgis_bin = _resolve_qgis_bin(args.qgis_bin)
    profiles_path.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["QT_OPENGL"] = args.opengl
    env["QGIS_DISABLE_MESSAGE_HOOKS"] = "1"
    env["QGIS_DISABLE_VERSION_CHECK"] = "1"
    env["QGIS_LOG_FILE"] = str(repo_root / "qgis" / "qgis_single_snapshot.log")

    cmd = [
        str(qgis_bin),
        "--nologo",
        "--noplugins",
        "--nocustomization",
        "--profile",
        args.profile,
        "--profiles-path",
        str(profiles_path),
        str(output_qgs),
    ]
    print("Launching QGIS...")
    print(" ".join(cmd))
    return subprocess.run(cmd, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())

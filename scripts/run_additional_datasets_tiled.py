#!/usr/bin/env python3
"""
Tile-based orchestrator for additional EE dataset downloads.

This script runs `scripts/download_additional_datasets.py` over a grid of tile
centers, storing outputs per tile and tracking status in a manifest CSV.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER = ROOT / "scripts" / "download_additional_datasets.py"

DEFAULT_LAT = -13.700278
DEFAULT_LON = -63.927778

MANIFEST_FIELDS = [
    "tile_id",
    "row",
    "col",
    "lat",
    "lon",
    "distance_km",
    "tile_out_dir",
    "datasets",
    "start",
    "end",
    "status",
    "attempts",
    "last_exit_code",
    "last_error",
    "dw_tif",
    "s3_tif",
    "s2_tif",
    "total_tif",
    "updated_utc",
]


@dataclass(frozen=True)
class TileSpec:
    tile_id: str
    row: int
    col: int
    lat: float
    lon: float
    distance_km: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run tiled downloads for additional datasets.")
    p.add_argument("--project-id", default="gen-lang-client-0296388721")
    p.add_argument("--lat", type=float, default=DEFAULT_LAT, help="AOI center latitude.")
    p.add_argument("--lon", type=float, default=DEFAULT_LON, help="AOI center longitude.")
    p.add_argument("--radius-km", type=float, required=True, help="Expansion radius from center.")
    p.add_argument("--shape", choices=["circle", "square"], default="circle")
    p.add_argument("--tile-buffer-km", type=float, default=15.0, help="Per-tile buffer passed to downloader.")
    p.add_argument(
        "--tile-step-km",
        type=float,
        default=0.0,
        help="Distance between tile centers. Default: tile-buffer-km * 1.8",
    )
    p.add_argument("--start", required=True, help="Inclusive start date (YYYY-MM-DD).")
    p.add_argument("--end", required=True, help="Exclusive end date (YYYY-MM-DD).")
    p.add_argument(
        "--datasets",
        default="dynamicworld,s3olci,sentinel2",
        help="Comma list: dynamicworld,s3olci,sentinel2",
    )
    p.add_argument(
        "--s3-bands",
        default="Oa06_radiance,Oa08_radiance,Oa17_radiance,Oa21_radiance",
        help="Pass-through bands for S3.",
    )
    p.add_argument(
        "--s2-bands",
        default="B2,B3,B4,B8,B11",
        help="Pass-through bands for S2.",
    )
    p.add_argument("--s2-cloudy-max", type=float, default=80.0)
    p.add_argument("--out-dir", default="output/flood/additional_tiled_2025")
    p.add_argument("--max-tiles", type=int, default=0, help="Optional cap for testing.")
    p.add_argument("--retries", type=int, default=2, help="Retry count after first attempt.")
    p.add_argument("--retry-delay-sec", type=int, default=20)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true", help="Ignore manifest status and rerun tiles.")
    p.add_argument("--skip-existing", dest="skip_existing", action="store_true")
    p.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    p.set_defaults(skip_existing=True)
    p.add_argument("--resume", dest="resume", action="store_true")
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.set_defaults(resume=True)
    return p.parse_args()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _km_to_deg_lat(km: float) -> float:
    return km / 110.574


def _km_to_deg_lon(km: float, lat: float) -> float:
    cos_lat = max(math.cos(math.radians(lat)), 1e-6)
    return km / (111.320 * cos_lat)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        res = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        return str(pid) in (res.stdout or "")
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists():
        raw = lock_path.read_text(encoding="utf-8").strip()
        prev_pid = 0
        if raw:
            try:
                prev_pid = int(raw.split(",", 1)[0].strip())
            except ValueError:
                prev_pid = 0
        if prev_pid > 0 and _pid_alive(prev_pid):
            raise RuntimeError(
                f"Another tiled run is already active (pid={prev_pid}). "
                f"Lock file: {lock_path}"
            )
        # stale lock
        lock_path.unlink(missing_ok=True)

    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    pid = os.getpid()
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(f"{pid},{_now_utc()}\n")
    return pid


def release_lock(lock_path: Path, owner_pid: int) -> None:
    if owner_pid <= 0 or not lock_path.exists():
        return
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
        pid = int(raw.split(",", 1)[0].strip()) if raw else 0
    except Exception:
        pid = 0
    if pid == owner_pid:
        lock_path.unlink(missing_ok=True)


def _tag(value: int, prefix: str) -> str:
    sign = "p" if value >= 0 else "m"
    return f"{prefix}{sign}{abs(value):03d}"


def normalize_datasets(raw: str) -> list[str]:
    alias = {
        "s2": "sentinel2",
        "s2sr": "sentinel2",
        "sentinel-2": "sentinel2",
        "dynamic-world": "dynamicworld",
        "s3": "s3olci",
    }
    valid = {"dynamicworld", "s3olci", "sentinel2"}
    requested = []
    for part in raw.split(","):
        name = part.strip().lower()
        if not name:
            continue
        n = alias.get(name, name)
        if n not in valid:
            raise ValueError(f"Unknown dataset '{name}'. Valid: {sorted(valid)}")
        if n not in requested:
            requested.append(n)
    if not requested:
        raise ValueError("No datasets requested.")
    return requested


def build_tiles(center_lat: float, center_lon: float, radius_km: float, step_km: float, shape: str) -> list[TileSpec]:
    n = int(math.ceil(radius_km / step_km))
    tiles: list[TileSpec] = []
    for row in range(n, -n - 1, -1):
        dy_km = row * step_km
        lat = center_lat + _km_to_deg_lat(dy_km)
        for col in range(-n, n + 1):
            dx_km = col * step_km
            distance_km = math.hypot(dx_km, dy_km)
            if shape == "circle" and distance_km > radius_km + 1e-9:
                continue
            lon = center_lon + _km_to_deg_lon(dx_km, lat)
            tile_id = f"{_tag(row, 'r')}_{_tag(col, 'c')}"
            tiles.append(
                TileSpec(
                    tile_id=tile_id,
                    row=row,
                    col=col,
                    lat=lat,
                    lon=lon,
                    distance_km=distance_km,
                )
            )
    return tiles


def read_manifest(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    out: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tid = row.get("tile_id", "").strip()
            if tid:
                out[tid] = row
    return out


def write_manifest(path: Path, rows: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for tile_id in sorted(rows):
            row = rows[tile_id]
            writer.writerow({k: row.get(k, "") for k in MANIFEST_FIELDS})


def count_tile_outputs(tile_dir: Path) -> tuple[int, int, int, int]:
    dw = len(list((tile_dir / "dynamicworld").glob("*.tif"))) if (tile_dir / "dynamicworld").exists() else 0
    s3 = len(list((tile_dir / "s3_olci").glob("*.tif"))) if (tile_dir / "s3_olci").exists() else 0
    s2 = (
        len(list((tile_dir / "sentinel2_sr_harmonized").glob("*.tif")))
        if (tile_dir / "sentinel2_sr_harmonized").exists()
        else 0
    )
    return dw, s3, s2, dw + s3 + s2


def build_downloader_cmd(args: argparse.Namespace, tile: TileSpec, tile_dir: Path, datasets_csv: str) -> list[str]:
    cmd = [
        sys.executable,
        str(DOWNLOADER),
        "--project-id",
        args.project_id,
        "--lat",
        f"{tile.lat:.8f}",
        "--lon",
        f"{tile.lon:.8f}",
        "--buffer-km",
        f"{args.tile_buffer_km}",
        "--start",
        args.start,
        "--end",
        args.end,
        "--datasets",
        datasets_csv,
        "--s3-bands",
        args.s3_bands,
        "--s2-bands",
        args.s2_bands,
        "--s2-cloudy-max",
        f"{args.s2_cloudy_max}",
        "--out-dir",
        str(tile_dir),
    ]
    if args.skip_existing:
        cmd.append("--skip-existing")
    return cmd


def run_tile(cmd: list[str], retries: int, retry_delay_sec: int) -> tuple[int, int]:
    attempts = 0
    total_attempts = retries + 1
    rc = 1
    for attempt in range(1, total_attempts + 1):
        attempts = attempt
        print(f"    attempt {attempt}/{total_attempts}")
        rc = subprocess.run(cmd, cwd=str(ROOT)).returncode
        if rc == 0:
            return rc, attempts
        if attempt < total_attempts:
            delay = retry_delay_sec * attempt
            print(f"    downloader failed with code {rc}; retrying in {delay}s...")
            time.sleep(delay)
    return rc, attempts


def main() -> int:
    args = parse_args()
    if not DOWNLOADER.exists():
        raise FileNotFoundError(f"Downloader not found: {DOWNLOADER}")
    if args.radius_km <= 0:
        raise ValueError("--radius-km must be > 0")
    if args.tile_buffer_km <= 0:
        raise ValueError("--tile-buffer-km must be > 0")

    step_km = args.tile_step_km if args.tile_step_km > 0 else args.tile_buffer_km * 1.8
    if step_km <= 0:
        raise ValueError("--tile-step-km must be > 0")

    datasets = normalize_datasets(args.datasets)
    datasets_csv = ",".join(datasets)

    out_dir = Path(args.out_dir)
    tiles_root = out_dir / "tiles"
    manifest_path = out_dir / "tile_manifest.csv"
    lock_path = out_dir / ".run_additional_datasets_tiled.lock"
    out_dir.mkdir(parents=True, exist_ok=True)
    tiles_root.mkdir(parents=True, exist_ok=True)
    lock_owner_pid = acquire_lock(lock_path)

    try:
        tiles = build_tiles(args.lat, args.lon, args.radius_km, step_km, args.shape)
        if args.max_tiles and args.max_tiles > 0:
            tiles = tiles[: args.max_tiles]

        existing = read_manifest(manifest_path)
        print("Tile run configuration")
        print(f" center:        ({args.lat:.6f}, {args.lon:.6f})")
        print(f" shape:         {args.shape}")
        print(f" radius_km:     {args.radius_km}")
        print(f" tile_buffer:   {args.tile_buffer_km}")
        print(f" tile_step_km:  {step_km}")
        print(f" datasets:      {datasets_csv}")
        print(f" range:         {args.start} -> {args.end} (end exclusive)")
        print(f" out_dir:       {out_dir}")
        print(f" tiles:         {len(tiles)}")
        print(f" manifest:      {manifest_path}")
        print(f" lock:          {lock_path}")

        rows: dict[str, dict[str, str]] = dict(existing)
        ok = 0
        skipped = 0
        failed = 0

        for i, tile in enumerate(tiles, start=1):
            tile_dir = tiles_root / tile.tile_id
            tile_dir.mkdir(parents=True, exist_ok=True)

            prev = rows.get(tile.tile_id, {})
            prev_status = prev.get("status", "")
            if args.resume and (not args.force) and prev_status == "ok":
                skipped += 1
                print(f"[{i}/{len(tiles)}] {tile.tile_id}: skip (manifest status=ok)")
                continue

            print(
                f"[{i}/{len(tiles)}] {tile.tile_id}: "
                f"lat={tile.lat:.6f}, lon={tile.lon:.6f}, dist={tile.distance_km:.1f} km"
            )
            cmd = build_downloader_cmd(args, tile, tile_dir, datasets_csv)
            if args.dry_run:
                print("    dry-run command:", " ".join(cmd))
                rc = 0
                attempts = 0
                status = "dry_run"
                err = ""
            else:
                rc, attempts = run_tile(cmd, retries=args.retries, retry_delay_sec=args.retry_delay_sec)
                status = "ok" if rc == 0 else "error"
                err = "" if rc == 0 else f"exit_code={rc}"

            dw, s3, s2, total = count_tile_outputs(tile_dir)
            rel_tile_dir = tile_dir.relative_to(ROOT) if tile_dir.is_relative_to(ROOT) else tile_dir
            rows[tile.tile_id] = {
                "tile_id": tile.tile_id,
                "row": str(tile.row),
                "col": str(tile.col),
                "lat": f"{tile.lat:.8f}",
                "lon": f"{tile.lon:.8f}",
                "distance_km": f"{tile.distance_km:.3f}",
                "tile_out_dir": str(rel_tile_dir),
                "datasets": datasets_csv,
                "start": args.start,
                "end": args.end,
                "status": status,
                "attempts": str(attempts),
                "last_exit_code": str(rc),
                "last_error": err,
                "dw_tif": str(dw),
                "s3_tif": str(s3),
                "s2_tif": str(s2),
                "total_tif": str(total),
                "updated_utc": _now_utc(),
            }
            write_manifest(manifest_path, rows)

            if status == "ok" or status == "dry_run":
                ok += 1
            else:
                failed += 1

        print("Tile run summary")
        print(f" ok:      {ok}")
        print(f" skipped: {skipped}")
        print(f" failed:  {failed}")
        print(f" manifest: {manifest_path}")
        return 0 if failed == 0 else 1
    finally:
        release_lock(lock_path, lock_owner_pid)


if __name__ == "__main__":
    raise SystemExit(main())

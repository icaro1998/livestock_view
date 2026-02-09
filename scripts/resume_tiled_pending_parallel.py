#!/usr/bin/env python3
"""
Resume pending tiled downloads in parallel without losing completed tiles.

Use case:
- Keep already-complete tiles (status=ok in tile_manifest.csv)
- Process only pending/missing tiles with N workers
- Update tile_manifest.csv as each tile finishes
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    p = argparse.ArgumentParser(description="Resume pending tiled downloads in parallel.")
    p.add_argument("--project-id", default="gen-lang-client-0296388721")
    p.add_argument("--lat", type=float, default=DEFAULT_LAT)
    p.add_argument("--lon", type=float, default=DEFAULT_LON)
    p.add_argument("--radius-km", type=float, required=True)
    p.add_argument("--shape", choices=["circle", "square"], default="circle")
    p.add_argument("--tile-buffer-km", type=float, default=15.0)
    p.add_argument("--tile-step-km", type=float, default=24.0)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument(
        "--datasets",
        default="sentinel2",
        help="Comma list: dynamicworld,s3olci,sentinel2",
    )
    p.add_argument("--s3-bands", default="Oa06_radiance,Oa08_radiance,Oa17_radiance,Oa21_radiance")
    p.add_argument("--s2-bands", default="B2,B3,B4,B8,B11")
    p.add_argument("--s2-cloudy-max", type=float, default=80.0)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--workers", type=int, default=2, help="Parallel tiles (recommended: 2 or 3).")
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--retry-delay-sec", type=int, default=30)
    p.add_argument("--max-pending", type=int, default=0, help="Optional cap for pending tiles.")
    p.add_argument("--only-tiles", default="", help="Optional comma list: tile_id,tile_id,...")
    p.add_argument("--skip-existing", action="store_true", default=True)
    p.add_argument("--force-rerun-ok", action="store_true", help="Also rerun tiles already marked as ok.")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _km_to_deg_lat(km: float) -> float:
    return km / 110.574


def _km_to_deg_lon(km: float, lat: float) -> float:
    cos_lat = max(math.cos(math.radians(lat)), 1e-6)
    return km / (111.320 * cos_lat)


def _tag(value: int, prefix: str) -> str:
    sign = "p" if value >= 0 else "m"
    return f"{prefix}{sign}{abs(value):03d}"


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
            raise RuntimeError(f"Another parallel-resume run is active (pid={prev_pid}). Lock: {lock_path}")
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


def normalize_datasets(raw: str) -> list[str]:
    alias = {
        "s2": "sentinel2",
        "s2sr": "sentinel2",
        "sentinel-2": "sentinel2",
        "dynamic-world": "dynamicworld",
        "s3": "s3olci",
    }
    valid = {"dynamicworld", "s3olci", "sentinel2"}
    requested: list[str] = []
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
    with path.open("r", encoding="utf-8-sig", newline="") as f:
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
    s2 = len(list((tile_dir / "sentinel2_sr_harmonized").glob("*.tif"))) if (tile_dir / "sentinel2_sr_harmonized").exists() else 0
    return dw, s3, s2, dw + s3 + s2


def build_cmd(args: argparse.Namespace, tile: TileSpec, tile_dir: Path, datasets_csv: str) -> list[str]:
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


def run_with_retries(cmd: list[str], retries: int, retry_delay_sec: int, cwd: Path) -> tuple[int, int, str]:
    attempts = 0
    total = retries + 1
    rc = 1
    err = ""
    for attempt in range(1, total + 1):
        attempts = attempt
        p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
        rc = p.returncode
        if rc == 0:
            return 0, attempts, ""
        err = (p.stderr or p.stdout or "").strip().splitlines()
        err = err[-1] if err else f"exit_code={rc}"
        if attempt < total:
            time.sleep(retry_delay_sec * attempt)
    return rc, attempts, err


def process_tile(args: argparse.Namespace, tile: TileSpec, datasets_csv: str, out_dir: Path) -> dict[str, str]:
    tile_dir = out_dir / "tiles" / tile.tile_id
    tile_dir.mkdir(parents=True, exist_ok=True)
    cmd = build_cmd(args, tile, tile_dir, datasets_csv)

    if args.dry_run:
        rc, attempts, err, status = 0, 0, "", "dry_run"
    else:
        rc, attempts, err = run_with_retries(cmd, retries=args.retries, retry_delay_sec=args.retry_delay_sec, cwd=ROOT)
        status = "ok" if rc == 0 else "error"

    dw, s3, s2, total = count_tile_outputs(tile_dir)
    rel_tile_dir = tile_dir.relative_to(ROOT) if tile_dir.is_relative_to(ROOT) else tile_dir

    return {
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


def main() -> int:
    args = parse_args()
    if not DOWNLOADER.exists():
        raise FileNotFoundError(f"Downloader not found: {DOWNLOADER}")
    if args.radius_km <= 0:
        raise ValueError("--radius-km must be > 0")
    if args.tile_buffer_km <= 0:
        raise ValueError("--tile-buffer-km must be > 0")
    if args.workers <= 0:
        raise ValueError("--workers must be > 0")

    step_km = args.tile_step_km if args.tile_step_km > 0 else args.tile_buffer_km * 1.8
    if step_km <= 0:
        raise ValueError("--tile-step-km must be > 0")

    datasets = normalize_datasets(args.datasets)
    datasets_csv = ",".join(datasets)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "tiles").mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "tile_manifest.csv"
    lock_path = out_dir / ".resume_pending_parallel.lock"
    lock_owner_pid = acquire_lock(lock_path)

    try:
        tiles = build_tiles(args.lat, args.lon, args.radius_km, step_km, args.shape)
        rows = read_manifest(manifest_path)

        only_ids = {x.strip() for x in args.only_tiles.split(",") if x.strip()} if args.only_tiles else set()

        pending: list[TileSpec] = []
        for t in tiles:
            if only_ids and t.tile_id not in only_ids:
                continue
            prev = rows.get(t.tile_id)
            prev_status = (prev or {}).get("status", "")
            if (not args.force_rerun_ok) and prev_status == "ok":
                continue
            pending.append(t)

        if args.max_pending > 0:
            pending = pending[: args.max_pending]

        ok_existing = sum(1 for v in rows.values() if v.get("status") == "ok")
        print("Parallel resume configuration")
        print(f" out_dir:          {out_dir}")
        print(f" datasets:         {datasets_csv}")
        print(f" expected_tiles:   {len(tiles)}")
        print(f" existing_ok:      {ok_existing}")
        print(f" pending_now:      {len(pending)}")
        print(f" workers:          {args.workers}")
        print(f" manifest:         {manifest_path}")

        if not pending:
            print("Nothing to do. All selected tiles already ok.")
            return 0

        if args.dry_run:
            for t in pending:
                print(f"[dry-run] {t.tile_id} lat={t.lat:.6f} lon={t.lon:.6f}")
            return 0

        completed = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            fut_map = {ex.submit(process_tile, args, tile, datasets_csv, out_dir): tile for tile in pending}
            for fut in as_completed(fut_map):
                tile = fut_map[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    row = {
                        "tile_id": tile.tile_id,
                        "row": str(tile.row),
                        "col": str(tile.col),
                        "lat": f"{tile.lat:.8f}",
                        "lon": f"{tile.lon:.8f}",
                        "distance_km": f"{tile.distance_km:.3f}",
                        "tile_out_dir": str((out_dir / "tiles" / tile.tile_id).relative_to(ROOT)),
                        "datasets": datasets_csv,
                        "start": args.start,
                        "end": args.end,
                        "status": "error",
                        "attempts": "0",
                        "last_exit_code": "999",
                        "last_error": f"exception={e}",
                        "dw_tif": "0",
                        "s3_tif": "0",
                        "s2_tif": "0",
                        "total_tif": "0",
                        "updated_utc": _now_utc(),
                    }

                rows[row["tile_id"]] = row
                write_manifest(manifest_path, rows)

                if row["status"] == "ok":
                    completed += 1
                else:
                    failed += 1

                ok_now = sum(1 for v in rows.values() if v.get("status") == "ok")
                print(
                    f"[{completed + failed}/{len(pending)}] {row['tile_id']} status={row['status']} "
                    f"s2_tif={row['s2_tif']} ok_total={ok_now}/{len(tiles)}"
                )

        print("Parallel resume summary")
        print(f" completed_now: {completed}")
        print(f" failed_now:    {failed}")
        print(f" manifest:      {manifest_path}")
        return 0 if failed == 0 else 1
    finally:
        release_lock(lock_path, lock_owner_pid)


if __name__ == "__main__":
    raise SystemExit(main())


[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$RunRoot,
  [string]$SnapshotBase = ".\output\flood\stable_snapshots",
  [string]$SnapshotName = "",
  [switch]$Force,
  [switch]$Validate
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Get-PythonPath {
  if (Test-Path ".\.venv\Scripts\python.exe") {
    return ".\.venv\Scripts\python.exe"
  }
  return "python"
}

function Read-StableManifest {
  param(
    [string]$Path,
    [int]$MaxAttempts = 6,
    [int]$DelayMs = 400
  )

  for ($i = 1; $i -le $MaxAttempts; $i++) {
    try {
      $r1 = @(Import-Csv $Path -ErrorAction Stop)
      Start-Sleep -Milliseconds $DelayMs
      $r2 = @(Import-Csv $Path -ErrorAction Stop)
      if ($r1.Count -eq $r2.Count) {
        return $r2
      }
    } catch {
      # Manifest may be mid-write; retry.
    }
    Start-Sleep -Milliseconds $DelayMs
  }

  throw "Could not obtain a stable read of manifest: $Path"
}

function Ensure-Dir {
  param([string]$Path)
  if (-not (Test-Path $Path)) {
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
  }
}

$runPath = (Resolve-Path $RunRoot -ErrorAction Stop).Path
$manifestPath = Join-Path $runPath "tile_manifest.csv"
if (-not (Test-Path $manifestPath)) {
  throw "Manifest not found: $manifestPath"
}

$rows = Read-StableManifest -Path $manifestPath
$okRows = @($rows | Where-Object { $_.status -eq "ok" } | Sort-Object tile_id)
$errRows = @($rows | Where-Object { $_.status -eq "error" })

if ($okRows.Count -eq 0) {
  throw "No status=ok tiles found in manifest. Nothing to snapshot."
}

$runTag = Split-Path $runPath -Leaf
if (-not $SnapshotName) {
  $ts = Get-Date -Format "yyyyMMdd_HHmmss"
  $SnapshotName = "{0}_stable_{1}" -f $runTag, $ts
}

$snapshotPath = Join-Path $SnapshotBase $SnapshotName
if (Test-Path $snapshotPath) {
  if (-not $Force) {
    throw "Snapshot path exists: $snapshotPath (use -Force to overwrite)"
  }
  Remove-Item -LiteralPath $snapshotPath -Recurse -Force -ErrorAction Stop
}

Ensure-Dir -Path $snapshotPath
$tilesSrc = Join-Path $runPath "tiles"
$tilesDst = Join-Path $snapshotPath "tiles"
Ensure-Dir -Path $tilesDst

$copiedTiles = 0
$missingTiles = 0
$copiedFiles = 0
$copiedBytes = [int64]0

foreach ($row in $okRows) {
  $tileId = [string]$row.tile_id
  $srcTile = Join-Path $tilesSrc $tileId
  if (-not (Test-Path $srcTile)) {
    Write-Warning "Tile marked ok but source tile folder missing: $tileId"
    $missingTiles++
    continue
  }

  $dstTile = Join-Path $tilesDst $tileId
  Ensure-Dir -Path $dstTile

  # Robocopy is robust for Windows large tree copies.
  & robocopy $srcTile $dstTile /E /NFL /NDL /NJH /NJS /NP /R:1 /W:1 | Out-Null
  if ($LASTEXITCODE -gt 7) {
    throw "Robocopy failed for tile=$tileId with exit code $LASTEXITCODE"
  }

  $files = @(Get-ChildItem $dstTile -Recurse -File -ErrorAction SilentlyContinue)
  $copiedTiles++
  $copiedFiles += $files.Count
  $sum = ($files | Measure-Object Length -Sum).Sum
  if ($sum) { $copiedBytes += [int64]$sum }
}

$okRows | Export-Csv -Path (Join-Path $snapshotPath "tile_manifest.csv") -NoTypeInformation -Encoding UTF8
$rows | Export-Csv -Path (Join-Path $snapshotPath "tile_manifest_full_at_freeze.csv") -NoTypeInformation -Encoding UTF8

$meta = [ordered]@{
  created_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
  source_run = $runPath
  source_manifest = $manifestPath
  source_rows = $rows.Count
  source_ok = $okRows.Count
  source_error = $errRows.Count
  snapshot_path = (Resolve-Path $snapshotPath).Path
  snapshot_tiles_copied = $copiedTiles
  snapshot_tiles_missing = $missingTiles
  snapshot_files = $copiedFiles
  snapshot_size_mb = [math]::Round($copiedBytes / 1MB, 2)
  snapshot_size_gb = [math]::Round($copiedBytes / 1GB, 3)
}
$meta | ConvertTo-Json -Depth 5 | Set-Content -Path (Join-Path $snapshotPath "snapshot_info.json") -Encoding UTF8

Write-Host "Stable snapshot created."
Write-Host " Source run:      $runPath"
Write-Host " Snapshot path:   $snapshotPath"
Write-Host " Tiles copied:    $copiedTiles (ok in source=$($okRows.Count))"
Write-Host " Files copied:    $copiedFiles"
Write-Host " Snapshot size:   $([math]::Round($copiedBytes / 1GB, 3)) GB"
Write-Host " Source err rows: $($errRows.Count)"

if ($Validate) {
  $python = Get-PythonPath
  Write-Host "==> Validating snapshot TIFFs..."
  & $python ".\scripts\validate_rasters.py" `
    --root $snapshotPath `
    --pattern "tiles/*/dynamicworld/*.tif" `
    --pattern "tiles/*/s3_olci/*.tif" `
    --pattern "tiles/*/sentinel2_sr_harmonized/*.tif"
  if ($LASTEXITCODE -ne 0) {
    throw "validate_rasters failed for snapshot with exit code $LASTEXITCODE"
  }
}

[CmdletBinding()]
param(
  [double]$RadiusKm = 60,
  [double]$TileBufferKm = 15,
  [double]$TileStepKm = 24,
  [ValidateSet("circle", "square")]
  [string]$Shape = "circle",
  [int]$TargetTiles = 21,
  [int]$PollSeconds = 20,
  [int]$StallMinutes = 15,
  [int]$MaxInternetFailStreak = 4,
  [int]$InnerRetries = 2,
  [int]$InnerRetryDelaySec = 30,
  [int]$MaxJobRestarts = 20,
  [int]$RestartDelaySec = 20,
  [string]$PingTarget = "1.1.1.1",
  [string]$OutputBase = ".\\output\\flood",
  [string]$LogFile = ".\\output\\_index\\autopilot_remaining_downloads.log",
  [switch]$ForceTakeover,
  [switch]$NoBeep
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$logDir = Split-Path -Parent $LogFile
if ($logDir -and -not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

function Write-Log {
  param([string]$Message)
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  Write-Host $line
  Add-Content -Path $LogFile -Value $line
}

function Invoke-Beeps {
  param(
    [int]$Count,
    [int]$Frequency = 1400,
    [int]$DurationMs = 180
  )
  if ($NoBeep) { return }
  for ($i = 1; $i -le $Count; $i++) {
    try {
      [Console]::Beep($Frequency, $DurationMs)
    } catch {
      [System.Media.SystemSounds]::Exclamation.Play()
    }
    Start-Sleep -Milliseconds 120
  }
}

function Resolve-PythonPath {
  $venvPy = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
  if (Test-Path $venvPy) {
    return (Resolve-Path $venvPy).Path
  }
  return "python"
}

function Get-ManifestStats {
  param(
    [string]$OutDir,
    [int]$ExpectedTiles
  )

  $manifestPath = Join-Path $OutDir "tile_manifest.csv"
  if (-not (Test-Path $manifestPath)) {
    return [pscustomobject]@{
      Exists = $false
      ManifestPath = $manifestPath
      Total = 0
      Ok = 0
      Err = 0
      Dry = 0
      Pending = $ExpectedTiles
      ManifestLastWriteUtc = $null
    }
  }

  $rows = Import-Csv $manifestPath
  $total = $rows.Count
  $ok = ($rows | Where-Object { $_.status -eq "ok" }).Count
  $err = ($rows | Where-Object { $_.status -eq "error" }).Count
  $dry = ($rows | Where-Object { $_.status -eq "dry_run" }).Count

  return [pscustomobject]@{
    Exists = $true
    ManifestPath = $manifestPath
    Total = $total
    Ok = $ok
    Err = $err
    Dry = $dry
    Pending = [math]::Max($ExpectedTiles - $ok, 0)
    ManifestLastWriteUtc = (Get-Item $manifestPath).LastWriteTimeUtc
  }
}

function Get-LatestRunActivityUtc {
  param([string]$OutDir)

  $times = @()
  $manifest = Join-Path $OutDir "tile_manifest.csv"
  if (Test-Path $manifest) {
    $times += (Get-Item $manifest).LastWriteTimeUtc
  }

  $tilesDir = Join-Path $OutDir "tiles"
  if (Test-Path $tilesDir) {
    $latestFile = Get-ChildItem $tilesDir -Recurse -File -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTimeUtc -Descending |
      Select-Object -First 1
    if ($latestFile) {
      $times += $latestFile.LastWriteTimeUtc
    }
  }

  if ($times.Count -eq 0) {
    return $null
  }

  return ($times | Sort-Object -Descending | Select-Object -First 1)
}

function Test-NetOnline {
  param([string]$TargetHost)

  try {
    return [bool](Test-Connection -TargetName $TargetHost -Count 1 -Quiet -ErrorAction Stop)
  } catch {
    try {
      return [bool](Test-Connection -ComputerName $TargetHost -Count 1 -Quiet -ErrorAction Stop)
    } catch {
      $pingOut = & ping -n 1 -w 1200 $TargetHost 2>$null
      return [bool]($pingOut -match "TTL=")
    }
  }
}

function Wait-Internet {
  param(
    [string]$Target,
    [int]$SleepSeconds = 15
  )

  $streak = 0
  while ($true) {
    $ok = Test-NetOnline -TargetHost $Target
    if ($ok) {
      if ($streak -gt 0) {
        Write-Log "Internet restored (target=$Target, failStreak=$streak)."
      }
      return
    }

    $streak += 1
    if (($streak % 4) -eq 0) {
      Write-Log "Waiting internet (target=$Target, failStreak=$streak)..."
    }
    Start-Sleep -Seconds $SleepSeconds
  }
}

function Get-WorkflowProcesses {
  return Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match "run_additional_datasets_tiled.py|download_additional_datasets.py" }
}

$jobs = @(
  @{ Dataset = "dynamicworld"; Start = "2025-01-01"; End = "2025-07-01"; Tag = "dw_2025H1" },
  @{ Dataset = "dynamicworld"; Start = "2025-07-01"; End = "2026-01-01"; Tag = "dw_2025H2" },
  @{ Dataset = "s3olci"; Start = "2025-01-01"; End = "2025-07-01"; Tag = "s3_2025H1" },
  @{ Dataset = "s3olci"; Start = "2025-07-01"; End = "2026-01-01"; Tag = "s3_2025H2" },
  @{ Dataset = "sentinel2"; Start = "2025-01-01"; End = "2025-07-01"; Tag = "s2_2025H1" },
  @{ Dataset = "sentinel2"; Start = "2025-07-01"; End = "2026-01-01"; Tag = "s2_2025H2" }
)

$python = Resolve-PythonPath
Write-Log "Autopilot start. python=$python"

$activeBefore = Get-WorkflowProcesses
if ($activeBefore) {
  if ($ForceTakeover) {
    Write-Log "ForceTakeover enabled. Stopping existing workflow processes..."
    $activeBefore | ForEach-Object {
      try { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } catch {}
    }
    Start-Sleep -Seconds 2
  } else {
    throw "Existing tiled/download workflow process detected. Re-run with -ForceTakeover to stop and continue."
  }
}

try {
  foreach ($job in $jobs) {
    $jobTag = [string]$job.Tag
    $dataset = [string]$job.Dataset
    $start = [string]$job.Start
    $end = [string]$job.End
    $outDir = Join-Path $OutputBase ("additional_tiled_60km_{0}" -f $jobTag)

    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
    $jobLogDir = Join-Path $outDir "logs"
    New-Item -ItemType Directory -Path $jobLogDir -Force | Out-Null

    $stats = Get-ManifestStats -OutDir $outDir -ExpectedTiles $TargetTiles
    if ($stats.Ok -ge $TargetTiles) {
      Write-Log "SKIP job=$jobTag (already complete: ok=$($stats.Ok)/$TargetTiles)."
      continue
    }

    Write-Log "START job=$jobTag dataset=$dataset range=$start->$end out=$outDir"

    $restartCount = 0
    $seenOk = $stats.Ok

    while ($true) {
      if ($seenOk -ge $TargetTiles) {
        break
      }

      if ($restartCount -ge $MaxJobRestarts) {
        throw "Job $jobTag exceeded MaxJobRestarts=$MaxJobRestarts"
      }

      Wait-Internet -Target $PingTarget -SleepSeconds 15

      $attemptId = Get-Date -Format "yyyyMMdd_HHmmss"
      $attemptOutLog = Join-Path $jobLogDir ("attempt_{0}.out.log" -f $attemptId)
      $attemptErrLog = Join-Path $jobLogDir ("attempt_{0}.err.log" -f $attemptId)

      $args = @(
        ".\\scripts\\run_additional_datasets_tiled.py",
        "--radius-km", "$RadiusKm",
        "--tile-buffer-km", "$TileBufferKm",
        "--tile-step-km", "$TileStepKm",
        "--shape", "$Shape",
        "--start", "$start",
        "--end", "$end",
        "--datasets", "$dataset",
        "--out-dir", "$outDir",
        "--retries", "$InnerRetries",
        "--retry-delay-sec", "$InnerRetryDelaySec",
        "--resume",
        "--skip-existing"
      )

      Write-Log "RUN job=$jobTag attempt=$($restartCount + 1) cmd=$python $($args -join ' ')"
      Write-Log "Attempt logs: out=$attemptOutLog err=$attemptErrLog"

      $proc = Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $repoRoot -PassThru -NoNewWindow -RedirectStandardOutput $attemptOutLog -RedirectStandardError $attemptErrLog

      $internetFailStreak = 0
      $stoppedForNetwork = $false
      $stoppedForStall = $false
      $lastActivityUtc = Get-LatestRunActivityUtc -OutDir $outDir

      while (-not $proc.HasExited) {
        $netOk = Test-NetOnline -TargetHost $PingTarget
        if (-not $netOk) {
          $internetFailStreak += 1
          Write-Log "WARN job=$jobTag internet check failed ($internetFailStreak/$MaxInternetFailStreak)."
        } else {
          $internetFailStreak = 0
        }

        if ($internetFailStreak -ge $MaxInternetFailStreak) {
          Write-Log "STOP job=$jobTag reason=network_down pid=$($proc.Id)"
          try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}
          $stoppedForNetwork = $true
          break
        }

        $stats = Get-ManifestStats -OutDir $outDir -ExpectedTiles $TargetTiles
        if ($stats.Ok -gt $seenOk) {
          $delta = $stats.Ok - $seenOk
          $seenOk = $stats.Ok
          for ($i = 0; $i -lt $delta; $i++) {
            Invoke-Beeps -Count 10 -Frequency 1350 -DurationMs 150
          }
          Write-Log "PROGRESS job=$jobTag +$delta tiles | ok=$seenOk/$TargetTiles err=$($stats.Err) dry=$($stats.Dry)"
        }

        $activityUtc = Get-LatestRunActivityUtc -OutDir $outDir
        if ($activityUtc) {
          $lastActivityUtc = $activityUtc
        }

        if ($lastActivityUtc) {
          $staleSec = [int](([datetime]::UtcNow - $lastActivityUtc).TotalSeconds)
          if ($staleSec -ge ($StallMinutes * 60)) {
            Write-Log "STOP job=$jobTag reason=stale_activity staleSec=$staleSec pid=$($proc.Id)"
            try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}
            $stoppedForStall = $true
            break
          }
        }

        Start-Sleep -Seconds $PollSeconds
        try { $proc.Refresh() } catch {}
      }

      try { $proc.Refresh() } catch {}
      $exitCode = if ($proc.HasExited) { $proc.ExitCode } else { -1 }

      $stats = Get-ManifestStats -OutDir $outDir -ExpectedTiles $TargetTiles
      if ($stats.Ok -gt $seenOk) {
        $delta = $stats.Ok - $seenOk
        $seenOk = $stats.Ok
        for ($i = 0; $i -lt $delta; $i++) {
          Invoke-Beeps -Count 10 -Frequency 1350 -DurationMs 150
        }
        Write-Log "PROGRESS job=$jobTag +$delta tiles | ok=$seenOk/$TargetTiles err=$($stats.Err) dry=$($stats.Dry)"
      }

      Write-Log "END job=$jobTag attempt=$($restartCount + 1) exit=$exitCode ok=$($stats.Ok)/$TargetTiles err=$($stats.Err) dry=$($stats.Dry)"

      if ($stats.Ok -ge $TargetTiles) {
        Write-Log "DONE job=$jobTag"
        break
      }

      $restartCount += 1
      $reason = if ($stoppedForNetwork) {
        "network"
      } elseif ($stoppedForStall) {
        "stall"
      } else {
        "process_exit"
      }

      Write-Log "RESTART job=$jobTag reason=$reason restart=$restartCount/$MaxJobRestarts wait=${RestartDelaySec}s"
      Start-Sleep -Seconds $RestartDelaySec
    }
  }

  Write-Log "All jobs finished. Running final validation..."

  $validateArgs = @(
    ".\\scripts\\validate_rasters.py",
    "--root", ".\\output\\flood",
    "--pattern", "additional_tiled_60km_*/tiles/*/dynamicworld/*.tif",
    "--pattern", "additional_tiled_60km_*/tiles/*/s3_olci/*.tif",
    "--pattern", "additional_tiled_60km_*/tiles/*/sentinel2_sr_harmonized/*.tif"
  )

  $validateLog = ".\\output\\_index\\autopilot_final_validate.log"
  & $python @validateArgs 2>&1 | Tee-Object -FilePath $validateLog -Append
  if ($LASTEXITCODE -ne 0) {
    throw "Final validate_rasters failed with exit code $LASTEXITCODE"
  }

  & $python ".\\scripts\\rebuild_dataset_index.py" 2>&1 | Tee-Object -FilePath $LogFile -Append
  if ($LASTEXITCODE -ne 0) {
    throw "rebuild_dataset_index failed with exit code $LASTEXITCODE"
  }

  Invoke-Beeps -Count 20 -Frequency 1500 -DurationMs 170
  Write-Log "AUTOPILOT COMPLETE."
}
catch {
  Write-Log "FATAL: $($_.Exception.Message)"
  Invoke-Beeps -Count 10 -Frequency 900 -DurationMs 220
  throw
}

param(
    [int]$IntervalSeconds = 5,
    [string]$PingTarget = "1.1.1.1",
    [ValidateSet("auto", "icmp", "tcp")]
    [string]$LatencyMode = "auto",
    [int]$TcpPort = 443,
    [int]$ConsecutiveAlertCount = 3,
    [double]$RamFreeMinGb = 4.0,
    [double]$RamJumpGb = 3.0,
    [double]$RamUsedMaxPct = 90.0,
    [double]$LatencyMaxMs = 180.0,
    [double]$LatencyJumpMs = 80.0,
    [int]$WifiSignalMinPct = 35,
    [switch]$NoBeep,
    [string]$LogFile = ""
)

$ErrorActionPreference = "SilentlyContinue"

function Write-Alert {
    param(
        [string]$Message
    )
    $line = ("[{0}] ALERT: {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
    Write-Host $line -ForegroundColor Red
    if (-not $NoBeep) {
        try { [Console]::Beep(1000, 250) } catch {}
    }
    if ($LogFile) {
        try {
            $dir = Split-Path -Parent $LogFile
            if ($dir) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
            Add-Content -Path $LogFile -Value $line
        } catch {}
    }
}

function Get-RamStatus {
    $os = Get-CimInstance Win32_OperatingSystem
    if (-not $os) { return $null }
    $totalGb = ([double]$os.TotalVisibleMemorySize * 1KB) / 1GB
    $freeGb = ([double]$os.FreePhysicalMemory * 1KB) / 1GB
    $usedGb = $totalGb - $freeGb
    $usedPct = if ($totalGb -gt 0) { ($usedGb / $totalGb) * 100.0 } else { 0.0 }
    return @{
        TotalGb = [math]::Round($totalGb, 2)
        FreeGb = [math]::Round($freeGb, 2)
        UsedGb = [math]::Round($usedGb, 2)
        UsedPct = [math]::Round($usedPct, 1)
    }
}

function Get-PingStatus {
    param(
        [string]$Target,
        [string]$Mode,
        [int]$Port
    )

    if ($Mode -in @("icmp", "auto")) {
        try {
            $pinger = New-Object System.Net.NetworkInformation.Ping
            $reply = $pinger.Send($Target, 1500)
            if ($reply -and $reply.Status -eq [System.Net.NetworkInformation.IPStatus]::Success) {
                return @{ Ok = $true; LatencyMs = [double]$reply.RoundtripTime; Method = "icmp" }
            }
        } catch {}
        if ($Mode -eq "icmp") {
            return @{ Ok = $false; LatencyMs = $null; Method = "icmp" }
        }
    }

    if ($Mode -in @("tcp", "auto")) {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $client = New-Object System.Net.Sockets.TcpClient
        try {
            $async = $client.BeginConnect($Target, $Port, $null, $null)
            $ok = $async.AsyncWaitHandle.WaitOne(2000, $false)
            if ($ok -and $client.Connected) {
                $client.EndConnect($async)
                $sw.Stop()
                return @{ Ok = $true; LatencyMs = [double]$sw.Elapsed.TotalMilliseconds; Method = "tcp:$Port" }
            }
        } catch {}
        finally {
            try { $client.Close() } catch {}
        }
        return @{ Ok = $false; LatencyMs = $null; Method = "tcp:$Port" }
    }

    return @{ Ok = $false; LatencyMs = $null; Method = "unknown" }
}

function Get-WifiStatus {
    $lines = netsh wlan show interfaces 2>$null
    if (-not $lines) {
        return @{
            Connected = $false
            SSID = "(unknown)"
            SignalPct = $null
        }
    }

    $text = ($lines -join "`n")
    $connected = $false
    $stateMatch = [regex]::Match($text, "(?im)^\s*(State|Estado)\s*:\s*(.+)$")
    if ($stateMatch.Success) {
        $stateValue = $stateMatch.Groups[2].Value.Trim().ToLowerInvariant()
        if ($stateValue -match "connected|conectado") {
            $connected = $true
        }
    }

    $ssid = "(unknown)"
    $ssidMatch = [regex]::Match($text, "(?im)^\s*SSID\s*:\s*(.+)$")
    if ($ssidMatch.Success) {
        $ssid = $ssidMatch.Groups[1].Value.Trim()
    }

    $signalPct = $null
    $sigLine = $lines | Where-Object { $_ -match ":\s*\d+\s*%" } | Select-Object -First 1
    if ($sigLine -and ($sigLine -match "(\d+)\s*%")) {
        $signalPct = [int]$matches[1]
    }

    return @{
        Connected = $connected
        SSID = $ssid
        SignalPct = $signalPct
    }
}

$prevFreeGb = $null
$prevLatencyMs = $null
$ramLowCount = 0
$ramHighCount = 0
$ramJumpCount = 0
$latFailCount = 0
$latHighCount = 0
$latJumpCount = 0
$wifiDownCount = 0
$wifiLowCount = 0

Write-Host ("Monitoring every {0}s | target={1}" -f $IntervalSeconds, $PingTarget) -ForegroundColor Cyan
Write-Host ("Consecutive alerts required: {0}" -f $ConsecutiveAlertCount) -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop." -ForegroundColor Cyan

while ($true) {
    $timestamp = Get-Date -Format "HH:mm:ss"
    $ram = Get-RamStatus
    $ping = Get-PingStatus -Target $PingTarget -Mode $LatencyMode -Port $TcpPort
    $wifi = Get-WifiStatus

    $latTxt = if ($ping.Ok -and $ping.LatencyMs -ne $null) { ("{0} ms ({1})" -f [math]::Round($ping.LatencyMs, 1), $ping.Method) } else { ("down ({0})" -f $ping.Method) }
    $wifiSigTxt = if ($wifi.SignalPct -ne $null) { ("{0}%" -f $wifi.SignalPct) } else { "n/a" }
    $line = (
        "{0} | RAM {1}/{2} GB used ({3}%) free {4} GB | Latency {5} -> {6} | WiFi {7} ({8}) signal {9}" -f
        $timestamp, $ram.UsedGb, $ram.TotalGb, $ram.UsedPct, $ram.FreeGb, $PingTarget, $latTxt,
        ($(if ($wifi.Connected) { "connected" } else { "down" })), $wifi.SSID, $wifiSigTxt
    )
    Write-Host $line

    if ($ram.FreeGb -lt $RamFreeMinGb) { $ramLowCount++ } else { $ramLowCount = 0 }
    if ($ramLowCount -eq $ConsecutiveAlertCount) {
        Write-Alert ("Low RAM free: {0} GB < {1} GB (x{2})" -f $ram.FreeGb, $RamFreeMinGb, $ConsecutiveAlertCount)
    }
    if ($ram.UsedPct -ge $RamUsedMaxPct) { $ramHighCount++ } else { $ramHighCount = 0 }
    if ($ramHighCount -eq $ConsecutiveAlertCount) {
        Write-Alert ("High RAM usage: {0}% >= {1}% (x{2})" -f $ram.UsedPct, $RamUsedMaxPct, $ConsecutiveAlertCount)
    }
    if ($prevFreeGb -ne $null -and [math]::Abs($ram.FreeGb - $prevFreeGb) -ge $RamJumpGb) {
        $ramJumpCount++
    } else {
        $ramJumpCount = 0
    }
    if ($ramJumpCount -eq $ConsecutiveAlertCount) {
        Write-Alert ("RAM free changed by {0} GB (threshold {1} GB, x{2})" -f [math]::Round([math]::Abs($ram.FreeGb - $prevFreeGb), 2), $RamJumpGb, $ConsecutiveAlertCount)
    }

    if (-not $ping.Ok -or $ping.LatencyMs -eq $null) {
        $latFailCount++
        if ($latFailCount -eq $ConsecutiveAlertCount) {
            Write-Alert ("Latency probe failed to {0} via {1} (x{2})" -f $PingTarget, $ping.Method, $ConsecutiveAlertCount)
        }
        $latHighCount = 0
        $latJumpCount = 0
    } else {
        $latFailCount = 0
        if ($ping.LatencyMs -ge $LatencyMaxMs) {
            $latHighCount++
        } else {
            $latHighCount = 0
        }
        if ($latHighCount -eq $ConsecutiveAlertCount) {
            Write-Alert ("High latency: {0} ms >= {1} ms (x{2})" -f [math]::Round($ping.LatencyMs, 1), $LatencyMaxMs, $ConsecutiveAlertCount)
        }
        if ($prevLatencyMs -ne $null -and [math]::Abs($ping.LatencyMs - $prevLatencyMs) -ge $LatencyJumpMs) {
            $latJumpCount++
        } else {
            $latJumpCount = 0
        }
        if ($latJumpCount -eq $ConsecutiveAlertCount) {
            Write-Alert ("Latency jump: {0} ms (threshold {1} ms, x{2})" -f [math]::Round([math]::Abs($ping.LatencyMs - $prevLatencyMs), 1), $LatencyJumpMs, $ConsecutiveAlertCount)
        }
    }

    if (-not $wifi.Connected) {
        $wifiDownCount++
        if ($wifiDownCount -eq $ConsecutiveAlertCount) {
            Write-Alert ("WiFi disconnected (x{0})" -f $ConsecutiveAlertCount)
        }
        $wifiLowCount = 0
    } elseif ($wifi.SignalPct -ne $null -and $wifi.SignalPct -lt $WifiSignalMinPct) {
        $wifiDownCount = 0
        $wifiLowCount++
        if ($wifiLowCount -eq $ConsecutiveAlertCount) {
            Write-Alert ("Low WiFi signal: {0}% < {1}% (x{2})" -f $wifi.SignalPct, $WifiSignalMinPct, $ConsecutiveAlertCount)
        }
    } else {
        $wifiDownCount = 0
        $wifiLowCount = 0
    }

    $prevFreeGb = $ram.FreeGb
    $prevLatencyMs = if ($ping.Ok) { $ping.LatencyMs } else { $prevLatencyMs }
    Start-Sleep -Seconds $IntervalSeconds
}

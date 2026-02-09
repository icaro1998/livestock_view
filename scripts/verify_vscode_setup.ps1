[CmdletBinding()]
param()

$ErrorActionPreference = "Continue"

$requiredExtensions = @(
    @{ Id = "ms-toolsai.jupyter"; Name = "Jupyter" },
    @{ Id = "ms-python.python"; Name = "Python" },
    @{ Id = "Google.colab"; Name = "Google Colab" }
)

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-Host "=== $Title ===" -ForegroundColor Cyan
}

function Write-InstallGuidance {
    Write-Section "Install Commands"
    Write-Host "Run these commands in PowerShell:"
    foreach ($ext in $requiredExtensions) {
        Write-Host "  code --install-extension $($ext.Id)"
    }

    Write-Section "If 'code' is not in PATH (Windows)"
    Write-Host "1. Close VS Code."
    Write-Host "2. Re-run the VS Code installer."
    Write-Host "3. Enable: 'Add to PATH' (User or System)."
    Write-Host "4. Open a new PowerShell and run: code --version"
    Write-Host ""
    Write-Host "Temporary fix for current terminal only:"
    Write-Host '  $env:Path += ";$env:LOCALAPPDATA\Programs\Microsoft VS Code\bin"'
}

Write-Section "VS Code CLI"
$codeCmd = Get-Command code -ErrorAction SilentlyContinue

if (-not $codeCmd) {
    Write-Warning "VS Code CLI ('code') was not found in PATH."
    Write-InstallGuidance
    exit 0
}

$versionOutput = & code --version 2>$null
if ($LASTEXITCODE -eq 0 -and $versionOutput) {
    Write-Host "code --version:"
    $versionOutput | Select-Object -First 3 | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Warning "Unable to read VS Code version via CLI."
}

Write-Section "Installed Extensions"
$installedExtensions = @()
$extOutput = & code --list-extensions 2>$null
if ($LASTEXITCODE -eq 0 -and $extOutput) {
    $installedExtensions = $extOutput
    $installedExtensions | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Warning "Unable to list extensions with 'code --list-extensions'."
}

Write-Section "Required Extension Status"
foreach ($ext in $requiredExtensions) {
    if ($installedExtensions -contains $ext.Id) {
        Write-Host "[OK]      $($ext.Name) ($($ext.Id))" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] $($ext.Name) ($($ext.Id))" -ForegroundColor Yellow
        Write-Host "          Install: code --install-extension $($ext.Id)"
    }
}

Write-Section "Colab Kernel Reminder"
Write-Host "In VS Code notebook: Select Kernel -> Colab -> New Colab Server"
Write-Host "If you see 'No assigned colab servers', attach a new server and reselect the kernel."

Write-InstallGuidance

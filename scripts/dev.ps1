[CmdletBinding()]
param(
    [ValidateSet("verify", "install", "lint")]
    [string]$Task = "verify"
)

$repoRoot = Split-Path -Parent $PSScriptRoot

switch ($Task) {
    "verify" {
        & "$PSScriptRoot/verify_vscode_setup.ps1"
    }
    "install" {
        Push-Location $repoRoot
        try {
            python -m pip install -r requirements.txt
        } finally {
            Pop-Location
        }
    }
    "lint" {
        Push-Location $repoRoot
        try {
            if (Get-Command ruff -ErrorAction SilentlyContinue) {
                ruff check .
            } else {
                Write-Host "ruff not installed. Install with: python -m pip install ruff"
            }

            if (Get-Command black -ErrorAction SilentlyContinue) {
                black --check .
            } else {
                Write-Host "black not installed. Install with: python -m pip install black"
            }
        } finally {
            Pop-Location
        }
    }
}

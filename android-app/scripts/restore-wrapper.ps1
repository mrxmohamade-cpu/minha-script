$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$wrapperDir = Join-Path $scriptDir "..\gradle\wrapper"
$base64File = Join-Path $wrapperDir "gradle-wrapper.jar.base64"
$jarFile = Join-Path $wrapperDir "gradle-wrapper.jar"

if (-Not (Test-Path $base64File)) {
    throw "Base64 wrapper file not found: $base64File"
}

$bytes = [System.Convert]::FromBase64String((Get-Content -Raw $base64File))
[System.IO.File]::WriteAllBytes($jarFile, $bytes)

Write-Host "Restored $jarFile"

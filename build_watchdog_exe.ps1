$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

Write-Host "Checking dependencies..."
python .\check_dependencies.py --install --include-build

$PyInstallerArgs = @(
    '--noconfirm'
    '--clean'
    '--windowed'
    '--name', 'WatchdogTerminal'
    '--collect-all', 'win11toast'
    '--collect-all', 'win10toast'
    '--hidden-import', 'bs4'
    '--hidden-import', 'requests'
    '--hidden-import', 'tkinter'
    '--distpath', (Join-Path $Root 'dist')
    '--workpath', (Join-Path $Root 'build')
    '--specpath', $Root
    '.\watchdog_gui.py'
)

Write-Host "Building WatchdogTerminal.exe..."
python -m PyInstaller @PyInstallerArgs

Write-Host "Build finished. Output folder: $Root\dist\WatchdogTerminal"

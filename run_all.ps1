<#
.SYNOPSIS
  Starts every component of both assignments plus all UIs (Windows PowerShell).

.EXAMPLE
  .\run_all.ps1              # start everything and open all UIs in the browser
  .\run_all.ps1 -Etl         # also (re)run the batch ETL flow first (~12 min)
  .\run_all.ps1 -NoBrowser   # start everything, don't open browser tabs
  .\run_all.ps1 -Stop        # stop the UIs and all containers (data volumes are kept)

  Prerequisite: assignment-1-batch-etl\setup.sh has been run once (venv + data).
  If script execution is blocked: powershell -ExecutionPolicy Bypass -File .\run_all.ps1
#>
param([switch]$Etl, [switch]$Stop, [switch]$NoBrowser)

$ErrorActionPreference = "Stop"
$Root   = $PSScriptRoot
$Batch  = Join-Path $Root "assignment-1-batch-etl"
$Stream = Join-Path $Root "assignment-2-streaming"
$RunDir = Join-Path $Root ".run"
$Py     = Join-Path $Batch ".venv\Scripts\python.exe"

# Background Python services: name, url to probe, working directory, arguments
$Services = @(
    @{ Name = "prefect";   Url = "http://127.0.0.1:4200/api/health";    Dir = $Batch;
       Args = @("-m", "prefect", "server", "start", "--host", "127.0.0.1", "--port", "4200") },
    @{ Name = "streamlit"; Url = "http://localhost:8501/_stcore/health"; Dir = $Batch;
       Args = @("-m", "streamlit", "run", "dashboard/app.py", "--server.headless", "true",
                "--server.port", "8501", "--browser.gatherUsageStats", "false") },
    @{ Name = "console";   Url = "http://localhost:8765/index.html";    Dir = $Root;
       Args = @("verification-ui/server.py") }
)

$Uis = [ordered]@{
    "Verification console" = "http://localhost:8765/"
    "Streamlit dashboard"  = "http://localhost:8501/"
    "Prefect UI"           = "http://127.0.0.1:4200/runs"
    "Grafana (admin/admin)" = "http://localhost:3300/d/taxi-streaming"
    "Prometheus"           = "http://localhost:9090/targets"
    "Kafka UI"             = "http://localhost:8088/"
}

function Log($msg) { Write-Host ("{0}  {1}" -f (Get-Date -Format "HH:mm:ss"), $msg) }

function Test-Url($url) {
    try { Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 3 | Out-Null; return $true } catch { return $false }
}

function Wait-Until($what, [scriptblock]$condition, $timeoutSec = 240) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (& $condition) { Log "$what is ready"; return }
        Start-Sleep -Seconds 3
    }
    Log "WARNING: $what did not become ready within $timeoutSec s (continuing)"
}

function Invoke-Native([string]$exe, [string[]]$arguments) {
    # Runs a native command; output goes to the console, failures don't stop the script.
    $old = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    & $exe @arguments 2>&1 | ForEach-Object { "$_" } | Where-Object { $_ -notmatch "^\s*$" } | Out-Host
    $code = $LASTEXITCODE; $ErrorActionPreference = $old; return $code
}

function Test-Docker {
    $old = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    docker info --format "{{.ServerVersion}}" *> $null
    $ok = ($LASTEXITCODE -eq 0); $ErrorActionPreference = $old; return $ok
}

function Get-ServiceProcesses {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object {
        $_.CommandLine -match "prefect.+server start|streamlit.+dashboard/app\.py|verification-ui/server\.py"
    }
}

if ($Stop) {
    Get-ServiceProcesses | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Log "stopped Prefect, Streamlit and the verification console"
    if (Test-Docker) {
        Invoke-Native "docker" @("compose", "-f", "$Stream\docker-compose.yml", "--profile", "ui", "stop") | Out-Null
        Invoke-Native "docker" @("compose", "-f", "$Batch\docker-compose.yml", "stop") | Out-Null
        Log "containers stopped (volumes kept)"
    }
    return
}

if (-not (Test-Path $Py)) { throw "No virtualenv at $Py - run assignment-1-batch-etl/setup.sh first." }
New-Item -ItemType Directory -Force $RunDir | Out-Null

# 1. Docker Desktop
if (-not (Test-Docker)) {
    Log "starting Docker Desktop"
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    Wait-Until "Docker" { Test-Docker } 300
}

# 2. Assignment 1 database
Log "starting PostgreSQL (assignment 1)"
Invoke-Native "docker" @("compose", "-f", "$Batch\docker-compose.yml", "up", "-d", "postgres") | Out-Null
Wait-Until "PostgreSQL" {
    $old = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    docker exec taxi-batch-postgres pg_isready -U taxi -d taxi_dw *> $null
    $r = ($LASTEXITCODE -eq 0); $ErrorActionPreference = $old; $r
}

# 3. Python services, detached so they keep running after this window closes
$env:PYTHONDONTWRITEBYTECODE = "1"
foreach ($svc in $Services) {
    if (Test-Url $svc.Url) { Log "$($svc.Name) already running"; continue }
    Log "starting $($svc.Name)"
    Start-Process -FilePath $Py -ArgumentList $svc.Args -WorkingDirectory $svc.Dir -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $RunDir "$($svc.Name).log") `
        -RedirectStandardError  (Join-Path $RunDir "$($svc.Name).err.log") | Out-Null
    if ($svc.Name -eq "prefect") { Wait-Until "Prefect UI" { Test-Url $svc.Url } }
}

# 4. Optional batch ETL run (recorded in the Prefect UI)
if ($Etl) {
    Log "running the batch ETL flow (~5 min per month)"
    $env:PREFECT_API_URL = "http://127.0.0.1:4200/api"
    Push-Location $Batch
    try { & $Py -m flows.taxi_etl_flow run } finally { Pop-Location }
}

# 5. Assignment 2 streaming stack + Kafka UI
Log "starting the streaming stack + Kafka UI (assignment 2)"
Invoke-Native "docker" @("compose", "-f", "$Stream\docker-compose.yml", "--profile", "ui", "up", "-d", "--build") | Out-Null

foreach ($svc in $Services) { Wait-Until $svc.Name { Test-Url $svc.Url } }
Wait-Until "Grafana"  { Test-Url "http://localhost:3300/api/health" }
Wait-Until "Kafka UI" { Test-Url "http://localhost:8088/actuator/health" }

Write-Host ""
foreach ($ui in $Uis.GetEnumerator()) { Write-Host ("  {0,-24} {1}" -f $ui.Key, $ui.Value) }
Write-Host ""
Write-Host "  Give the stream ~1 minute to produce data before the first verification run."
Write-Host "  Logs: .run\*.log    Stop everything: .\run_all.ps1 -Stop"
Write-Host ""

if (-not $NoBrowser) { foreach ($url in $Uis.Values) { Start-Process $url } }

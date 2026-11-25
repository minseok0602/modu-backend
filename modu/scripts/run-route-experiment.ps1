param([switch]$Performance)
$ErrorActionPreference = 'Stop'
$taskContainer = 'ieum-route-experiment-mysql'
$taskLabel = 'ieum-route-experiment'
$startedHere = $false
$projectPath = Split-Path $PSScriptRoot -Parent

try {
    $existing = & docker ps -a --filter "name=^/$taskContainer$" --format '{{.Names}}'
    if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop is unavailable.' }
    if ($existing) {
        $label = & docker inspect --format '{{index .Config.Labels "codex.task"}}' $taskContainer
        if ($label -ne $taskLabel) { throw 'The container name belongs to a different task.' }
        $running = & docker inspect --format '{{.State.Running}}' $taskContainer
        if ($running -ne 'true') {
            & docker start $taskContainer | Out-Null
            if ($LASTEXITCODE -ne 0) { throw 'Could not start the isolated database.' }
            $startedHere = $true
        }
    } else {
        & docker run -d --name $taskContainer --label "codex.task=$taskLabel" `
            -p '127.0.0.1:13316:3306' -e 'MYSQL_ROOT_PASSWORD=ieum-local-only' `
            -e 'MYSQL_DATABASE=ieum_experiment' --tmpfs '/var/lib/mysql:rw,size=768m' `
            mysql:8.0 --performance-schema=ON | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Could not create the isolated database.' }
        $startedHere = $true
    }
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        & docker exec $taskContainer mysqladmin ping -h 127.0.0.1 -uroot -pieum-local-only 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 1
    }
    if (!$ready) { throw 'The isolated database did not become ready.' }
    Push-Location $projectPath
    try {
        $gradleArguments = @('--no-daemon', 'routeExperiment')
        if ($Performance) { $gradleArguments += '-Dieum.performance=true' }
        & .\gradlew.bat @gradleArguments
        if ($LASTEXITCODE -ne 0) { throw 'Route experiments failed. Inspect build/reports/tests/routeExperiment.' }
    } finally { Pop-Location }
} finally {
    if ($startedHere) { & docker stop $taskContainer | Out-Null }
}

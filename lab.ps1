# Lab komutları. Tüm mantık tests/lab-check içinde; lab.sh aynı aracı çalıştırır.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location -Path $PSScriptRoot

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    Write-Host '[HATA] Java bulunamadi. Java 17 kur, sonra tekrar dene.'
    exit 1
}
$versionLine = (& { $ErrorActionPreference = 'Continue'; java -version 2>&1 } | Select-Object -First 1).ToString()
$major = 0
if ($versionLine -match '"(\d+)') { $major = [int]$Matches[1] }
if ($major -lt 17) {
    Write-Host "[HATA] Java 17 veya üzeri gerekli. Bulunan: $major"
    exit 1
}

$jar = 'tests/lab-check/target/lab-check.jar'
$rebuild = -not (Test-Path $jar)
if (-not $rebuild) {
    $built = (Get-Item $jar).LastWriteTime
    $newer = Get-ChildItem -Path 'tests/lab-check/src', 'tests/lab-check/pom.xml' -Recurse -File |
        Where-Object { $_.LastWriteTime -gt $built } | Select-Object -First 1
    $rebuild = $null -ne $newer
}
if ($rebuild) {
    Write-Host 'lab-check derleniyor...'
    $mvnw = if ($IsLinux -or $IsMacOS) { './mvnw' } else { '.\mvnw.cmd' }
    & $mvnw -q -DskipTests -pl tests/lab-check package
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

& java -jar $jar @args
exit $LASTEXITCODE

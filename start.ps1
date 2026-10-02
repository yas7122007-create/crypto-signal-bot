param([switch]$Install, [switch]$Remove)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$taskName = 'CryptoSignalBot'
if ($Install -and $Remove) { throw 'Pilih -Install atau -Remove.' }
if ($Remove) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Output 'Startup otomatis bot dinonaktifkan.'
    exit 0
}
if ($Install) {
    $taskUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $action = New-ScheduledTaskAction -Execute 'powershell.exe' -WorkingDirectory $PSScriptRoot -Argument ('-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}"' -f $PSCommandPath)
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $taskUser
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
    $principal = New-ScheduledTaskPrincipal -UserId $taskUser -LogonType Interactive -RunLevel Limited
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description ('Bot simulasi crypto lokal: ' + $PSScriptRoot) | Out-Null
    Write-Output 'Bot akan mulai saat login Windows. Log: runtime/startup.log'
    exit 0
}

$botPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $botPython)) { throw 'Virtual environment belum ada. Ikuti README.md.' }
New-Item -ItemType Directory -Path (Join-Path $PSScriptRoot 'runtime') -Force | Out-Null
Start-Transcript -Path (Join-Path $PSScriptRoot 'runtime\startup.log') -Append | Out-Null
try {
    & $botPython -u setup_local.py services
    if ($LASTEXITCODE -ne 0) { throw 'Layanan belum siap.' }
    & $botPython -u setup_local.py doctor
    if ($LASTEXITCODE -ne 0) { Write-Output 'Ada layanan belum siap; siklus terjadwal akan mencoba kembali. Lihat pemeriksaan di atas.' }
    & $botPython -u bot.py serve
    if ($LASTEXITCODE -ne 0) { throw 'Bot berhenti karena kesalahan; Task Scheduler akan mencoba ulang.' }
} catch {
    Write-Output $_.Exception.Message
    exit 1
} finally {
    Stop-Transcript | Out-Null
}

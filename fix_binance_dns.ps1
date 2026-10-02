param([switch]$Undo)
$ErrorActionPreference = 'Stop'
$botIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$botPrincipal = New-Object Security.Principal.WindowsPrincipal($botIdentity)
if (-not $botPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Jalankan PowerShell sebagai Administrator untuk mengatur DNS Windows.'
}
$botBackupPath = Join-Path $PSScriptRoot 'runtime\binance-dns-backup.json'
$botLogPath = Join-Path $PSScriptRoot 'runtime\binance-dns-fix.log'
$botRuleLabel = 'Crypto Signal Bot - Binance DNS'
Start-Transcript -Path $botLogPath -Append | Out-Null

function Restore-BotDns($state) {
    if ($state.ServerAddress -ne '8.8.8.8') { throw 'Backup DNS tidak sesuai.' }
    if ($state.RuleName) {
        $rule = Get-DnsClientNrptRule -Name $state.RuleName -ErrorAction SilentlyContinue
        if ($rule) {
            if ($rule.DisplayName -ne $botRuleLabel -or $rule.Namespace.Count -ne 1 -or
                $rule.Namespace[0] -ne 'fapi.binance.com' -or $rule.NameServers -ne '8.8.8.8') {
                throw 'Aturan DNS telah berubah; pembatalan otomatis dihentikan.'
            }
            Remove-DnsClientNrptRule -Name $rule.Name -Force
        }
    }
    Set-DnsClientDohServerAddress -ServerAddress '8.8.8.8' -AutoUpgrade ([bool]$state.AutoUpgrade) -AllowFallbackToUdp ([bool]$state.AllowFallbackToUdp)
    Clear-DnsClientCache
    $state.Active = $false
    [IO.File]::WriteAllText($botBackupPath, ($state | ConvertTo-Json))
    Write-Output 'Pengaturan DNS sebelumnya dipulihkan.'
}

try {
    if ($Undo) {
        $botState = Get-Content -LiteralPath $botBackupPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($botState.Active) { Restore-BotDns $botState }
        else { Write-Output 'Perbaikan DNS sudah tidak aktif.' }
    } else {
        if (Test-Path -LiteralPath $botBackupPath) {
            $botPrevious = Get-Content -LiteralPath $botBackupPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($botPrevious.Active) { throw 'Perbaikan sudah aktif. Gunakan -Undo untuk membatalkan.' }
        }
        if (@(Get-DnsClientNrptRule | Where-Object { $_.Namespace -contains 'fapi.binance.com' }).Count) {
            throw 'Sudah ada aturan DNS untuk Binance; periksa aturan tersebut terlebih dahulu.'
        }
        $botDoh = Get-DnsClientDohServerAddress -ServerAddress '8.8.8.8'
        if ($botDoh.DohTemplate -ne 'https://dns.google/dns-query') { throw 'Konfigurasi Google DoH tidak sesuai.' }
        $botState = [PSCustomObject]@{ServerAddress='8.8.8.8';AutoUpgrade=[bool]$botDoh.AutoUpgrade;
            AllowFallbackToUdp=[bool]$botDoh.AllowFallbackToUdp;RuleName='';Active=$true}
        [IO.File]::WriteAllText($botBackupPath, ($botState | ConvertTo-Json))
        try {
            Set-DnsClientDohServerAddress -ServerAddress '8.8.8.8' -AutoUpgrade $true -AllowFallbackToUdp $false
            $botRule = Add-DnsClientNrptRule -Namespace 'fapi.binance.com' -NameServers '8.8.8.8' -DisplayName $botRuleLabel -PassThru
            $botState.RuleName = $botRule.Name
            [IO.File]::WriteAllText($botBackupPath, ($botState | ConvertTo-Json))
            Clear-DnsClientCache
            & (Join-Path $PSScriptRoot '.venv\Scripts\python.exe') (Join-Path $PSScriptRoot 'analyze.py') --symbol BTCUSDT
            if ($LASTEXITCODE -ne 0) { throw 'Analisis live gagal; pengaturan DNS dibatalkan.' }
            Write-Output 'PASS: analisis Binance live berhasil melalui DNS Windows yang diperbaiki.'
        } catch {
            Restore-BotDns $botState
            throw
        }
    }
} finally {
    Stop-Transcript | Out-Null
}

# Bot analisis crypto Python


## Status fitur

Bot melakukan scan terjadwal, konfirmasi AI, paper trading, dan pengiriman sinyal teks ke Telegram jika diaktifkan. Gambar sinyal, perintah `/signal`, dan chat Telegram belum tersedia.

Engine membaca candle tertutup 15m, 1h, dan 4h untuk menghasilkan kandidat LONG, SHORT, atau HOLD beserta alasannya. Perhitungan mencakup EMA20/50, ATR, support/resistance, volume relatif, taker delta, sweep, breakout, dan harga entry/SL/TP.

## Menjalankan engine

Jalankan PowerShell dari folder proyek. Virtual environment lokal sudah tersedia; untuk instalasi baru gunakan `python -m venv .venv`, lalu `.\.venv\Scripts\python.exe -m pip install -r requirements.txt`.

Contoh offline yang disediakan memakai **data sintetis**, bukan harga pasar atau hasil backtest profitabilitas:

```powershell
.\.venv\Scripts\python.exe analyze.py --input reports\synthetic-snapshot.json
```

Mengambil data Binance Futures dan menyimpan input agar dapat dianalisis ulang:

```powershell
.\.venv\Scripts\python.exe analyze.py --symbol BTCUSDT --save-snapshot reports\btc-snapshot.json
.\.venv\Scripts\python.exe analyze.py --input reports\btc-snapshot.json
```

`--save-snapshot` membuat file baru dan menolak menimpa file yang sudah ada. Buat folder tujuan terlebih dahulu jika memakai lokasi lain. Pengambilan live memerlukan akses ke Binance; kegagalan HTTP dilaporkan dengan exit code 1 tanpa mengganti data dengan data sintetis.

Hasil JSON berlabel `ANALYSIS_ONLY` dan `market_checks: NOT_RUN`. LONG/SHORT di sini adalah kandidat engine: pemeriksaan spread, funding, jurnal, serta konfirmasi AI dilakukan pada alur bot penuh. Perintah ini tidak membuat sinyal tersimpan, mengirim Telegram, atau memasang order. HOLD adalah hasil analisis normal dengan exit code 0.

Mode offline hanya memakai standard library Python, tanpa layanan database, Prefect, Ollama, maupun koneksi jaringan. Mode live menggunakan client Binance yang sudah ada dan konfigurasi aturan dari `.env`.

## Format snapshot

| Field | Isi |
| --- | --- |
| `symbol` | Simbol uppercase seperti `BTCUSDT` |
| `tick` | Tick size kontrak, misalnya `"0.10"` |
| `asof_ms` | Waktu acuan analisis dalam milidetik UTC, bilangan bulat |
| `frames` | Objek dengan kunci `15m`, `1h`, dan `4h`; masing-masing berisi array kline Binance mentah |
| `rules` | Opsional; field dari `engine.Rules`. Jika dihilangkan, memakai default engine, bukan `.env` |

Setiap timeframe memerlukan sedikitnya 60 candle tertutup berurutan, tanpa celah atau duplikat, hingga candle terakhir sebelum `asof_ms`. Replay memakai waktu snapshot tersebut, sehingga hasilnya merupakan analisis historis.

Urutan field kline mengikuti [dokumentasi market data Binance](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data): open time, open, high, low, close, volume, close time, quote volume, jumlah transaksi, taker buy base volume, taker buy quote volume, dan field ignore. `tick` berasal dari `PRICE_FILTER.tickSize` pada exchange information.

## Pengujian

```powershell
.\.venv\Scripts\python.exe check.py
.\.venv\Scripts\python.exe check.py --db
```

Pemeriksaan kedua memerlukan MariaDB yang terkonfigurasi dan tabel bot; perubahan uji database di-rollback. Pemeriksaan mencakup arah LONG/SHORT/HOLD, candle tertutup, data tidak valid, biaya/funding simulasi, replay JSON tanpa dependency, penolakan overwrite, dan kegagalan AI. Pengujian ini memakai data sintetis.

## Pemulihan DNS Binance

Pada komputer ini, DNS jaringan memberikan alamat Binance yang berbeda dari DNS publik. Mengakses alamat hasil DNS publik dengan hostname dan verifikasi HTTPS yang sama berhasil HTTP 200. Perbaikan sudah diterapkan menggunakan aturan [NRPT Windows](https://learn.microsoft.com/en-us/powershell/module/dnsclient/add-dnsclientnrptrule) khusus `fapi.binance.com` melalui Google DNS over HTTPS. Pengaturan DNS adaptor Wi-Fi tetap seperti sebelumnya.

`fix_binance_dns.ps1` menyimpan konfigurasi sebelumnya di `runtime/binance-dns-backup.json`, menjalankan analisis live untuk memeriksa hasilnya, dan memulihkan konfigurasi jika pemeriksaan gagal. Log ada di `runtime/binance-dns-fix.log`. Script memerlukan hak administrator dan menolak penerapan ulang selama perbaikan masih aktif.

Untuk membatalkan perbaikan, jalankan dari PowerShell Administrator:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\fix_binance_dns.ps1 -Undo
```

Analisis Python sehari-hari tetap dapat dijalankan dari terminal biasa.

## Bot terjadwal

Alur yang sudah ada tetap dijalankan melalui `start.ps1`, dengan MariaDB, PostgreSQL untuk Prefect, dan model Ollama yang dikonfigurasi di `.env`. Periksa kesiapan layanan dengan:

```powershell
.\.venv\Scripts\python.exe setup_local.py doctor
```

Untuk instalasi layanan baru, sediakan MariaDB dan Ollama. Jalankan `setup_local.py prepare` untuk membuat `.env` beserta password lokal dan tabel bot; sesuaikan kredensial MariaDB bila diperlukan. Unduh model Ollama sesuai `OLLAMA_MODEL`, lalu jalankan `setup_local.py install-postgres` dan `setup_local.py services` menggunakan Python virtual environment. Simpan token/password hanya di `.env`. Pengiriman Telegram dikendalikan oleh `TELEGRAM_ENABLED` dan default-nya `false`.

## Hermes Agent lokal

Bot mendukung `AI_PROVIDER=ollama` (langsung) atau `AI_PROVIDER=hermes` (melalui Hermes Agent). Hermes hanya memeriksa kandidat dan mengembalikan JSON `CONFIRM`/`HOLD`; engine tetap menentukan entry, SL, TP, serta batas risiko. Respons gagal, terpotong, tidak valid, atau melewati batas waktu menjadi HOLD.

Hermes menggunakan profil khusus `crypto-signal-bot` di `%LOCALAPPDATA%\hermes\profiles\crypto-signal-bot`. Konfigurasi sumbernya adalah `hermes-config.yaml`, dengan toolset CLI kosong dan model Ollama lokal. Profil utama Hermes tetap dapat digunakan untuk keperluan lain. Integrasi memakai [CLI JSONL resmi Hermes](https://hermes-agent.nousresearch.com/docs/reference/cli-commands), tanpa server tambahan.

Hermes membutuhkan konteks minimal 64.000 token. Model Qwen 3 8B yang lama tidak memenuhi batas itu. [Qwen 3.5 9B](https://ollama.com/library/qwen3.5:9b) diunduh sekali (sekitar 6,6 GB), lalu dibuat alias dengan konteks 65.536 melalui `Hermes.Modelfile`:

```powershell
& "$env:LOCALAPPDATA\Programs\Ollama\ollama.exe" pull qwen3.5:9b
& "$env:LOCALAPPDATA\Programs\Ollama\ollama.exe" create crypto-hermes -f .\Hermes.Modelfile
.\.venv\Scripts\python.exe setup_local.py hermes
```

Setel di `.env` setelah model siap:

```dotenv
AI_PROVIDER=hermes
OLLAMA_MODEL=crypto-hermes:latest
HERMES_TIMEOUT_SECONDS=180
```

`HERMES_EXECUTABLE` boleh diisi dengan path `hermes.exe` jika tidak ditemukan otomatis. Setelah mengedit `hermes-config.yaml`, jalankan ulang `setup_local.py hermes` untuk menyalinnya ke profil bot. Perubahan model juga harus disesuaikan dengan `OLLAMA_MODEL` untuk pemeriksaan kesiapan. Periksa integrasi sungguhan dengan:

```powershell
.\.venv\Scripts\python.exe check.py --db --hermes
```

Uji ini meminta HOLD untuk data pasar yang tidak tersedia; tidak mengirim pesan dan tidak membuat order. Model lokal memakai RAM/GPU laptop dan tidak memerlukan saldo OpenRouter.

## Otomatis saat laptop dipakai

Daftarkan sekali dari PowerShell:

```powershell
.\start.ps1 -Install
```

Task Scheduler `CryptoSignalBot` menjalankan bot tersembunyi saat pengguna login Windows, termasuk saat memakai baterai. Bot berjalan hanya ketika laptop aktif; saat sleep atau mati, pemantauan berhenti. Setelah bangun dari sleep, Windows melanjutkan proses yang masih berjalan. Koneksi internet diperlukan untuk data Binance.

Mulai atau hentikan bot pada sesi Windows sekarang:

```powershell
Start-ScheduledTask -TaskName CryptoSignalBot
Stop-ScheduledTask -TaskName CryptoSignalBot
```

Hapus startup otomatis dengan `.\start.ps1 -Remove`; gunakan perintah stop di atas jika ingin menghentikan bot yang sedang berjalan. Task Scheduler mencegah dua instance dari task yang sama dan mencoba ulang sampai tiga kali jika proses startup gagal. Gangguan koneksi saat bot berjalan dicoba kembali pada siklus terjadwal berikutnya.

Lihat log di `runtime/startup.log`, status layanan dengan `setup_local.py doctor`, hasil simulasi dengan `bot.py report`, dan jadwal di http://127.0.0.1:4200. MariaDB harus berjalan sebagai layanan otomatis Windows. Telegram baru aktif setelah token/chat ID diisi dan `TELEGRAM_ENABLED=true`.

`requirements.txt` membatasi SQLAlchemy di bawah 2.1 karena [bug kompatibilitas scheduler Prefect](https://github.com/PrefectHQ/prefect/issues/23199). Status API sehat saja belum membuktikan jadwal bekerja; pastikan flow run terjadwal muncul dan selesai di dashboard.

Setelah bot berjalan, `python check.py --scheduler` dari virtual environment memeriksa bahwa scheduler aktif dan sudah membuat flow run. Pemeriksaan ini hanya membaca status.

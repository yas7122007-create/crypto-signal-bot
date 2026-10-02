"""Local Windows setup. Secrets stay in .env and are never printed."""
import argparse
import json
import logging
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
from tempfile import TemporaryFile
from urllib.parse import urlparse, unquote
import zipfile

import httpx
import psycopg
from psycopg import sql
import pymysql
from dotenv import dotenv_values, load_dotenv

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"
PG_URL = "https://get.enterprisedb.com/postgresql/postgresql-17.11-3-windows-x64-binaries.zip"
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
load_dotenv(ROOT / ".env")


def environment():
    return {**os.environ, **{k: v for k, v in dotenv_values(ROOT / ".env").items() if v is not None},
            "PREFECT_HOME": str(RUNTIME / "prefect")}


def run(args, **kwargs):
    # Windows daemons can inherit PIPE handles and prevent communicate() from returning.
    with TemporaryFile(mode="w+", encoding="utf-8") as output:
        result = subprocess.run([str(a) for a in args], cwd=ROOT, creationflags=NO_WINDOW,
                                stdout=output, stderr=subprocess.STDOUT, timeout=90, **kwargs)
        output.seek(0)
        text = output.read()
    if result.returncode:
        (RUNTIME / "setup-command.log").write_text(text, encoding="utf-8")
        raise RuntimeError(f"{Path(args[0]).name} gagal (exit {result.returncode}); periksa runtime/setup-command.log")
    return text


def prepare():
    RUNTIME.mkdir(exist_ok=True)
    envfile = ROOT / ".env"
    if not envfile.exists():
        text = (ROOT / ".env.example").read_text(encoding="utf-8")
        text = text.replace("DB_PASSWORD=\n", "DB_PASSWORD=" + secrets.token_urlsafe(24) + "\n")
        text = text.replace("CHANGE_ME", secrets.token_urlsafe(24))
        text += "\nPG_ADMIN_PASSWORD=" + secrets.token_urlsafe(24) + "\n"
        envfile.write_text(text, encoding="utf-8")
    load_dotenv(envfile, override=True)
    name, user = os.environ["DB_NAME"], os.environ["DB_USER"]
    if not all(re.fullmatch(r"[A-Za-z0-9_]+", v) for v in (name, user)):
        raise ValueError("Nama database/user hanya boleh huruf, angka, underscore")
    if not os.environ.get("DB_PASSWORD"):
        raise ValueError("Isi DB_PASSWORD di .env")
    conn = pymysql.connect(host=os.environ["DB_HOST"], port=int(os.environ["DB_PORT"]),
                           user=os.getenv("MYSQL_ADMIN_USER", "root"),
                           password=os.getenv("MYSQL_ADMIN_PASSWORD", ""), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            for host in ("localhost", "127.0.0.1"):
                cur.execute("CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s", (user, host, os.environ["DB_PASSWORD"]))
                cur.execute(f"GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,INDEX,REFERENCES ON `{name}`.* TO %s@%s", (user, host))
    finally:
        conn.close()
    from services import database, init_schema
    with database() as db:
        init_schema(db)
    print("MariaDB dan .env siap. Password disimpan lokal; tidak ditampilkan.")


def install_postgres():
    RUNTIME.mkdir(exist_ok=True)
    binary = RUNTIME / "pgsql" / "bin" / "postgres.exe"
    if binary.exists():
        print("Binary PostgreSQL sudah tersedia.")
        return
    archive = RUNTIME / "postgresql.zip"
    if not archive.exists():
        partial = RUNTIME / "postgresql.zip.part"
        print("Mengunduh PostgreSQL dari EDB...", flush=True)
        with httpx.stream("GET", PG_URL, follow_redirects=True, timeout=60) as response:
            response.raise_for_status()
            total, checkpoint = 0, 0
            with partial.open("wb") as output:
                for chunk in response.iter_bytes(1024 * 1024):
                    output.write(chunk)
                    total += len(chunk)
                    if total > 1_000_000_000:
                        raise RuntimeError("Ukuran arsip PostgreSQL tidak wajar")
                    if total - checkpoint > 50_000_000:
                        print(f"PostgreSQL: {total // 1_000_000} MB", flush=True)
                        checkpoint = total
        partial.replace(archive)
    with zipfile.ZipFile(archive) as source:
        for item in source.infolist():
            if not item.filename.startswith(("pgsql/bin/", "pgsql/lib/", "pgsql/share/")):
                continue
            target = (RUNTIME / item.filename).resolve()
            if not target.is_relative_to(RUNTIME.resolve()):
                raise RuntimeError("Path arsip tidak aman")
            source.extract(item, RUNTIME)
    print("PostgreSQL portable siap.")


def postgres():
    cfg = environment()
    if not cfg.get("PG_ADMIN_PASSWORD"):
        raise ValueError("Jalankan prepare lebih dahulu; PG_ADMIN_PASSWORD belum ada")
    binpath, data = RUNTIME / "pgsql" / "bin", RUNTIME / "pgdata"
    if not (binpath / "postgres.exe").exists():
        raise ValueError("Jalankan install-postgres terlebih dahulu")
    address = urlparse(cfg["PREFECT_SERVER_DATABASE_CONNECTION_URL"])
    port = address.port or 5433
    if address.hostname not in ("127.0.0.1", "localhost"):
        raise ValueError("Setup portable hanya untuk localhost")
    if not (data / "PG_VERSION").exists():
        password_file = RUNTIME / "init-password.txt"
        password_file.write_text(cfg["PG_ADMIN_PASSWORD"], encoding="utf-8")
        try:
            output = run([binpath / "initdb.exe", "-D", data, "-U", "postgres_local",
                          "--encoding=UTF8", "--locale=C", "--auth=scram-sha-256",
                          "--pwfile=" + str(password_file)])
            (RUNTIME / "initdb.log").write_text(output, encoding="utf-8")
        finally:
            password_file.unlink(missing_ok=True)
    status = subprocess.run([str(binpath / "pg_ctl.exe"), "-D", str(data), "status"],
                            capture_output=True, creationflags=NO_WINDOW)
    if status.returncode:
        run([binpath / "pg_ctl.exe", "-D", data, "-l", RUNTIME / "postgres.log", "-w",
             "-o", f"-h 127.0.0.1 -p {port}", "start"])
    with psycopg.connect(host=address.hostname, port=port, user="postgres_local",
                         password=cfg["PG_ADMIN_PASSWORD"], dbname="postgres", autocommit=True, connect_timeout=5) as conn:
        user, password, dbname = unquote(address.username), unquote(address.password), address.path.lstrip("/")
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (user,)).fetchone():
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(user), sql.Literal(password)))
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,)).fetchone():
            conn.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(dbname), sql.Identifier(user)))
    print(f"PostgreSQL untuk Prefect siap di 127.0.0.1:{port}.")


def ready(url):
    try:
        return httpx.get(url, timeout=3).status_code == 200
    except httpx.HTTPError:
        return False


def background(name, args, extra_env=None):
    RUNTIME.mkdir(exist_ok=True)
    with (RUNTIME / f"{name}.log").open("a", encoding="utf-8") as log:
        process = subprocess.Popen([str(a) for a in args], cwd=ROOT, env={**environment(), **(extra_env or {})},
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                   creationflags=NO_WINDOW)
    (RUNTIME / f"{name}.pid").write_text(str(process.pid), encoding="ascii")
    print(f"{name} dimulai; log: runtime/{name}.log")


def services():
    postgres()
    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    if not ready(ollama_url + "/api/tags"):
        executable = Path(os.environ["LOCALAPPDATA"]) / "Programs" / "Ollama" / "ollama.exe"
        if not executable.exists():
            raise ValueError("Ollama belum terpasang: https://ollama.com/download/windows")
        background("ollama-server", [executable, "serve"], {"OLLAMA_HOST": "127.0.0.1:11434"})
    api = os.getenv("PREFECT_API_URL", "http://127.0.0.1:4200/api")
    if not ready(api + "/health"):
        background("prefect-server", [sys.executable, "-m", "prefect", "server", "start", "--host", "127.0.0.1"])
    for _ in range(30):
        if ready(api + "/health") and ready(ollama_url + "/api/tags"):
            print("Prefect dan Ollama siap.")
            return
        time.sleep(1)
    raise ValueError("Layanan belum siap. Periksa runtime/*.log lalu jalankan doctor.")


def hermes():
    from services import hermes_executable, hermes_home
    if not Path(hermes_executable()).is_file():
        raise ValueError("Hermes Agent belum terpasang; isi HERMES_EXECUTABLE di .env")
    home = hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / "hermes-config.yaml", home / "config.yaml")
    print("Profil Hermes crypto-signal-bot siap; model lokal dari hermes-config.yaml.")


def doctor():
    failed = 0
    from services import database, query, safe_error
    try:
        with database() as db:
            version = query(db, "SELECT VERSION() AS version")[0]["version"]
            tables = query(db, "SHOW TABLES")
        print(f"OK MariaDB {version}; {len(tables)} tabel")
    except Exception as exc:
        print("BELUM SIAP MariaDB:", safe_error(exc))
        failed += 1
    cfg = environment()
    try:
        url = cfg["PREFECT_SERVER_DATABASE_CONNECTION_URL"].replace("postgresql+asyncpg://", "postgresql://", 1)
        with psycopg.connect(url, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        print("OK PostgreSQL Prefect")
    except Exception as exc:
        print("BELUM SIAP PostgreSQL:", type(exc).__name__)
        failed += 1
    for label, url in (("Prefect", cfg.get("PREFECT_API_URL", "http://127.0.0.1:4200/api") + "/health"),
                        ("Binance Futures", cfg.get("BINANCE_URL", "https://fapi.binance.com") + "/fapi/v1/time"),
                        ("Ollama", cfg.get("OLLAMA_URL", "http://127.0.0.1:11434") + "/api/tags")):
        try:
            response = httpx.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            if label == "Ollama":
                models = [m["name"] for m in payload["models"]]
                expected = cfg.get("OLLAMA_MODEL", "qwen3:8b")
                if expected not in models:
                    raise ValueError(f"Model {expected} belum diunduh")
                print("OK Ollama:", expected)
            else:
                print("OK", label)
        except Exception as exc:
            print("BELUM SIAP", label + ":", safe_error(exc))
            failed += 1
    configured = bool(cfg.get("TELEGRAM_BOT_TOKEN") and cfg.get("TELEGRAM_CHAT_ID"))
    provider = cfg.get("AI_PROVIDER", "ollama").lower()
    print("Pemeriksa AI:", provider)
    if provider == "hermes":
        from services import hermes_executable, hermes_home
        if not Path(hermes_executable()).is_file() or not (hermes_home() / "config.yaml").is_file():
            print("BELUM SIAP Hermes: jalankan setup_local.py hermes")
            failed += 1
    elif provider != "ollama":
        print("BELUM SIAP AI_PROVIDER: harus ollama atau hermes")
        failed += 1
    print("Telegram:", "konfigurasi terisi" if configured else "isi token dan chat ID di .env")
    print("Pengiriman Telegram:", cfg.get("TELEGRAM_ENABLED", "false"))
    return 1 if failed else 0


def telegram_chat():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", token):
        raise ValueError("Isi TELEGRAM_BOT_TOKEN di .env dahulu")
    # Read only. No messages sent and no updates acknowledged via an offset.
    response = httpx.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"timeout": 0}, timeout=15)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise ValueError("Telegram menolak getUpdates")
    chats = {str(u["message"]["chat"]["id"]): u["message"]["chat"].get("type", "unknown")
             for u in data["result"] if "message" in u}
    print(json.dumps(chats) if chats else "Buka bot di Telegram, tekan Start, lalu jalankan lagi.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "install-postgres", "postgres", "services", "doctor", "hermes", "telegram-chat"])
    args = parser.parse_args()
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        if args.command == "doctor":
            raise SystemExit(doctor())
        {"prepare": prepare, "install-postgres": install_postgres, "postgres": postgres,
         "services": services, "hermes": hermes, "telegram-chat": telegram_chat}[args.command]()
    except Exception as exc:
        from services import safe_error
        print("GAGAL:", safe_error(exc))
        raise SystemExit(1)

import os
import socket
from app import app
from utils.backup import start_daily_backup

try:
    from waitress import serve as waitress_serve
except Exception:
    waitress_serve = None


def _detect_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


if __name__ == "__main__":
    host = os.getenv("HOST") or os.getenv("FLASK_RUN_HOST") or "0.0.0.0"
    port = int(os.getenv("PORT") or os.getenv("FLASK_RUN_PORT") or "5000")
    threads = int(os.getenv("THREADS") or "8")

    lan_ip = _detect_lan_ip()
    print("==== Server Configuration ====")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Threads: {threads}")
    print(f"Local:   http://127.0.0.1:{port}/")
    print(f"LAN:     http://{lan_ip}:{port}/")
    print("================================")

    # Start daily backup at configured time (defaults: 07:00 local time)
    try:
        backup_dir = os.getenv("BACKUP_DIR", r"D:\TS\BACKUP")
        backup_hour = int(os.getenv("BACKUP_HOUR", "7"))
        backup_minute = int(os.getenv("BACKUP_MINUTE", "0"))
        start_daily_backup(app, backup_dir=backup_dir, hour=backup_hour, minute=backup_minute)
        print(f"[Backup] Daily backup enabled -> dir={backup_dir}, time={backup_hour:02d}:{backup_minute:02d}")
    except Exception as ex:
        print(f"[Backup] Failed to start daily backup scheduler: {ex}")

    if waitress_serve:
        try:
            waitress_serve(app, host=host, port=port, threads=threads)
        except Exception as e:
            print(f"Waitress failed ({e}), falling back to Flask dev server...")
            app.run(host=host, port=port)
    else:
        app.run(host=host, port=port)

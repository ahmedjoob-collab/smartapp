import os
import threading
import time
from datetime import datetime, timedelta
import sqlite3
from utils.settings import load_settings


def _parse_sqlite_path(sqlalchemy_uri: str, fallback_path: str) -> str:
    """Extract the filesystem path from a SQLAlchemy sqlite URI.
    Example: sqlite:///C:/path/to/db.sqlite -> C:/path/to/db.sqlite
    """
    try:
        if sqlalchemy_uri and sqlalchemy_uri.lower().startswith("sqlite:///"):
            return sqlalchemy_uri[10:]  # remove 'sqlite:///'
    except Exception:
        pass
    return fallback_path


def _backup_sqlite(src_path: str, dest_path: str) -> None:
    """Perform a consistent SQLite backup using the sqlite3 backup API."""
    src = None
    dest = None
    try:
        src = sqlite3.connect(src_path)
        dest = sqlite3.connect(dest_path)
        with dest:
            src.backup(dest)
    finally:
        try:
            if dest:
                dest.close()
        except Exception:
            pass
        try:
            if src:
                src.close()
        except Exception:
            pass


def start_daily_backup(app, backup_dir: str, hour: int = 7, minute: int = 0) -> None:
    """Start a daemon thread that backs up the app database every day at the given local time.

    - backup_dir: destination directory, will be created if missing.
    - hour/minute: local time to run each day.
    """
    # Determine DB file path from app config, fallback to instance/database.db
    base_dir = app.root_path if hasattr(app, 'root_path') else os.path.abspath(os.path.dirname(__file__))
    fallback_db = os.path.join(base_dir, 'instance', 'database.db')
    db_uri = app.config.get('SQLALCHEMY_DATABASE_URI', '')
    db_path = _parse_sqlite_path(db_uri, fallback_db)

    # Ensure destination directory exists
    try:
        os.makedirs(backup_dir, exist_ok=True)
    except Exception:
        pass

    def _loop():
        while True:
            now = datetime.now()
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            delay = (target - now).total_seconds()
            time.sleep(max(1, int(delay)))

            # Run backup
            try:
                # Reload backup_dir from settings if available
                try:
                    settings = load_settings(app)
                    bd = settings.get('backup_dir')
                    if bd:
                        # ensure directory exists
                        os.makedirs(bd, exist_ok=True)
                        effective_backup_dir = bd
                    else:
                        effective_backup_dir = backup_dir
                except Exception:
                    effective_backup_dir = backup_dir
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'database_{ts}.db'
                dest_path = os.path.join(effective_backup_dir, filename)
                _backup_sqlite(db_path, dest_path)
                print(f"[Backup] Database backed up to: {dest_path}")
            except Exception as ex:
                print(f"[Backup] Failed to backup database: {ex}")

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
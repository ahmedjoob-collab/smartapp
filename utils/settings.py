import os
import json


def _settings_path(app) -> str:
    base_dir = os.path.abspath(os.path.dirname(__file__))
    # على Railway/Render نستخدم /tmp لأنه قابل للكتابة في وقت التشغيل
    if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RENDER"):
        try:
            os.makedirs("/tmp", exist_ok=True)
        except Exception:
            pass
        return os.path.join("/tmp", 'app_settings.json')

    # المشروع محليًا: مجلد instance داخل الجذر
    root = os.path.dirname(base_dir)
    return os.path.join(root, 'instance', 'app_settings.json')


def load_settings(app) -> dict:
    """Load application settings JSON from instance/app_settings.json.
    Returns an empty dict if file doesn't exist or is invalid.
    """
    path = _settings_path(app)
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def save_settings(app, new_settings: dict) -> None:
    """Persist settings to instance/app_settings.json. Creates directory if needed."""
    path = _settings_path(app)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(new_settings or {}, f, ensure_ascii=False, indent=2)
    except Exception:
        # best-effort persistence; swallow exceptions to avoid crashing UI
        pass


def get_setting(app, key: str, default=None):
    s = load_settings(app)
    return s.get(key, default)
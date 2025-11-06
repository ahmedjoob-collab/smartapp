# routes/settings.py
import os
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, current_app
from flask_login import login_required, current_user
from utils.settings import load_settings, save_settings
from utils.backup import _parse_sqlite_path, _backup_sqlite

settings_bp = Blueprint('settings_bp', __name__)


def _require_settings_permission() -> bool:
    is_admin = getattr(current_user, 'role', None) == 'admin'
    can_settings = bool(getattr(current_user, 'can_settings', False))
    return is_admin or can_settings


@settings_bp.route('/settings', methods=['GET'])
@login_required
def index():
    if not _require_settings_permission():
        # عرض الصفحة بشكل محدود دون إجراءات
        return render_template('settings.html', title='الإعدادات العامة', settings={}, readonly=True)

    s = load_settings(current_app)
    # تحضير مسار الصوت للعرض إن وُجد
    sound_filename = s.get('reminder_sound_filename')
    sound_url = None
    if sound_filename:
        sound_url = url_for('uploads_sound', filename=sound_filename)
    return render_template('settings.html', title='الإعدادات العامة', settings=s, sound_url=sound_url, readonly=False)


@settings_bp.route('/settings/backup_dir', methods=['POST'])
@login_required
def update_backup_dir():
    if not _require_settings_permission():
        flash('ليس لديك صلاحية لتعديل الإعدادات.', 'danger')
        return redirect(url_for('settings_bp.index'))

    backup_dir = (request.form.get('backup_dir') or '').strip()
    if not backup_dir:
        flash('يرجى إدخال مسار صالح للنسخة الاحتياطية.', 'warning')
        return redirect(url_for('settings_bp.index'))

    try:
        os.makedirs(backup_dir, exist_ok=True)
    except Exception as ex:
        flash(f'تعذر إنشاء/الوصول إلى المجلد: {ex}', 'danger')
        return redirect(url_for('settings_bp.index'))

    s = load_settings(current_app)
    s['backup_dir'] = backup_dir
    save_settings(current_app, s)
    flash('تم تحديث مسار النسخة الاحتياطية بنجاح.', 'success')
    return redirect(url_for('settings_bp.index'))


@settings_bp.route('/settings/export', methods=['GET'])
@login_required
def export_backup():
    if not _require_settings_permission():
        flash('ليس لديك صلاحية لتصدير النسخة الاحتياطية.', 'danger')
        return redirect(url_for('settings_bp.index'))

    # تحديد مسار قاعدة البيانات
    base_dir = os.path.abspath(os.path.dirname(__file__))
    root = os.path.dirname(base_dir)
    fallback_db = os.path.join(root, 'instance', 'database.db')
    db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
    db_path = _parse_sqlite_path(db_uri, fallback_db)

    # إنشاء نسخة مؤقتة للتحميل
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    tmp_name = f'database_{ts}.db'
    tmp_dir = os.path.join(root, 'instance', 'tmp_export')
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, tmp_name)

    try:
        _backup_sqlite(db_path, tmp_path)
    except Exception as ex:
        flash(f'تعذر إنشاء النسخة الاحتياطية: {ex}', 'danger')
        return redirect(url_for('settings_bp.index'))

    return send_file(tmp_path, as_attachment=True, download_name=tmp_name)


@settings_bp.route('/settings/import', methods=['POST'])
@login_required
def import_backup():
    if not _require_settings_permission():
        flash('ليس لديك صلاحية لاستيراد نسخة احتياطية.', 'danger')
        return redirect(url_for('settings_bp.index'))

    file = request.files.get('backup_file')
    if not file or not file.filename:
        flash('يرجى اختيار ملف نسخة احتياطية بصيغة .db', 'warning')
        return redirect(url_for('settings_bp.index'))

    # حفظ الملف المرفوع مؤقتًا
    base_dir = os.path.abspath(os.path.dirname(__file__))
    root = os.path.dirname(base_dir)
    uploads_dir = os.path.join(root, 'instance', 'uploads')
    os.makedirs(uploads_dir, exist_ok=True)
    tmp_src = os.path.join(uploads_dir, file.filename)
    file.save(tmp_src)

    # تحديد مسار قاعدة البيانات الفعّالة
    fallback_db = os.path.join(root, 'instance', 'database.db')
    db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
    db_path = _parse_sqlite_path(db_uri, fallback_db)

    try:
        # نسخ آمن عبر SQLite backup API
        _backup_sqlite(tmp_src, db_path)
        flash('تم استيراد النسخة الاحتياطية بنجاح.', 'success')
    except Exception as ex:
        flash(f'فشل استيراد النسخة الاحتياطية: {ex}', 'danger')
    finally:
        try:
            os.remove(tmp_src)
        except Exception:
            pass

    return redirect(url_for('settings_bp.index'))


@settings_bp.route('/settings/reminder_sound', methods=['POST'])
@login_required
def reminder_sound():
    if not _require_settings_permission():
        flash('ليس لديك صلاحية لتعديل الإعدادات.', 'danger')
        return redirect(url_for('settings_bp.index'))

    f = request.files.get('sound_file')
    if not f or not f.filename:
        flash('يرجى اختيار ملف صوتي (MP3/WAV).', 'warning')
        return redirect(url_for('settings_bp.index'))

    base_dir = os.path.abspath(os.path.dirname(__file__))
    root = os.path.dirname(base_dir)
    sounds_dir = os.path.join(root, 'instance', 'sounds')
    os.makedirs(sounds_dir, exist_ok=True)

    # اسم الملف الآمن: إزالة المسارات
    filename = os.path.basename(f.filename)
    dest_path = os.path.join(sounds_dir, filename)
    f.save(dest_path)

    s = load_settings(current_app)
    s['reminder_sound_filename'] = filename
    save_settings(current_app, s)
    flash('تم تحديث صوت التذكير بنجاح.', 'success')
    return redirect(url_for('settings_bp.index'))


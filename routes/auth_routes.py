# routes/auth_routes.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy.exc import OperationalError
from models import db, User
import re

auth_bp = Blueprint('auth_bp', __name__)

# ============ Helpers ============
def _is_safe_next(next_url: str) -> bool:
    # السماح بروابط داخلية فقط
    return next_url and not re.match(r'^[a-z]+://', next_url, re.I)

def _ensure_user_table_and_seed():
    """تكوين الجداول وإنشاء admin/admin لو غير موجود."""
    with current_app.app_context():
        db.create_all()
        admin = User.query.filter_by(username="admin").first()
        if not admin:
            admin = User(
                username="admin",
                role="admin",
                password_hash=generate_password_hash("admin")  # غيّرها بعد الدخول من زر "تغيير كلمتي"
            )
            db.session.add(admin)
            db.session.commit()
            current_app.logger.info("Seeded default admin (admin/admin).")

# ============ Views ============
@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    # لو مسجّل دخوله بالفعل
    if getattr(current_user, "is_authenticated", False):
        # إذا كان هناك next وتم طلب صفحة محمية، ارجع لها بدل لوحة التحكم
        next_url = request.args.get('next', '')
        if _is_safe_next(next_url):
            return redirect(next_url)
        return redirect(url_for('main_bp.dashboard'))

    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '')

        # 1) حاول الاستعلام
        try:
            user = User.query.filter_by(username=username).first()
        except OperationalError as ex:
            # 2) الجدول غير موجود -> أنشئه فورًا + Seed الأدمن -> أعد المحاولة
            current_app.logger.warning("User table missing; creating now. ex=%s", ex)
            _ensure_user_table_and_seed()
            user = User.query.filter_by(username=username).first()

        # 3) تحقق من البيانات
        if not user or not check_password_hash(user.password_hash, password):
            flash("بيانات الدخول غير صحيحة.", "danger")
            return render_template('login.html')

        # 4) دخول
        login_user(user, remember=False)

        next_url = request.args.get('next', '')
        if _is_safe_next(next_url):
            return redirect(next_url)
        return redirect(url_for('main_bp.dashboard'))

    # GET
    return render_template('login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth_bp.login'))

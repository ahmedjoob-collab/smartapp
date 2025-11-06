# routes/user_routes.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash
from urllib.parse import urlencode
from models import db, User
from utils.decorators import role_required

users_bp = Blueprint('users_bp', __name__)
ADMIN_USERNAME = "admin"

# ---------- Helpers ----------
def build_page_url(base_endpoint: str, page: int, extra_params: dict):
    params = {**extra_params, "page": page}
    return url_for(base_endpoint) + "?" + urlencode(params)

def _is_admin_user(user: User) -> bool:
    return bool(user and user.username == ADMIN_USERNAME)

def _get_bool(name: str) -> bool:
    # يقرأ checkbox من الـ form (on/True/1)
    v = (request.form.get(name) or "").strip().lower()
    return v in ("on", "true", "1", "yes")

# ---------- List + Search + Filter + Pagination (Admin only) ----------
@users_bp.route('/', methods=['GET'])
@login_required
@role_required('admin')
def users():
    q = (request.args.get('q') or '').strip()
    role = (request.args.get('role') or 'ALL').strip().lower()
    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        page = 1
    per_page = 25

    qry = User.query
    if q:
        like = f"%{q}%"
        qry = qry.filter(User.username.ilike(like))
    if role in ('user', 'admin'):
        qry = qry.filter(User.role == role)

    total = qry.count()
    items = (qry.order_by(User.username.asc())
                 .offset((page - 1) * per_page)
                 .limit(per_page)
                 .all())

    total_pages = (total + per_page - 1) // per_page if total else 1
    has_prev = page > 1
    has_next = page < total_pages

    base_params = {}
    if q:
        base_params["q"] = q
    if role and role != 'ALL':
        base_params["role"] = role

    # لو محتاج تعرض الصفحات في القالب — جاهزة
    page_urls = {
        "prev": build_page_url('users_bp.users', page - 1, base_params) if has_prev else None,
        "next": build_page_url('users_bp.users', page + 1, base_params) if has_next else None,
        "pages": [{"n": n,
                   "url": build_page_url('users_bp.users', n, base_params),
                   "active": (n == page)} for n in range(1, total_pages + 1)]
    }

    # NOTE: نرجّع items لأن list.html عندك بيستخدم items
    return render_template('users/list.html',
                           items=items, title='إدارة المستخدمين',
                           q=q, role=role, page=page, per_page=per_page,
                           total=total, total_pages=total_pages, page_urls=page_urls)

# ---------- Create (Admin only) ----------
@users_bp.route('/add', methods=['POST'])
@login_required
@role_required('admin')
def add():
    username = (request.form.get('username') or '').strip()
    password = (request.form.get('password') or '')
    role = (request.form.get('role') or 'user').strip() or 'user'
    can_trader = _get_bool('can_trader_services')
    can_support = _get_bool('can_support')
    # 🎉 قراءة الصلاحيات الجديدة
    can_settings = _get_bool('can_settings')
    can_general_reports = _get_bool('can_general_reports')
    can_inquiry = _get_bool('can_inquiry')
    # صلاحيات فرعية لخدمات التجار
    can_trader_frequent = _get_bool('can_trader_frequent')
    can_trader_primary = _get_bool('can_trader_primary')


    if not username or not password:
        flash('اسم المستخدم وكلمة المرور مطلوبان', 'warning')
        return redirect(url_for('users_bp.users'))

    user = User(username=username,
                password_hash=generate_password_hash(password),
                role=role,
                can_trader_services=can_trader,
                can_support=can_support,
                # 🎉 تعيين الصلاحيات الجديدة
                can_settings=can_settings,
                can_general_reports=can_general_reports,
                can_inquiry=can_inquiry,
                can_trader_frequent=can_trader_frequent,
                can_trader_primary=can_trader_primary)
    db.session.add(user)
    try:
        db.session.commit()
        flash('تم إنشاء المستخدم بنجاح', 'success')
    except IntegrityError:
        db.session.rollback()
        flash('اسم المستخدم موجود بالفعل', 'danger')
    return redirect(url_for('users_bp.users'))

# إبقاء اسم الدالة القديمة add_user (توافق رجعي إن كان مستدعى من مكان آخر)
@users_bp.route('/add_user', methods=['POST'])
@login_required
@role_required('admin')
def add_user():
    return add()

# ---------- Update (Admin only) ----------
@users_bp.route('/<int:user_id>/edit', methods=['POST'])
@login_required
@role_required('admin')
def edit_user(user_id):
    user = User.query.get_or_404(user_id)

    # حماية حساب الأدمن من أي تعديل (اسم/دور/تعطيل/صلاحيات)
    if _is_admin_user(user):
        flash('لا يمكن تعديل حساب الأدمن.', 'warning')
        return redirect(url_for('users_bp.users'))

    username = (request.form.get('username') or '').strip()
    role = (request.form.get('role') or 'user').strip() or 'user'
    can_trader = _get_bool('can_trader_services')
    can_support = _get_bool('can_support')
    # 🎉 قراءة الصلاحيات الجديدة
    can_settings = _get_bool('can_settings')
    can_general_reports = _get_bool('can_general_reports')
    can_inquiry = _get_bool('can_inquiry')
    # صلاحيات فرعية لخدمات التجار
    can_trader_frequent = _get_bool('can_trader_frequent')
    can_trader_primary = _get_bool('can_trader_primary')

    if not username:
        flash('اسم المستخدم مطلوب', 'warning')
        return redirect(url_for('users_bp.users'))

    user.username = username
    user.role = role
    user.can_trader_services = can_trader
    user.can_support = can_support
    # 🎉 تعيين الصلاحيات الجديدة
    user.can_settings = can_settings
    user.can_general_reports = can_general_reports
    user.can_inquiry = can_inquiry
    user.can_trader_frequent = can_trader_frequent
    user.can_trader_primary = can_trader_primary
    
    try:
        db.session.commit()
        flash('تم تحديث بيانات المستخدم', 'success')
    except IntegrityError:
        db.session.rollback()
        flash('اسم المستخدم موجود بالفعل', 'danger')
    return redirect(url_for('users_bp.users'))

# ---------- Toggle Suspend (Admin only) ----------
@users_bp.route('/<int:user_id>/toggle_suspend', methods=['POST'])
@login_required
@role_required('admin')
def toggle_suspend(user_id):
    user = User.query.get_or_404(user_id)

    # لا يمكن إيقاف الأدمن
    if _is_admin_user(user):
        flash('لا يمكن إيقاف حساب الأدمن.', 'warning')
        return redirect(url_for('users_bp.users'))

    user.suspended = not bool(user.suspended)
    db.session.commit()
    flash('تم تحديث حالة المستخدم (إيقاف/تشغيل).', 'success')
    return redirect(url_for('users_bp.users'))

# ---------- Update Password (Admin only for others) ----------
@users_bp.route('/<int:user_id>/update_password', methods=['POST'])
@login_required
@role_required('admin')
def update_password(user_id):
    user = User.query.get_or_404(user_id)
    new_pass = (request.form.get('new_password') or '').strip()
    if len(new_pass) < 6:
        flash('كلمة المرور يجب ألا تقل عن 6 أحرف', 'warning')
        return redirect(url_for('users_bp.users'))

    user.password_hash = generate_password_hash(new_pass)
    db.session.commit()
    flash('تم تحديث كلمة المرور', 'success')
    return redirect(url_for('users_bp.users'))

# alias متوافق مع القوالب القديمة: users_bp.change_password
@users_bp.route('/<int:user_id>/change_password', methods=['POST'])
@login_required
@role_required('admin')
def change_password(user_id):
    return update_password(user_id)

# ---------- Delete (Admin only) ----------
@users_bp.route('/<int:user_id>/delete', methods=['POST'])
@login_required
@role_required('admin')
def delete_user(user_id):
    user = User.query.get_or_404(user_id)

    # حماية حساب الأدمن من الحذف
    if _is_admin_user(user):
        flash('لا يمكن حذف حساب الأدمن.', 'danger')
        return redirect(url_for('users_bp.users'))

    # لا تحذف نفسك حرصًا
    if user.id == getattr(current_user, 'id', None):
        flash('لا يمكنك حذف نفسك.', 'warning')
        return redirect(url_for('users_bp.users'))

    db.session.delete(user)
    db.session.commit()
    flash('تم حذف المستخدم', 'info')
    return redirect(url_for('users_bp.users'))

# ---------- أي مستخدم يغيّر كلمة مروره (Self-service) ----------
@users_bp.route('/me/password', methods=['POST'])
@login_required
def me_password():
    new_pass = (request.form.get('new_password') or '').strip()
    # يُفترض أن حقل تأكيد كلمة المرور في القالب اسمه 'confirm_password'
    confirm = (request.form.get('confirm_password') or '').strip() 

    if not new_pass or len(new_pass) < 6:
        flash('كلمة المرور يجب ألا تقل عن 6 أحرف.', 'warning')
        return redirect(url_for('main_bp.dashboard'))
    
    if new_pass != confirm:
        flash('تأكيد كلمة المرور غير مطابق.', 'warning')
        return redirect(url_for('main_bp.dashboard'))

    try:
        if not current_user.is_authenticated:
             # هذه الحالة يفترض ألا تحدث بسبب @login_required، لكن للإحتياط
             flash('يجب تسجيل الدخول لتغيير كلمة المرور.', 'danger')
             return redirect(url_for('auth_bp.login'))

        # تحديث كلمة المرور للمستخدم الحالي مباشرة
        current_user.password_hash = generate_password_hash(new_pass)
        db.session.commit()
        flash('✅ تم تغيير كلمة مرورك بنجاح.', 'success')
    except Exception as e:
        flash('⚠️ حدث خطأ أثناء تحديث كلمة المرور. الرجاء المحاولة لاحقاً.', 'danger')
        # طباعة الخطأ في الـ console للمطورين
        print(f"Error updating self-service user password for user {current_user.id}: {e}") 
        db.session.rollback()
        
    return redirect(url_for('main_bp.dashboard'))

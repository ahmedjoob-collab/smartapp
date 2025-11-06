from utils.decorators import admin_required
# -*- coding: utf-8 -*-
# Route file: user_management.py

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required

user_management_bp = Blueprint('user_management', __name__)

# --- Content from: add_user_route.py ---
from flask import Blueprint, render_template, request, redirect, flash, session
from models import db, User
from werkzeug.security import generate_password_hash
from functools import wraps

# دالة مساعدة للتأكد من أن المستخدم مدير
users_add_bp = Blueprint('users_add_bp', __name__)

@users_add_bp.route('/users/add', methods=['GET', 'POST'])
@admin_required # ✅ حماية المسار
def add_user():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        role = request.form.get('role')
        sections = request.form.getlist('sections') # قائمة بصلاحيات الأقسام المحددة

        if not username or not password or not role:
            flash("❌ جميع الحقول المطلوبة (اسم المستخدم، كلمة المرور، الدور) مطلوبة", "danger")
            return render_template("add_user_with_sections.html"), 400

        if User.query.filter_by(username=username).first():
            flash("⚠️ اسم المستخدم مستخدم من قبل، يرجى اختيار اسم آخر.", "warning")
            return render_template("add_user_with_sections.html")

        hashed_password = generate_password_hash(password)
        # تحويل قائمة الصلاحيات إلى نص مفصول بفاصلة
        allowed_sections = ','.join(sections) 

        new_user = User(username=username, password_hash=hashed_password, role=role, allowed_sections=allowed_sections)
        db.session.add(new_user)
        db.session.commit()

        flash("✅ تم إضافة المستخدم بنجاح", "success")
        return redirect("/users")

    return render_template("add_user_with_sections.html")
# --- Content from: edit_user_route.py ---
from flask import Blueprint, render_template, request, redirect, flash, session
from models import db, User
from werkzeug.security import generate_password_hash
from functools import wraps

# دالة مساعدة للتأكد من أن المستخدم مدير
users_bp = Blueprint('users_bp', __name__)

@users_bp.route('/users/edit/<int:user_id>', methods=['GET', 'POST'])
@admin_required # ✅ حماية المسار
def edit_user(user_id):
    user = User.query.get_or_404(user_id) 

    if request.method == 'POST':
        password = request.form.get('password')
        role = request.form.get('role')
        sections = request.form.getlist('sections') 

        # منع تغيير دور 'admin'
        if user.username == 'admin' and role != 'admin':
            flash("⚠️ لا يمكن تغيير دور المستخدم الرئيسي 'admin'.", "warning")
            allowed_sections_list = user.allowed_sections.split(',') if user.allowed_sections else []
            return render_template("edit_user_with_sections.html", user=user, allowed_sections=allowed_sections_list)


        if password:
            user.password_hash = generate_password_hash(password)

        user.role = role
        user.allowed_sections = ','.join(sections)

        db.session.commit()
        flash("✅ تم تعديل المستخدم بنجاح", "success")
        return redirect("/users")

    # عند التحميل (GET)
    allowed_sections_list = user.allowed_sections.split(',') if user.allowed_sections else []

    return render_template("edit_user_with_sections.html", user=user, allowed_sections=allowed_sections_list)

# ✅ مسار حذف المستخدم
@users_bp.route('/users/delete/<int:user_id>')
@admin_required
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    
    # منع حذف المستخدم 'admin'
    if user.username == 'admin':
        flash("❌ لا يمكن حذف المستخدم الرئيسي 'admin'.", "danger")
        return redirect("/users")
        
    db.session.delete(user)
    db.session.commit()
    flash(f"✅ تم حذف المستخدم {user.username} بنجاح.", "success")
    return redirect("/users")

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required
from werkzeug.security import generate_password_hash
from models import db, User

@user_management_bp.route('/manage_users')
@login_required
@admin_required
def manage_users():
    users = User.query.all()
    return render_template('manage_users.html', users=users)

@user_management_bp.route('/add_user', methods=['POST'])
@login_required
@admin_required
def add_user():
    username = request.form.get('username')
    password = request.form.get('password')
    role = request.form.get('role')

    if User.query.filter_by(username=username).first():
        flash('اسم المستخدم موجود بالفعل', 'danger')
        return redirect(url_for('user_management.manage_users'))

    new_user = User(username=username, password=generate_password_hash(password), role=role)
    db.session.add(new_user)
    db.session.commit()
    flash('تمت إضافة المستخدم بنجاح', 'success')
    return redirect(url_for('user_management.manage_users'))

@user_management_bp.route('/update_role/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def update_role(user_id):
    role = request.form.get('role')
    user = User.query.get(user_id)
    if user:
        user.role = role
        db.session.commit()
        flash('تم تحديث الدور بنجاح', 'success')
    return redirect(url_for('user_management.manage_users'))

@user_management_bp.route('/delete_user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    user = User.query.get(user_id)
    if user:
        db.session.delete(user)
        db.session.commit()
        flash('تم حذف المستخدم بنجاح', 'warning')
    return redirect(url_for('user_management.manage_users'))


@user_management_bp.route('/update_password/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def update_password(user_id):
    user = User.query.get(user_id)
    new_password = request.form.get('new_password')
    if user and new_password:
        user.password = generate_password_hash(new_password)
        db.session.commit()
        flash('تم تحديث كلمة المرور بنجاح', 'info')
    return redirect(url_for('user_management.manage_users'))
@user_management_bp.route('/users')
@login_required
def list_users():
    users = User.query.all()
    return render_template('users_list.html', users=users)
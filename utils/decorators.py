from functools import wraps
from flask import redirect, url_for, render_template, abort
from flask_login import current_user

# 1. admin_required: يفرض أن يكون الدور 'admin'
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'admin':
            return render_template('403.html'), 403
        return f(*args, **kwargs)
    return decorated_function

# 2. role_required: يفرض وجود دور معين في قائمة الأدوار المسموحة
def role_required(roles):
    """يفرض وجود دور معين في قائمة الأدوار المسموحة (Admin يمر دائماً)."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return abort(401)
            
            # السماح للأدمن بالمرور دائماً
            if current_user.role == 'admin':
                return f(*args, **kwargs)

            # التحقق من الدور المطلوب
            if current_user.role not in roles:
                return render_template('403.html'), 403
                
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# 3. permission_required: يفرض وجود صلاحية محددة (مثل can_inquiry)
def permission_required(permission_name):
    """
    يفرض وجود صلاحية معينة (مثل 'can_inquiry') للمستخدم. الأدمن يمر دائماً.
    يستخدم الحقول البوليانية من نموذج المستخدم (models.py).
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return abort(401)
            
            # 1. السماح للأدمن بالمرور دائماً
            if current_user.role == 'admin':
                return f(*args, **kwargs)

            # 2. التحقق من صلاحية المستخدم
            # نستخدم getattr() للحصول على قيمة العمود (مثل current_user.can_inquiry)
            if not getattr(current_user, permission_name, False):
                return render_template('403.html'), 403
                
            return f(*args, **kwargs)
        return decorated_function
    return decorator
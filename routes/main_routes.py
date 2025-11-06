# routes/main_routes.py
from flask import Blueprint, render_template, url_for
from flask_login import login_required, current_user
from werkzeug.routing import BuildError

main_bp = Blueprint('main_bp', __name__)

def _safe_url(endpoint: str, **kwargs) -> str:
    """يرجع رابط المسار لو موجود، وإلا يعيد '#' بدون ما يكسر الصفحة."""
    try:
        return url_for(endpoint, **kwargs)
    except BuildError:
        return "#"

@main_bp.route('/dashboard')
@login_required
def dashboard():
    """
    لوحة التحكم:
    - الأدمن: كل الأقسام نشِطة.
    - المستخدم العادي:
        * لو عنده can_support: يفعّل "الدعم الفني" + "التقارير العامة".
        * لو عنده can_trader_services: يفعّل "خدمات التجار" + "التقارير العامة".
        * لو عنده الاثنين: التلاتة مفعّلين.
        * بدون صلاحيات: "التقارير العامة" فقط مفعّلة.
      "إدارة المستخدمين" للأدمن فقط، والباقي يظهر باللون الرمادي ومعطّل.
    """
    is_admin = getattr(current_user, "role", None) == "admin"
    can_support = bool(getattr(current_user, "can_support", False))
    can_trader_section  = bool(getattr(current_user, "can_trader_services", False))
    # صلاحيات فرعية لخدمات التجار
    can_trader_frequent = bool(getattr(current_user, "can_trader_frequent", False))
    can_trader_primary  = bool(getattr(current_user, "can_trader_primary", False))
    # التقارير العامة حسب الصلاحية
    enable_reports = bool(getattr(current_user, "can_general_reports", False)) or is_admin
    # الاستعلام حسب الصلاحية
    enable_inquiry = bool(getattr(current_user, "can_inquiry", False)) or is_admin

    sections = []

    # ===== 💡 إضافة قسم الاستعلام الجديد =====
    sections.append({
        "title": "الاستعلام",
        "description": "بحث سريع في سجلات المخابز، التموين، والاستبدال.",
        "icon": "fas fa-search",
        "badge": "info",
        "url": _safe_url('machine_reports_bp.inquiry_view') if enable_inquiry else None,
        "disabled": False if enable_inquiry else True
    })
    # ====================================

    # الدعم الفني (بديل إدارة العملاء)
    sections.append({
        "title": "الدعم الفني",
        "description": "سجلات الدعم الفني: عرض وبحث وتصدير (والإضافة/التعديل/الحذف للأدمن).",
        "icon": "fas fa-headset",
        "badge": "info",
        "url": _safe_url('support_bp.index') if (is_admin or can_support) else None,
        "disabled": False if (is_admin or can_support) else True
    })

    # الخدمات التجارية → صفحة القسم (قائمة بالشاشتين)
    allow_trader = is_admin or can_trader_section or can_trader_frequent or can_trader_primary
    sections.append({
        "title": "خدمات التجار",
        "description": "المترددين والماكينات الأساسية للعملاء وماكينات الفرع.",
        "icon": "fas fa-briefcase",
        "badge": "success",
        "url": _safe_url('trader_services_bp.index') if allow_trader else None,
        "disabled": False if allow_trader else True
    })

    # التقارير العامة
    sections.append({
        "title": "التقارير العامة",
        "description": "استيراد (أدمن) ثم بحث وتصدير للجميع.",
        "icon": "fas fa-file-excel",
        "badge": "warning",
        "url": _safe_url('machine_reports_bp.index') if enable_reports else None,
        "disabled": False if enable_reports else True
    })

    # إدارة المستخدمين
    if is_admin:
        sections.append({
            "title": "إدارة المستخدمين",
            "description": "إضافة وتعديل وحذف المستخدمين وأدوارهم.",
            "icon": "fas fa-user-shield",
            "badge": "danger",
            "url": _safe_url('users_bp.users'),
            "disabled": False
        })
    else:
        sections.append({
            "title": "إدارة المستخدمين",
            "description": "يتطلب صلاحيات مدير.",
            "icon": "fas fa-user-shield",
            "badge": "secondary",
            "url": None,          # يظهر كمعطّل
            "disabled": True
        })

    # الإعدادات العامة
    can_settings = bool(getattr(current_user, "can_settings", False)) or is_admin
    sections.append({
        "title": "الإعدادات العامة",
        "description": "تغيير صوت التذكير، مسار النسخة الاحتياطية، الاستيراد/التصدير.",
        "icon": "fas fa-cogs",
        "badge": "primary",
        "url": _safe_url('settings_bp.index') if can_settings else None,
        "disabled": False if can_settings else True
    })

    # تحضير رابط الاستعلام بشكل آمن لتجنب BuildError في القالب
    inquiry_url = _safe_url('machine_reports_bp.inquiry_view') if enable_inquiry else None

    return render_template('dashboard.html',
                           title='لوحة التحكم',
                           sections=sections,
                           inquiry_url=inquiry_url)


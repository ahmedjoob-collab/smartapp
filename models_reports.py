# models_reports.py
from datetime import datetime
from models import db
from sqlalchemy import UniqueConstraint # 💡 يجب استيرادها لقيد التفرد

class ReportState(db.Model):
    __tablename__ = "report_state"

    id = db.Column(db.Integer, primary_key=True)
    # الأقسام: bakeries | ration | substitute
    category = db.Column(db.String(50), nullable=False, index=True)

    # 💡 العمود الجديد: لربط حالة التقرير بالمستخدم
    # بما أنك قمت بإضافته في SQLite يدوياً، لن تحتاج لترحيل جديد.
    user_id = db.Column(db.Integer, nullable=False, index=True) 

    # البيانات مخزنة كـ JSON (orient="records")
    data_json = db.Column(db.Text, nullable=True)

    # إعدادات المابنج: {"rename": {"old": "new"}, "order": ["colA", "colB", ...]}
    mapping_json = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # 💡 قيد التفرد: يضمن أن يكون لكل مستخدم حالة واحدة فقط لكل قسم (category)
    # هذا يحل مشكلة التكرار عند الحفظ.
    __table_args__ = (
        UniqueConstraint('category', 'user_id', name='_category_user_uc'),
    )

    def __repr__(self):
        return f'<ReportState {self.category}/{self.user_id}>'


# نموذج جديد: تذاكر الخدمات المرتبطة بالمسلسلات وتفاصيل الماكينات
class ServiceTicket(db.Model):
    __tablename__ = "service_tickets"

    id = db.Column(db.Integer, primary_key=True)

    # التاريخ يُسجل تلقائياً (غير قابل للتعديل من الواجهة)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    # تبويب الشاشة (type) — مفتاح وفئة عربية
    category_key = db.Column(db.String(20), nullable=False)  # bakeries | ration | substitute
    category_label = db.Column(db.String(50), nullable=False)  # مخبز | تموين | الاستبدال

    # نوع العطل (من القائمة المنسدلة)
    fault_type = db.Column(db.String(50), nullable=False)

    # رقم الإذن — أرقام فقط، يُسمح بتكراره وفق سياسة التطبيق
    order_number = db.Column(db.String(50), nullable=False)

    # المستخدم المسجّل حالياً
    username = db.Column(db.String(150), nullable=False)

    # بيانات مرتبطة بشاشة الاستعلام وعناوين المترددين
    customer_code = db.Column(db.String(100))   # رقم العميل
    customer_name = db.Column(db.String(255))   # اسم العميل
    machine_code  = db.Column(db.String(100))   # رقم الماكينة
    machine_serial = db.Column(db.String(100))  # مسلسل
    main_sub      = db.Column(db.String(50))    # رئيسية/فرعية
    status        = db.Column(db.String(100))   # حالة الماكينة
    sim1          = db.Column(db.String(50))    # شريحة1
    sim2          = db.Column(db.String(50))    # شريحة2

    # عامود خدمات/صيانه — وفق المتطلبات
    services      = db.Column(db.String(100))   # نخزن نوع العطل هنا
    maintenance   = db.Column(db.String(100), default="")  # يظل فارغًا

    def __repr__(self):
        return f"<ServiceTicket id={self.id} order={self.order_number} serial={self.machine_serial}>"
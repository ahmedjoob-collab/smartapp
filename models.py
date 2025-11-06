from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

# ===== المستخدمون =====
class User(UserMixin, db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')  # 'admin' | 'user'

    # صلاحيات تفصيلية (الخمسة حقول المطلوبة)
    can_trader_services = db.Column(db.Boolean, default=False, nullable=False)  # خدمات التجار
    can_support           = db.Column(db.Boolean, default=False, nullable=False)  # الدعم الفني
    can_settings          = db.Column(db.Boolean, default=False, nullable=False)  # الإعدادات العامة
    can_general_reports   = db.Column(db.Boolean, default=False, nullable=False)  # التقارير العامة
    can_inquiry           = db.Column(db.Boolean, default=False, nullable=False)  # الاستعلام
    # صلاحيات فرعية داخل خدمات التجار
    can_trader_frequent   = db.Column(db.Boolean, default=False, nullable=False)  # شاشة المترددين
    can_trader_primary    = db.Column(db.Boolean, default=False, nullable=False)  # شاشة الماكينات الأساسية/الفرع

    # إيقاف مؤقت للحساب
    suspended = db.Column(db.Boolean, default=False, nullable=False)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    @property
    def is_active(self):
        # يضمن أن المستخدم المعلق لا يمكنه تسجيل الدخول
        return not self.suspended

    # دالة تعيين كلمة المرور (جديدة/مضافة)
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    # دالة فحص كلمة المرور (جديدة/مضافة)
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User id={self.id} username={self.username} role={self.role} suspended={self.suspended}>"

# (اترك بقية موديلاتك كما هي)
class Client(db.Model):
    __tablename__ = 'clients'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20))
    service = db.Column(db.String(100))
    join_date = db.Column(db.String(20))

class FrequentVisitor(db.Model):
    __tablename__ = 'frequent_visitors'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    visit_count = db.Column(db.Integer)
    data = db.Column(db.String(255))

class BasicCustomer(db.Model):
    __tablename__ = 'basic_customers'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    data = db.Column(db.String(255))

class MachineReport(db.Model):
    __tablename__ = 'machine_reports'
    id = db.Column(db.Integer, primary_key=True)
    report_data = db.Column(db.String(255))
    timestamp = db.Column(db.DateTime)

# ===== الدعم الفني =====
class SupportCase(db.Model):
    __tablename__ = "support_case"

    id = db.Column(db.Integer, primary_key=True)

    # أساسية
    name = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(50), nullable=True, index=True)      # اختياري (نص)
    work_type = db.Column(db.String(50), nullable=False)           # "أعمال دعم عامة" | "حسابات بنكية"
    work_detail = db.Column(db.Text, default="")
    sender_email_name = db.Column(db.String(255), default="")
    notes = db.Column(db.Text, default="")

    # تذكير (يُحفظ كنص UTC "YYYY-MM-DD HH:MM")
    reminder_message = db.Column(db.Text, default="")
    reminder_at = db.Column(db.String(32), default="")              # UTC string
    next_fire_at = db.Column(db.String(32), default="")             # UTC string (لـ snooze)
    dismissed = db.Column(db.Boolean, default=False, nullable=False)

    # ملكية السجل
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    creator = db.relationship("User", backref="support_cases")

    @property
    def owner(self):
        return self.creator

    # حقول الحساب البنكي
    bank_request_number = db.Column(db.String(100), default="")
    bank_bakery_code    = db.Column(db.String(100), default="")
    bank_id             = db.Column(db.String(100), default="")
    bank_acc_number     = db.Column(db.String(100), default="")      # يبدأ بـ EG
    bank_acc_name       = db.Column(db.String(255), default="")
    bank_national_id    = db.Column(db.String(100), default="")

    # تتبع
    # 💥💥 هذا العمود هو "تاريخ التسجيل" المطلوب 💥💥
    # يقوم بتسجيل التاريخ والوقت الحالي تلقائياً عند إضافة السجل
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<SupportCase id={self.id} name={self.name} code={self.code}>"

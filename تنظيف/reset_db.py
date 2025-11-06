from app import app, db
from models import User
from werkzeug.security import generate_password_hash

with app.app_context():
    # إنشاء جميع الجداول
    db.create_all()
    print("✅ تم إنشاء الجداول بنجاح.")

    # إنشاء مستخدم admin افتراضي
    admin = User(
        username='admin',
        password_hash=generate_password_hash('admin123'),
        role='admin',
        allowed_sections='clients,support,settings,trader_services,machine_reports,user_management'
    )

    db.session.add(admin)
    db.session.commit()
    print("✅ تم إنشاء مستخدم admin (كلمة المرور: admin123).")

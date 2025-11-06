# seed_admin.py
from werkzeug.security import generate_password_hash
from app import create_app
from models import db, User

"""
يشغِّل create_all() لإنشاء الجداول (لو مش موجودة)
ويضيف مستخدم أدمن افتراضي لو مش موجود.
"""

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin123"  # غيّرها بعد أول تسجيل دخول

app = create_app()

with app.app_context():
    # إنشاء الجداول
    db.create_all()

    # التحقق من وجود الأدمن
    user = User.query.filter_by(username=ADMIN_USERNAME).first()
    if not user:
        user = User(
            username=ADMIN_USERNAME,
            password_hash=generate_password_hash(ADMIN_PASSWORD),
            role="admin"
        )
        db.session.add(user)
        db.session.commit()
        print(f"[OK] تم إنشاء الأدمن: {ADMIN_USERNAME} / {ADMIN_PASSWORD}")
    else:
        print("[OK] الأدمن موجود بالفعل")

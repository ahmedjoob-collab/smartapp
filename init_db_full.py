
from flask import Flask
from models import db
from sqlalchemy import inspect, text

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'super-secret-key'

db.init_app(app)

with app.app_context():
    # إنشاء الجداول إن لم تكن موجودة
    db.create_all()

    # فحص الأعمدة الحالية في جدول users
    inspector = inspect(db.engine)
    columns = [col['name'] for col in inspector.get_columns('users')]

    if 'allowed_sections' not in columns:
        try:
            with db.engine.connect() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN allowed_sections VARCHAR;"))
            print("✅ تم إضافة العمود allowed_sections بنجاح.")
        except Exception as e:
            print(f"❌ خطأ أثناء إضافة العمود allowed_sections: {e}")
    else:
        print("ℹ️ العمود allowed_sections موجود مسبقًا.")

    # بعد التأكد من أن العمود موجود، استورد User
    from models import User

    # إنشاء مستخدم الأدمن إذا لم يكن موجودًا
    if not User.query.filter_by(username='admin').first():
        admin = User(username='admin', password='admin123', role='admin', allowed_sections='tech,trader,settings')
        db.session.add(admin)
        db.session.commit()
        print("✅ تم إنشاء مستخدم الأدمن: admin / admin123")
    else:
        print("ℹ️ مستخدم الأدمن موجود بالفعل.")

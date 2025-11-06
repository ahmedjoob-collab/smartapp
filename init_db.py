# init_db.py
import os
from flask import Flask
from models import db, User
from werkzeug.security import generate_password_hash 

# تحديد المسار المطلق للمشروع (هام لضمان عمل قاعدة البيانات في جميع الظروف)
basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
# 🟢 الإصلاح 1: توحيد مسار قاعدة البيانات ليكون مطلقًا
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'instance', 'database.db') 
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

with app.app_context():
    db.create_all()

    # 🔑 الإصلاح 2 (الحرج - حل مشكلة تسجيل الدخول): تشفير كلمة مرور الأدمن
    if not User.query.filter_by(username='admin').first():
        admin = User(
            username='admin', 
            # يجب استخدام password_hash كاسم عمود وتخزين القيمة المشفرة
            password_hash=generate_password_hash('admin123'), 
            role='admin'
        )
        db.session.add(admin)
        db.session.commit()
        print("✅ تم إنشاء مستخدم الأدمن (مشفر): admin / admin123")
    else:
        print("ℹ️ مستخدم الأدمن موجود بالفعل.")
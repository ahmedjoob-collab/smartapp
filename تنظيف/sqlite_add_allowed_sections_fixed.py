
from sqlalchemy import create_engine, text

# غيّر هذا المسار إذا كانت قاعدة بياناتك باسم مختلف
engine = create_engine('sqlite:///database.db')

with engine.connect() as conn:
    try:
        conn.execute(text("ALTER TABLE users ADD COLUMN allowed_sections VARCHAR;"))
        print("✅ تم إضافة العمود allowed_sections بنجاح.")
    except Exception as e:
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            print("ℹ️ العمود allowed_sections موجود مسبقًا.")
        else:
            print("❌ خطأ أثناء تعديل الجدول:", e)

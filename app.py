# app.py
import os
from flask import Flask, render_template, redirect, url_for, request, send_from_directory
from flask_login import LoginManager, current_user
from werkzeug.security import generate_password_hash
from models import db, User  # models.py يُعرّف db = SQLAlchemy() و User
from sqlalchemy import text # تم استيرادها هنا لتجنب مشاكل النطاق

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # ===== إعدادات أساسية =====
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
    
    # ===== إعداد قاعدة البيانات الذكي =====
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    else:
        data_dir = "/tmp"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, "database.db")
        app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"

    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # ===== تهيئة قاعدة البيانات =====
    db.init_app(app)

    # ===== إعداد تسجيل الدخول =====
    login_manager = LoginManager()
    login_manager.login_view = "auth_bp.login"
    login_manager.login_message_category = "warning"
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id: str):
        # يدعم البحث بالرقم والنص
        try:
            u = User.query.get(user_id)
            if u:
                return u
        except Exception:
            pass
        try:
            return User.query.get(int(user_id))
        except Exception:
            return None

    # ===== استيراد وتسجيل الـ Blueprints بعد إنشاء app =====
    from routes.auth_routes import auth_bp
    from routes.main_routes import main_bp
    # الإعدادات العامة
    try:
        from routes.settings import settings_bp as _settings_bp
    except Exception:
        _settings_bp = None

    # دعم العملاء (إن وُجد)
    clients_bp = None
    for mod, attr in [
        ("routes.client_routes", "clients_bp"),
        ("routes.clients_routes", "clients_bp"),
        ("routes.clients_routes", "client_bp"),
    ]:
        try:
            m = __import__(mod, fromlist=[attr])
            clients_bp = getattr(m, attr)
            break
        except Exception:
            continue

    # إدارة المستخدمين (users_routes أو user_routes)
    users_bp = None
    for mod in ["routes.users_routes", "routes.user_routes"]:
        try:
            m = __import__(mod, fromlist=["users_bp"])
            users_bp = getattr(m, "users_bp")
            break
        except Exception:
            continue

    # التقارير العامة (اختياري)
    machine_reports_bp = None
    try:
        from routes.machine_reports import machine_reports_bp as _mr
        machine_reports_bp = _mr
    except Exception as ex:
        # 💥💥 التصحيح: إذا كان machine_reports_bp هو سبب التعطيل، يجب أن نتمكن من المتابعة
        print(f"Failed to import machine_reports_bp: {ex}") 
        machine_reports_bp = None

    # خدمات التجار (اختياري)
    trader_services_bp = None
    try:
        from routes.trader_services import trader_services_bp as _ts
        trader_services_bp = _ts
    except Exception:
        trader_services_bp = None

    # الدعم الفني (القسم الجديد) — مهم
    support_bp = None
    try:
        from routes.support import support_bp as _sbp
        support_bp = _sbp
    except Exception as ex:
        pass


    # تسجيل جميع البلوبرنتس
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    if clients_bp:
        app.register_blueprint(clients_bp, url_prefix="/clients")
    if users_bp:
        app.register_blueprint(users_bp, url_prefix="/users")
    if machine_reports_bp:
        # 🛠️ الخطوة الأساسية لحل BuildError: تسجيل Blueprint مع بادئة المسار
        app.register_blueprint(machine_reports_bp, url_prefix="/reports") 
    if trader_services_bp:
        app.register_blueprint(trader_services_bp, url_prefix="/trader")
    if support_bp:
        app.register_blueprint(support_bp)  # له url_prefix داخل الملف نفسه (/support)
    if _settings_bp:
        app.register_blueprint(_settings_bp)


    # ===== إنشاء الجداول + إنشاء admin/admin إن لم يوجد + ترقيع أعمدة support_case و user =====
    with app.app_context():
        # إنشاء الجداول
        db.create_all()

        # --- ترقيع قيود التفرد على رقم الإذن في جدول service_tickets ---
        # السماح بتكرار رقم الإذن لنفس العميل عبر عدة صفوف
        try:
            def _drop_unique_index_on(table: str, column: str):
                idx_rows = db.session.execute(text(f"PRAGMA index_list({table});")).fetchall()
                # صفوف PRAGMA index_list: (seq, name, unique, origin, partial)
                for r in idx_rows:
                    idx_name = r[1]
                    is_unique = bool(r[2])
                    if not is_unique:
                        continue
                    cols = db.session.execute(text(f"PRAGMA index_info({idx_name});")).fetchall()
                    # صفوف PRAGMA index_info: (seqno, cid, name)
                    idx_cols = [c[2] for c in cols]
                    if column in idx_cols:
                        try:
                            db.session.execute(text(f"DROP INDEX IF EXISTS {idx_name};"))
                        except Exception:
                            pass
                db.session.commit()

            _drop_unique_index_on('service_tickets', 'order_number')

            # إذا كان المفتاح الفريد مضمّنًا كفهرس داخلي لا يمكن إسقاطه (sqlite_autoindex)،
            # نعيد بناء الجدول لإزالة قيد UNIQUE من التعريف.
            def _has_unique_on_column(table: str, column: str) -> bool:
                try:
                    rows = db.session.execute(text(f"PRAGMA index_list({table});")).fetchall()
                    for r in rows:
                        idx_name = r[1]
                        is_unique = bool(r[2])
                        if not is_unique:
                            continue
                        cols = db.session.execute(text(f"PRAGMA index_info({idx_name});")).fetchall()
                        idx_cols = [c[2] for c in cols]
                        if column in idx_cols:
                            return True
                except Exception:
                    pass
                return False

            if _has_unique_on_column('service_tickets', 'order_number'):
                try:
                    # إنشاء جدول جديد بدون UNIQUE
                    db.session.execute(text(
                        """
                        CREATE TABLE IF NOT EXISTS service_tickets_new (
                            id INTEGER PRIMARY KEY,
                            created_at DATETIME NOT NULL,
                            category_key VARCHAR(20) NOT NULL,
                            category_label VARCHAR(50) NOT NULL,
                            fault_type VARCHAR(50) NOT NULL,
                            order_number VARCHAR(50) NOT NULL,
                            username VARCHAR(150) NOT NULL,
                            customer_code VARCHAR(100),
                            customer_name VARCHAR(255),
                            machine_code VARCHAR(100),
                            machine_serial VARCHAR(100),
                            main_sub VARCHAR(50),
                            status VARCHAR(100),
                            sim1 VARCHAR(50),
                            sim2 VARCHAR(50),
                            services VARCHAR(100),
                            maintenance VARCHAR(100) DEFAULT ''
                        );
                        """
                    ))
                    # نسخ البيانات من الجدول القديم
                    db.session.execute(text(
                        """
                        INSERT INTO service_tickets_new (
                            id, created_at, category_key, category_label, fault_type, order_number, username,
                            customer_code, customer_name, machine_code, machine_serial, main_sub, status, sim1, sim2,
                            services, maintenance
                        )
                        SELECT id, created_at, category_key, category_label, fault_type, order_number, username,
                               customer_code, customer_name, machine_code, machine_serial, main_sub, status, sim1, sim2,
                               services, maintenance
                        FROM service_tickets;
                        """
                    ))
                    # حذف الجدول القديم وإعادة تسمية الجديد
                    db.session.execute(text("DROP TABLE service_tickets;"))
                    db.session.execute(text("ALTER TABLE service_tickets_new RENAME TO service_tickets;"))
                    db.session.commit()
                except Exception as e2:
                    print(f"Table rebuild error (service_tickets): {e2}")
                    db.session.rollback()
        except Exception as e:
            print(f"Patch unique index error: {e}")
            db.session.rollback()

        # --- ترقيع أعمدة support_case و user المفقودة (SQLite) ---

        def table_has_column(table_name: str, col_name: str) -> bool:
            try:
                rows = db.session.execute(text(f"PRAGMA table_info({table_name});")).fetchall()
                # صفوف PRAGMA: (cid, name, type, notnull, dflt_value, pk)
                return any(r[1] == col_name for r in rows)
            except Exception:
                return False

        def add_column_if_missing(table: str, col: str, ddl_sql: str):
            if not table_has_column(table, col):
                db.session.execute(text(ddl_sql))

        # حقول بنكية (support_case)
        if table_has_column("support_case", "id"):
            add_column_if_missing("support_case", "bank_request_number",
                                 "ALTER TABLE support_case ADD COLUMN bank_request_number VARCHAR(100) DEFAULT ''")
            add_column_if_missing("support_case", "bank_bakery_code",
                                 "ALTER TABLE support_case ADD COLUMN bank_bakery_code VARCHAR(100) DEFAULT ''")
            add_column_if_missing("support_case", "bank_id",
                                 "ALTER TABLE support_case ADD COLUMN bank_id VARCHAR(100) DEFAULT ''")
            add_column_if_missing("support_case", "bank_acc_number",
                                 "ALTER TABLE support_case ADD COLUMN bank_acc_number VARCHAR(100) DEFAULT ''")
            add_column_if_missing("support_case", "bank_acc_name",
                                 "ALTER TABLE support_case ADD COLUMN bank_acc_name VARCHAR(255) DEFAULT ''")
            add_column_if_missing("support_case", "bank_national_id",
                                 "ALTER TABLE support_case ADD COLUMN bank_national_id VARCHAR(100) DEFAULT ''")

            # أعمدة التذكير (للتكرار والإيقاف) (support_case)
            add_column_if_missing("support_case", "next_fire_at",
                                 "ALTER TABLE support_case ADD COLUMN next_fire_at VARCHAR(32) DEFAULT ''")
            add_column_if_missing("support_case", "dismissed",
                                 "ALTER TABLE support_case ADD COLUMN dismissed BOOLEAN DEFAULT 0")

        # --- ترقيع أعمدة User المفقودة ---
        if table_has_column("user", "id"):
            add_column_if_missing("user", "can_trader_services",
                                 "ALTER TABLE user ADD COLUMN can_trader_services BOOLEAN DEFAULT 0")
            add_column_if_missing("user", "can_support",
                                 "ALTER TABLE user ADD COLUMN can_support BOOLEAN DEFAULT 0")
            add_column_if_missing("user", "suspended",
                                 "ALTER TABLE user ADD COLUMN suspended BOOLEAN DEFAULT 0")
            
            # 🎉 ترقيع الأعمدة الجديدة لحل مشكلة 'no such column'
            add_column_if_missing("user", "can_settings",
                                 "ALTER TABLE user ADD COLUMN can_settings BOOLEAN DEFAULT 0")

            add_column_if_missing("user", "can_general_reports",
                                 "ALTER TABLE user ADD COLUMN can_general_reports BOOLEAN DEFAULT 0")
            add_column_if_missing("user", "can_inquiry",
                                 "ALTER TABLE user ADD COLUMN can_inquiry BOOLEAN DEFAULT 0")
            # صلاحيات فرعية لخدمات التجار
            add_column_if_missing("user", "can_trader_frequent",
                                 "ALTER TABLE user ADD COLUMN can_trader_frequent BOOLEAN DEFAULT 0")
            add_column_if_missing("user", "can_trader_primary",
                                 "ALTER TABLE user ADD COLUMN can_trader_primary BOOLEAN DEFAULT 0")
            
            # التصحيح النهائي لـ created_at
            add_column_if_missing("user", "created_at",
                                 "ALTER TABLE user ADD COLUMN created_at VARCHAR(32) DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now'))")

            db.session.commit()

            # --- ترقيع قيم الأعمدة الفارغة (للمستخدمين الموجودين) ---
            try:
                db.session.execute(text(
                    "UPDATE user SET created_at = strftime('%Y-%m-%d %H:%M:%S', 'now') WHERE created_at = ''"
                ))
                # 🛠️ تحديث لضبط القيم الافتراضية للأعمدة التي تم ترقيعها حديثًا
                db.session.execute(text(
                    """
                    UPDATE user SET 
                        can_trader_services = 0, 
                        can_support = 0, 
                        suspended = 0,
                        can_settings = 0,
                        can_general_reports = 0,
                        can_inquiry = 0,
                        can_trader_frequent = 0,
                        can_trader_primary = 0
                    WHERE 
                        can_trader_services IS NULL OR 
                        can_support IS NULL OR 
                        suspended IS NULL OR
                        can_settings IS NULL OR
                        can_general_reports IS NULL OR
                        can_inquiry IS NULL OR
                        can_trader_frequent IS NULL OR
                        can_trader_primary IS NULL
                    """
                ))
                db.session.commit()
            except Exception as e:
                print(f"Error during patching default values: {e}")
                db.session.rollback()


        # إنشاء مستخدم admin لو غير موجود
        if User.query.filter_by(username="admin").first() is None:
            admin = User(
                username="admin", 
                role="admin",
                password_hash=generate_password_hash("admin"),
                # 🎉 إضافة الصلاحيات الجديدة للمستخدم الأدمين
                can_trader_services=True,
                can_support=True,
                can_settings=True,
                can_general_reports=True,
                can_inquiry=True,
                suspended=False
            )
            db.session.add(admin)
            db.session.commit()

    # ===== المسارات العامة =====
    @app.route("/")
    def index():
        if getattr(current_user, "is_authenticated", False):
            return redirect(url_for("main_bp.dashboard"))
        nxt = request.args.get("next")
        if nxt:
            return redirect(url_for("auth_bp.login", next=nxt))
        return redirect(url_for("auth_bp.login"))

    # favicon
    @app.route("/favicon.ico")
    def favicon():
        return send_from_directory(
            os.path.join(app.root_path, "static", "images"),
            "icon.ico",
            mimetype="image/vnd.microsoft.icon",
        )

    # App Icon (لتصحيح خطأ 404 لملف /static/images/app-icon-192.png)
    @app.route("/static/images/app-icon-192.png")
    def app_icon():
        return send_from_directory(
            os.path.join(app.root_path, "static", "images"),
            "app-icon-192.png",
            mimetype="image/png",
        )

    # صفحات أخطاء (لو القوالب غير متاحة)
    @app.errorhandler(403)
    def forbidden(e):
        try:
            return render_template("errors/403.html"), 403
        except Exception:
            return "403 Forbidden", 403

    @app.errorhandler(404)
    def not_found(e):
        try:
            return render_template("errors/404.html"), 404
        except Exception:
            return "404 Not Found", 404

    @app.errorhandler(500)
    def server_error(e):
        try:
            return render_template("errors/500.html"), 500
        except Exception:
            return "500 Internal Server Error", 500

    # ===== خدمة عرض ملفات الصوت المرفوعة =====
    @app.route('/uploads/sounds/<path:filename>')
    def uploads_sound(filename: str):
        base_dir = os.path.abspath(os.path.dirname(__file__))
        sounds_dir = os.path.join(base_dir, 'instance', 'sounds')
        return send_from_directory(sounds_dir, filename)

    return app


# كائن app العالمي لـ flask run
app = create_app()

# تشغيل مباشر: python app.py
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

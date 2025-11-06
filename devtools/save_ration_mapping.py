import importlib.util
import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
APP_PATH = os.path.join(BASE_DIR, 'app.py')
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
spec = importlib.util.spec_from_file_location('appmod', APP_PATH)
appmod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(appmod)
app = appmod.app

from models import db, User

def main():
    with app.app_context():
        client = app.test_client()
        with client.session_transaction() as sess:
            admin = User.query.filter_by(username='admin').first()
            if not admin:
                try:
                    admin = User(username='admin', role='admin', can_inquiry=True)
                    admin.set_password('admin')
                    db.session.add(admin)
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                    admin = User.query.filter_by(username='admin').first()
            sess['_user_id'] = str(admin.id if admin else 1)

        form_data = {
            'order_csv': 'اسم العميل,رقم العميل,مسلسل,رقم الماكينة',
            'rename_lines': ''
        }
        resp = client.post('/reports/ration/save_mapping', data=form_data)
        print('save_mapping_status:', resp.status_code)
        print('redirect_to:', resp.headers.get('Location'))

        payload = {
            'category': 'ration',
            'search_type': 'code',
            'query': '224000000109',
            'visit_period': 'month'
        }
        r = client.post('/reports/api/inquiry_search', json=payload)
        print('inquiry_status:', r.status_code)
        try:
            data = r.get_json()
        except Exception:
            data = None
        print('primary_record_keys:', list((data or {}).get('primary_record', {}).keys())[:20])

if __name__ == '__main__':
    main()
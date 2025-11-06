import json
import importlib.util
import sys
import os

# تحميل app.py بطريقة صريحة مثل سكربت الاختبار السابق
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
        # ضمان وجود جلسة دخول لأدمن
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

        payload = {
            'category': 'ration',
            'query': '00018',  # جزء من الرقم فقط
            'search_type': 'code',
            'visit_period': 'recent_program'
        }
        resp = client.post('/reports/api/inquiry_search', json=payload)
        print('Status:', resp.status_code)
        try:
            data = resp.get_json()
        except Exception:
            data = None
        if not data:
            print('json: null')
            print('response_text:', resp.get_data(as_text=True)[:500])
            return

        print('Success:', data.get('success'))
        print('Message:', data.get('message'))
        items = data.get('items', [])
        print('Found items:', len(items))
        if items:
            first = items[0]
            print('Primary keys:', first.get('primary_record_keys'))
            cust = first.get('customer_data', {})
            print('Customer number:', cust.get('رقم العميل'))
            print('Customer name:', cust.get('اسم العميل'))

if __name__ == '__main__':
    main()
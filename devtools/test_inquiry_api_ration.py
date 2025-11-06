import json
import importlib.util
import sys
import os

# Load app.py explicitly
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
        # Authenticate session for Flask-Login
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

        # Verify inquiry page loads
        ping = client.get('/reports/inquiry')
        print('inquiry_view_status:', ping.status_code)

        # Use a code from ration dataset sample
        payload = {
            'category': 'ration',
            'search_type': 'code',
            'query': '224000000109',
            'visit_period': 'month'
        }
        resp = client.post('/reports/api/inquiry_search', json=payload)
        print('status:', resp.status_code)
        try:
            data = resp.get_json()
        except Exception:
            data = None
        if data is None:
            print('json: null')
            print('response_text:', resp.get_data(as_text=True)[:500])
            return
        keys_to_show = {
            'success': data.get('success'),
            'message': data.get('message'),
            'primary_match_mode': data.get('primary_match_mode'),
            'primary_record': data.get('primary_record'),
            'branch_section': data.get('branch_section'),
            'dynamic_fields': data.get('dynamic_fields'),
            'visit_debug_primary': data.get('visit_debug', {}).get('primary')
        }
        print(json.dumps(keys_to_show, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
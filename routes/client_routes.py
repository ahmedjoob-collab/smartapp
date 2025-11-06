# routes/client_routes.py

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from utils.decorators import role_required 
from models import db, Client 
from datetime import datetime

# تعريف الـ Blueprint باسم 'clients_bp'
clients_bp = Blueprint('clients_bp', __name__)

@clients_bp.route('/clients')
@login_required
@role_required('admin')
def clients():
    """
    مسار عرض قائمة العملاء (متاح للأدمن فقط).
    """
    clients_list = Client.query.all()
    return render_template('clients.html', clients=clients_list)

@clients_bp.route('/clients/add', methods=['POST'])
@login_required
@role_required('admin')
def add_client():
    """
    مسار إضافة عميل جديد.
    """
    name = request.form.get('name')
    phone = request.form.get('phone')
    service = request.form.get('service')
    # التاريخ الناقص الذي قمنا بإضافته في models.py سابقاً
    join_date = datetime.now().strftime('%Y-%m-%d') 
    
    if not name:
        flash('يجب إدخال اسم العميل.', 'danger')
        return redirect(url_for('clients_bp.clients'))

    new_client = Client(name=name, phone=phone, service=service, join_date=join_date)
    
    try:
        db.session.add(new_client)
        db.session.commit()
        flash('تم إضافة العميل بنجاح.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'حدث خطأ أثناء إضافة العميل: {str(e)}', 'danger')
        
    return redirect(url_for('clients_bp.clients'))


@clients_bp.route('/clients/delete/<int:client_id>', methods=['POST'])
@login_required
@role_required('admin')
def delete_client(client_id):
    """
    مسار حذف عميل.
    """
    client = Client.query.get_or_404(client_id)
    
    try:
        db.session.delete(client)
        db.session.commit()
        flash('تم حذف العميل بنجاح.', 'info')
    except Exception as e:
        db.session.rollback()
        flash(f'حدث خطأ أثناء حذف العميل: {str(e)}', 'danger')
        
    return redirect(url_for('clients_bp.clients'))

# يمكنك إضافة المزيد من الدوال هنا (مثل التعديل أو البحث)
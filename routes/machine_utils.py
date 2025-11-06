# routes/machine_utils.py

import sqlite3
import os
import pandas as pd
from flask import send_file
import openpyxl
import json # تم إضافة استيراد مكتبة json


# تحديد مسار قاعدة البيانات
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'instance', 'database.db')
# 🛡️ قائمة بيضاء لأسماء الجداول المسموح بها لمنع حقن SQL
ALLOWED_TABLES = ['machine_reports'] 


def search_in_reports(query, page=1, per_page=10):
    """
    البحث في جدول تقارير الآلات.
    """
    table_name = 'machine_reports'
    if table_name not in ALLOWED_TABLES:
        return {'items': [], 'total': 0, 'pages': 0}

    offset = (page - 1) * per_page
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if query:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE report_data LIKE ? OR timestamp LIKE ?", ('%' + query + '%', '%' + query + '%',))
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT * FROM {table_name} WHERE report_data LIKE ? OR timestamp LIKE ? LIMIT ? OFFSET ?", ('%' + query + '%', '%' + query + '%', per_page, offset))
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (per_page, offset))

    items = cursor.fetchall()
    conn.close()
    
    pages = (total + per_page - 1) // per_page
    return {'items': items, 'total': total, 'pages': pages}


def merge_machine_reports(file_path, table_name):
    """
    دمج تقارير الآلات من ملف Excel إلى قاعدة البيانات.
    """
    if table_name not in ALLOWED_TABLES: 
        return False, f"Invalid table name: {table_name}"
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 💡 تم تعديل هنا: يمكن إضافة (dtype=str) للقراءة كنص أثناء الاستيراد لمنع تحويل الأرقام (إذا كنت تستخدمها):
        # df = pd.read_excel(file_path, dtype=str) 
        df = pd.read_excel(file_path)
        
        if 'report_data' not in df.columns or 'timestamp' not in df.columns:
            return False, "ملف Excel يجب أن يحتوي على عمودي 'report_data' و 'timestamp'."

        for index, row in df.iterrows():
            report_data = str(row['report_data'])
            timestamp = str(row['timestamp'])

            cursor.execute(f"INSERT INTO {table_name} (report_data, timestamp) VALUES (?, ?)", 
                           (report_data, timestamp))
            
        conn.commit()
        conn.close()
        return True, "تم دمج التقارير بنجاح."
    
    except Exception as e:
        conn.close()
        return False, f"حدث خطأ أثناء الدمج: {str(e)}"

def export_table_to_excel(table_name):
    """
    تصدير تقرير من قاعدة البيانات إلى ملف Excel.
    """
    if table_name not in ALLOWED_TABLES: 
        return None, "Invalid table name"
        
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except pd.io.sql.DatabaseError as e:
        conn.close()
        return None, str(e)
    conn.close()

    output_filename = f"{table_name}_report.xlsx"
    output_path = os.path.join(os.path.dirname(__file__), '..', 'tmp', output_filename) 
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_excel(output_path, index=False)
    
    return send_file(output_path, as_attachment=True, download_name=output_filename), None

# 💡 الدالة المُبسَّطة: inquiry_search_in_reports
def inquiry_search_in_reports(category: str, search_type: str, query: str):
    """
    البحث في التقارير العامة (machine_reports) بناءً على تبويب ونوع بحث معين.
    """
    if not query or not category or not search_type:
        return {'success': False, 'message': 'برجاء إدخال قيمة بحث صالحة.'}

    table_name = 'machine_reports'
    if table_name not in ALLOWED_TABLES:
        return {'success': False, 'message': 'خطأ في إعداد قاعدة البيانات.'}

    # 1. تحديد اسم العمود (المفتاح داخل JSON) بناءً على الأسماء المؤكدة
    if search_type == 'code':
        # التحديث: استخدام 'رقم العميل' بدلاً من 'رقم المخبز'/'رقم التاجر'
        if category in ['bakeries', 'ration', 'substitute']:
            col_name = 'رقم العميل'
        else:
            return {'success': False, 'message': 'فئة غير صالحة.'}
    elif search_type == 'name':
        # إضافة خيار البحث بـ 'اسم العميل'
        if category in ['bakeries', 'ration', 'substitute']:
            col_name = 'اسم العميل'
        else:
            return {'success': False, 'message': 'فئة غير صالحة.'}
    elif search_type == 'serial':
        # دعم مرادفات متعددة لمسلسل الماكينة عبر الجداول المختلفة
        possible_serial_cols = [
            'مسلسل الماكينة', 'مسلسل', 'Serial', 'POS Serial', 'SN', 'رقم الماكينة'
        ]
        col_name = None
        # سنختار أول عمود متاح من القائمة بناءً على الأعمدة الفعلية في السجل
        # ملاحظة: يتطلب معرفة الأعمدة؛ إن لم نستطع هنا، نستخدم القيمة الافتراضية
        # الافتراضي يبقى 'مسلسل الماكينة' لضمان التوافق الخلفي
        col_name = 'مسلسل الماكينة'
    else:
        return {'success': False, 'message': 'نوع بحث غير صالح.'}

    # تجهيز قيمة البحث النظيفة (إزالة الفراغات من إدخال المستخدم)
    query_stripped = query.strip()
    
    # 2. البحث الأولي في قاعدة البيانات لتضييق النطاق
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # البحث عن القيمة في كل السجل
    general_query_pattern = '%' + query_stripped + '%'
    cursor.execute(f"SELECT report_data FROM {table_name} WHERE report_data LIKE ?", (general_query_pattern,))
    
    raw_results = cursor.fetchall()
    conn.close()

    # 3. الفلترة الدقيقة في Python (بأقل تدخل ممكن)
    final_results = []
    
    for row in raw_results:
        report_data_str = row[0]
        try:
            # التعامل مع السجلات التالفة/الفارغة قبل التحليل
            if not report_data_str or report_data_str.strip() in ['{}', 'null', 'None']:
                continue
                
            data_dict = json.loads(report_data_str)
            
            # التحقق من وجود العمود المطلوب
            if col_name in data_dict:
                
                col_value_raw = data_dict.get(col_name)
                # الخطوة الوحيدة المتبقية: تحويل إلى نص وتنظيف الفراغات
                col_value_cleaned = str(col_value_raw or '').strip()
                
                # التحقق من أن قيمة البحث النظيفة موجودة كجزء من القيمة النظيفة للعمود
                if query_stripped in col_value_cleaned:
                    final_results.append(data_dict)
                    
        except json.JSONDecodeError:
            continue # تجاهل السجلات التي لا يمكن فك تشفيرها
        except Exception:
            continue
            
    # 4. التحقق من عدد النتائج
    if len(final_results) == 0:
        return {'success': False, 'message': 'لم يتم العثور على سجل يطابق معايير البحث.'}
    elif len(final_results) > 1:
        return {'success': False, 'message': 'برجاء إضافة رقم صحيح/فريد. تم العثور على أكثر من سجل مطابق.'}
    else:
        return {'success': True, 'data': final_results[0]}
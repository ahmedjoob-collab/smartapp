# routes/trader_utils.py

import sqlite3
import os
import pandas as pd
from flask import send_file

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'instance', 'database.db')
# 🛡️ قائمة بيضاء لأسماء الجداول
ALLOWED_TABLES = ['frequent_visitors', 'basic_customers'] 

# 🟢 الدالة المصححة: اسمها الآن 'import_excel_to_table'
def import_excel_to_table(file_path, table_name):
    """
    استيراد البيانات من ملف Excel إلى جدول محدد.
    """
    if table_name not in ALLOWED_TABLES: 
        return False, f"Invalid table name: {table_name}"
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        df = pd.read_excel(file_path)
        
        if table_name == 'frequent_visitors':
            required_cols = ['name', 'visit_count', 'data']
        elif table_name == 'basic_customers':
            required_cols = ['name', 'data']
        else:
            return False, "تكوين جدول غير معروف."
            
        if not all(col in df.columns for col in required_cols):
             return False, f"ملف Excel يجب أن يحتوي على الأعمدة التالية: {', '.join(required_cols)}."

        for index, row in df.iterrows():
            if table_name == 'frequent_visitors':
                # تأكد من تحويل الأعمدة الرقمية إلى int/str حسب الضرورة
                cursor.execute(f"INSERT INTO {table_name} (name, visit_count, data) VALUES (?, ?, ?)", 
                               (str(row['name']), int(row['visit_count']), str(row['data'])))
            elif table_name == 'basic_customers':
                cursor.execute(f"INSERT INTO {table_name} (name, data) VALUES (?, ?)", 
                               (str(row['name']), str(row['data'])))
            
        conn.commit()
        conn.close()
        return True, "تم استيراد البيانات بنجاح."
    
    except Exception as e:
        conn.close()
        return False, f"حدث خطأ أثناء الاستيراد: {str(e)}"


def search_in_table(table_name, query, page=1, per_page=10):
    """
    البحث في جدول محدد (المترددون أو العملاء الأساسيون).
    """
    if table_name not in ALLOWED_TABLES: 
        return {'items': [], 'total': 0, 'pages': 0}

    offset = (page - 1) * per_page
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if query:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE name LIKE ?", ('%' + query + '%',))
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT * FROM {table_name} WHERE name LIKE ? LIMIT ? OFFSET ?", ('%' + query + '%', per_page, offset))
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()[0]
        cursor.execute(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (per_page, offset))

    items = cursor.fetchall()
    conn.close()
    
    pages = (total + per_page - 1) // per_page
    return {'items': items, 'total': total, 'pages': pages}


def export_table_to_excel(table_name):
    """
    تصدير البيانات من جدول محدد إلى ملف Excel.
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
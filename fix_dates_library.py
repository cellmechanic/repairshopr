"""fix date formatting issue between MariaDB and RS"""
from datetime import datetime


def format_date_fordb(date_str):
    """pass in RS date, return MariaDB formatted"""
    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return date_obj.strftime('%Y-%m-%d %H:%M:%S')

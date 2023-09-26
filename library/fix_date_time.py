"""fix date / time formatting issue between MariaDB and RS"""
from datetime import datetime


def format_date_fordb(date_str):
    """pass in RS date, return MariaDB formatted"""
    if date_str is None:
        return None
    date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")


def rs_to_unix_timestamp(rs_time):
    """Parse the datetime string into a datetime object in local timezone"""
    if isinstance(rs_time, str):
        rs_time_str = rs_time
    else:
        rs_time_str = str(rs_time)

    convert = datetime.fromisoformat(rs_time_str)

    # Convert the datetime object to a UNIX timestamp
    timestamp = int(convert.timestamp())
    return timestamp


def get_timestamp_code():
    """return the correct timestamp code to stay uniform"""
    return "%Y-%m-%dT%H:%M:%S.%f%z"


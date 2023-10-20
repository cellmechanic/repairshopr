"""This module is responsible for updating the employee_output table"""
import datetime
from library import env_library
from library.db_general import connect_to_db
from library.output_library import (
    create_employee_output_table_if_not_exists,
    get_comments_db,
    get_intake_comments,
    get_invoice_numbers,
    get_output_comments,
    insert_intake_numbers,
    insert_invoice_numbers,
    insert_regex_comments,
)


def output(logger, lookback_days=30):
    """Setup the output table and pull todays comments"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_employee_output_table_if_not_exists(cursor)

    date = datetime.date.today()
    date = date - datetime.timedelta(days=lookback_days)

    all_comments = get_comments_db(logger, cursor, date)

    output_comments = get_output_comments(logger, all_comments)

    intake_comments = get_intake_comments(logger, cursor, date)

    invoices_created = get_invoice_numbers(cursor, date)

    insert_regex_comments(logger, cursor, output_comments)

    insert_intake_numbers(cursor, intake_comments)

    insert_invoice_numbers(cursor, invoices_created)

    connection.commit()
    connection.close()

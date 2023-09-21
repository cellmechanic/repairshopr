"""This module is responsible for updating the employee_output table"""
import datetime
from library import env_library
from library.db_general import connect_to_db
from library.db_output import (
    create_employee_output_table_if_not_exists,
    get_comments_db,
    get_intake_comments,
    get_output_comments,
    insert_intake_comments,
    insert_regex_comments,
)


def output(logger):
    """Setup the output table and pull todays comments"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_employee_output_table_if_not_exists(cursor)

    today = datetime.date.today()

    all_comments = get_comments_db(logger, cursor, today)

    output_comments = get_output_comments(logger, all_comments)

    intake_comments = get_intake_comments(logger, cursor, all_comments)

    insert_regex_comments(logger, cursor, output_comments)

    insert_intake_comments(logger, cursor, intake_comments)

    connection.commit()
    connection.close()

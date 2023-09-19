"""This module is responsible for updating the employee_output table"""
import datetime
from library import env_library
from library.db_general import connect_to_db
from library.db_output import (
    create_employee_output_table_if_not_exists,
    get_comments_db,
    get_output_comments,
    insert_output_db,
)


def output(logger):
    """Setup the output table and pull todays comments"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_employee_output_table_if_not_exists(cursor)

    today = datetime.date.today()

    query = """
        SELECT id as comment_id, created_at, ticket_id, body, tech, user_id
        FROM comments
        WHERE DATE(created_at) >= %s"""

    all_comments = get_comments_db(logger, cursor, query, today)

    output_comments = get_output_comments(logger, all_comments)

    insert_output_db(logger, cursor, output_comments)

    connection.commit()
    connection.close()

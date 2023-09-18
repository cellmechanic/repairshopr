"""This module is responsible for updating the employee_output table"""
import datetime
import re
from library import env_library
from library.db_create import create_employee_output_table_if_not_exists
from library.db_general import connect_to_db


def output(logger):
    """Setup the output table and pull todays comments"""
    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_employee_output_table_if_not_exists(cursor)

    today = datetime.datetime.now()

    query = """
        SELECT id, created_at, ticket_id, body, tech, employee_id
        FROM comments
        WHERE DATE(created_at) >= %s"""

    cursor.execute(query, (today,))
    comments = cursor.fetchall()

    pattern = r"\[(\w+)(?::(\d+)(?::(\d+))?)?\]"

    for comment in comments:
        id, created_at, ticket_id, body, tech, employee_id = comment

        matches = re.findall(pattern, body)

        for match in matches:
            category, count, employee_id = match

            if category == "r":
                sql = """
                    INSERT INTO employee_output
                    (created_at, ticket_id, body, tech, employee_id, count, category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE count = count + %s"""
                values = (
                    created_at,
                    ticket_id,
                    body,
                    tech,
                    employee_id,
                    count,
                    category,
                    count,
                )
                cursor.execute(sql, values)

    connection.commit()
    connection.close()

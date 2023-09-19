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

    today = datetime.date.today()

    query = """
        SELECT id as comment_id, created_at, ticket_id, body, tech, user_id
        FROM comments
        WHERE DATE(created_at) >= %s"""

    cursor.execute(query, (today,))
    todays_comments = cursor.fetchall()

    for data in todays_comments:
        comment_id, created_at, ticket_id, body, tech, user_id = data

        production_info = ""
        if body is not None and isinstance(body, str):
            production_info = re.findall(r"\[(.*?)\]", body)

        # Parse production information and insert as separate rows
        if production_info is not None:
            for info in production_info:
                parts = info.split(":")
                if len(parts) == 2:
                    job_type, count = parts
                    count = int(count)
                    if job_type.lower() == "r":
                        repairs = count
                        diagnostics = 0
                        quality_control = 0
                        quality_control_rejects = 0
                        quality_control_rejected_person = ""
                    elif job_type.lower() == "d":
                        repairs = 0
                        diagnostics = count
                        quality_control = 0
                        quality_control_rejects = 0
                        quality_control_rejected_person = ""
                    elif job_type.lower() == "qc":
                        repairs = 0
                        diagnostics = 0
                        quality_control = count
                        quality_control_rejects = 0
                        quality_control_rejected_person = ""
                    elif job_type.lower() == "qcr":
                        repairs = 0
                        diagnostics = 0
                        quality_control = 0
                        quality_control_rejects = count
                        # Extract the employee name from qcr entries
                        employee_name = re.search(r"\[.*?:(.*?)]", info)
                        if employee_name:
                            quality_control_rejected_person = employee_name.group(1)
                    else:  # incase there's bad data
                        repairs = 0
                        diagnostics = 0
                        quality_control = 0
                        quality_control_rejects = 0
                        quality_control_rejected_person = ""



            # Insert data into the employee_output table
            query = """
                    INSERT INTO employee_output
                    (ticket_id, comment_id, employee_id, username, repairs, diagnostics, quality_control, quality_control_rejects, quality_control_rejected_person, datetime)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            values = (
                ticket_id,
                comment_id,
                user_id,
                tech,
                repairs,
                diagnostics,
                quality_control,
                quality_control_rejects,
                quality_control_rejected_person,
                created_at,
            )
            cursor.execute(query, values)

    connection.commit()
    connection.close()

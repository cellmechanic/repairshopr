"""Library for the output module"""


import re

def create_employee_output_table_if_not_exists(cursor):
    """Create the employee_output db table if it doesn't already exist"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS employee_output (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticket_id INT,
            comment_id INT UNIQUE,
            employee_id INT,            
            username VARCHAR(255),
            repairs INT DEFAULT 0,
            board_repair INT DEFAULT 0,
            diagnostics INT DEFAULT 0,
            quality_control INT DEFAULT 0,
            quality_control_rejects INT DEFAULT 0,
            quality_control_rejected_person VARCHAR(255),
            datetime DATETIME,
            notes TEXT DEFAULT NULL
        )
        """
    )


def get_comments_db(logger, cursor, query, date):
    """Get comments from DB"""
    cursor.execute(query, (date,))
    logger.info(
        "Number of comments in DB: %s",
        cursor.rowcount,
        extra={"tags": {"service": "output comments"}},
    )
    return cursor.fetchall()


def get_output_comments(logger, all_comments):
    """Process all the comments and extract production ones"""
    production_comments = []

    if all_comments is None:
        logger.warning(
            "No comments found in DB",
            extra={"tags": {"service": "output comments"}},
        )

    if all_comments is not None:
        for comment_id, created_at, ticket_id, body, tech, user_id in all_comments:
            matches = re.findall(r"\[(.*?)\]", body)

            for match in matches:
                production_comments.append(
                    {
                        "comment_id": comment_id,
                        "created_at": created_at,
                        "ticket_id": ticket_id,
                        "body": match,
                        "tech": tech,
                        "user_id": user_id,
                    }
                )

    logger.info(
        "Number of potential production comments: %s",
        len(production_comments),
        extra={"tags": {"service": "output comments"}},
    )

    return production_comments


def insert_output_db(logger, cursor, data):
    """Insert employee output data into DB"""
    for comment in data:
        production_info = comment["body"]
        parts = [part.strip() for part in production_info.split(":")]
        if len(parts) == 2:
            job_type, count = parts
            if count.isdigit():
                count = int(count)
            else:
                count = 0
                logger.error(
                    "Production data with no number on ticket %s",
                    comment["ticket_id"],
                    extra={"tags": {"output errors": "no number"}},
                )
            quality_control_rejected_person = ""
            if job_type.lower() == "r":
                repairs = count
                board_repair = 0
                diagnostics = 0
                quality_control = 0
                quality_control_rejects = 0
                quality_control_rejected_person = ""
            elif job_type.lower() == "br":
                repairs = 0
                board_repair = count
                diagnostics = 0
                quality_control = 0
                quality_control_rejects = 0
                quality_control_rejected_person = ""
            elif job_type.lower() == "d":
                repairs = 0
                board_repair = 0
                diagnostics = count
                quality_control = 0
                quality_control_rejects = 0
                quality_control_rejected_person = ""
            elif job_type.lower() == "qc":
                repairs = 0
                board_repair = 0
                diagnostics = 0
                quality_control = count
                quality_control_rejects = 0
                quality_control_rejected_person = ""
            elif job_type.lower() == "qcr":
                repairs = 0
                board_repair = 0
                diagnostics = 0
                quality_control = 0
                quality_control_rejects = count
                # Extract the employee name from qcr entries
                employee_name = parts[1].strip().split(":")
                if len(employee_name) == 2:
                    quality_control_rejected_person = employee_name[1]
            else:  # in case there's bad data
                repairs = 0
                board_repair = 0
                diagnostics = 0
                quality_control = 0
                quality_control_rejects = 0
                quality_control_rejected_person = ""

            # Insert data into the employee_output table
            query = """
                INSERT IGNORE INTO employee_output
                (ticket_id, comment_id, employee_id, username, repairs, board_repair, diagnostics, quality_control, quality_control_rejects, quality_control_rejected_person, datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                comment["ticket_id"],
                comment["comment_id"],
                comment["user_id"],
                comment["tech"],
                repairs,
                board_repair,
                diagnostics,
                quality_control,
                quality_control_rejects,
                quality_control_rejected_person,
                comment["created_at"],
            )
            cursor.execute(query, values)

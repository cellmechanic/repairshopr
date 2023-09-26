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
            intake INT DEFAULT 0,
            invoices INT DEFAULT 0,
            datetime DATETIME,
            notes TEXT DEFAULT NULL
        )
        """
    )


def get_comments_db(logger, cursor, date):
    """Get comments from DB"""
    query = """
    SELECT id as comment_id, created_at, ticket_id, subject, body, tech, user_id
    FROM comments
    WHERE DATE(created_at) >= %s"""

    cursor.execute(query, (date,))
    logger.info(
        "Number of comments in DB: %s",
        cursor.rowcount,
        extra={"tags": {"service": "output comments"}},
    )

    comments_data = cursor.fetchall()
    comments_list = [
        {
            "comment_id": record[0],
            "created_at": record[1],
            "ticket_id": record[2],
            "subject": record[3],
            "body": record[4],
            "tech": record[5],
            "user_id": record[6],
        }
        for record in comments_data
    ]

    return comments_list


def get_output_comments(logger, all_comments):
    """Process all the comments and extract production ones"""
    production_comments = []

    if all_comments is None:
        logger.warning(
            "No comments found in DB",
            extra={"tags": {"service": "output comments"}},
        )

    if all_comments is not None:
        for comment in all_comments:
            comment_id = comment["comment_id"]
            created_at = comment["created_at"]
            ticket_id = comment["ticket_id"]
            subject = comment["subject"]
            body = comment["body"]
            tech = comment["tech"]
            user_id = comment["user_id"]
            matches = re.findall(r"\[(.*?)\]", body)

            for match in matches:
                production_comments.append(
                    {
                        "comment_id": comment_id,
                        "created_at": created_at,
                        "ticket_id": ticket_id,
                        "subject": subject,
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


def get_intake_comments(logger, cursor, all_comments):
    """Process all the comments and extract intake ones"""
    checked_in_comments = []

    if all_comments is None:
        logger.warning(
        "No comments found in DB",
        extra={"tags": {"service": "output comments"}},
    )

    if all_comments is not None:
        for comment in all_comments:
            if comment.get("subject") == "Initial Issue":
                ticket_id = comment.get("ticket_id")
                if ticket_id:
                    cursor.execute(
                        "SELECT num_devices FROM tickets WHERE id = %s", (ticket_id,)
                    )
                    result = cursor.fetchone()
                    if result:
                        num_devices = result[0]
                        checked_in_comments.append(
                            {
                                "comment_id": comment.get("comment_id"),
                                "created_at": comment.get("created_at"),
                                "ticket_id": ticket_id,
                                "subject": comment.get("subject"),
                                "num_devices": num_devices,
                                "user_id": comment.get("user_id"),
                                "tech": comment.get("tech"),
                                "body": comment.get("body"),
                            }
                        )

    logger.info(
        "Number of potential intake comments: %s",
        len(checked_in_comments),
        extra={"tags": {"service": "output comments"}},
    )

    return checked_in_comments


def insert_regex_comments(logger, cursor, data):
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
                    "Production data with no number on ticket"
                    " https://cellmechanic.repairshopr.com/tickets/%s",
                    comment["ticket_id"],
                    extra={"tags": {"output errors": "no number"}},
                )
            # In case of bad data, set defaults null
            quality_control_rejected_person = ""
            repairs = 0
            board_repair = 0
            diagnostics = 0
            quality_control = 0
            quality_control_rejects = 0

            if job_type.lower() == "r":
                repairs = count
            elif job_type.lower() == "br":
                board_repair = count
            elif job_type.lower() == "d":
                diagnostics = count
            elif job_type.lower() == "qc":
                quality_control = count
            elif job_type.lower() == "qcr":
                quality_control_rejects = count
                # Extract the employee name from qcr entries
                employee_name = parts[1].strip().split(":")
                if len(employee_name) == 2:
                    quality_control_rejected_person = employee_name[1]

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

def insert_intake_comments(logger, cursor, intake_comments):
    """Insert intake comments into DB"""
    for comment in intake_comments:
        query = """
            INSERT IGNORE INTO employee_output
            (ticket_id, comment_id, employee_id, username, intake, datetime)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            comment["ticket_id"],
            comment["comment_id"],
            comment["user_id"],
            comment["tech"],
            comment["num_devices"],
            comment["created_at"],
        )
        cursor.execute(query, values)

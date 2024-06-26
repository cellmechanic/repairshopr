"""Library for the output module"""
import datetime
import re
import json


def create_employee_output_table_if_not_exists(cursor):
    """Create the employee_output db table if it doesn't already exist"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS employee_output (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticket_id INT,
            comment_id INT UNIQUE,
            invoice_id INT UNIQUE,
            employee_id INT,            
            username VARCHAR(255),
            repairs INT DEFAULT 0,
            repair_rejects INT DEFAULT 0,
            board_repair_pass INT DEFAULT 0,
            board_repair_fail INT DEFAULT 0,
            diagnostics INT DEFAULT 0,
            quality_control INT DEFAULT 0,
            quality_control_rejects INT DEFAULT 0,
            quality_control_rejected_person VARCHAR(255),
            intake INT DEFAULT 0,
            invoices INT DEFAULT 0,
            datetime DATETIME,
            valid TINYINT DEFAULT 1,
            reworked TINYINT DEFAULT 0,
            wfh TINYINT DEFAULT 0,
            original_comment TEXT DEFAULT NULL,
            notes TEXT DEFAULT NULL,
            linked_comment_id INT DEFAULT 0
        )
        """
    )


def create_part_comments_table_if_not_exists(cursor):
    """Create the part_comments db table if it doesn't already exist"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS part_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticket_id INT,
            comment_id INT UNIQUE,
            employee_id INT,            
            parts JSON,
            notes TEXT,
            datetime DATETIME
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
                        "body": body,
                        "tech": tech,
                        "user_id": user_id,
                        "code": match,
                    }
                )

    logger.info(
        "Number of potential production comments: %s",
        len(production_comments),
        extra={"tags": {"service": "output comments"}},
    )

    return production_comments


def get_intake_comments(logger, cursor, date):
    """Process all the comments and extract intake ones"""

    checked_in_comments = []

    cursor.execute(
        "SELECT id, num_devices FROM tickets WHERE DATE(created_at) >= %s", (date,)
    )
    tickets_created = cursor.fetchall()

    if not tickets_created:
        logger.warning(
            "No comments / tickets found in DB",
            extra={"tags": {"service": "output comments"}},
        )
        return []

    for ticket in tickets_created:
        ticket_id = ticket[0]
        num_devices = ticket[1]

        cursor.execute(
            "SELECT id, created_at, user_id, tech FROM comments "
            "WHERE ticket_id = %s ORDER BY created_at ASC LIMIT 1",
            (ticket_id,),
        )
        comment = cursor.fetchone()

        if comment:
            checked_in_comments.append(
                {
                    "comment_id": comment[0],
                    "created_at": comment[1],
                    "ticket_id": ticket_id,
                    "num_devices": num_devices,
                    "user_id": comment[2],
                    "tech": comment[3],
                }
            )
    return checked_in_comments


def get_invoice_numbers(cursor, date):
    """Insert invoice creates into DB"""
    cursor.execute(
        "SELECT id, user_id, ticket_id, created_at FROM invoices WHERE DATE(created_at) >= %s",
        (date,),
    )
    invoices_created = []
    todays_invoices = cursor.fetchall()
    if todays_invoices:
        for invoice in todays_invoices:
            invoice_id = invoice[0]
            user_id = invoice[1]
            ticket_id = invoice[2]
            created_at = invoice[3]

            # Fetch the num_devices from the tickets table
            cursor.execute(
                "SELECT num_devices FROM tickets WHERE id = %s", (ticket_id,)
            )
            result = cursor.fetchone()
            if result:
                num_devices = result[0]
            else:
                num_devices = 1

            if user_id:
                # Fetch the username from the users table
                cursor.execute("SELECT name FROM users WHERE id = %s", (user_id,))
                user_result = cursor.fetchone()
                username = user_result[0] if user_result else "Unknown"
            else:
                username = "None"

            invoices_created.append(
                {
                    "invoice_id": invoice_id,
                    "user_id": user_id,
                    "ticket_id": ticket_id,
                    "num_devices": num_devices,
                    "username": username,
                    "created_at": created_at,
                }
            )

    return invoices_created


def insert_intake_numbers(cursor, intake_comments):
    """Insert intake comments into DB"""
    for comment in intake_comments:
        query = """
            INSERT employee_output
            (ticket_id, comment_id, employee_id, username, intake, datetime, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            notes = values(notes),
            intake = values(intake)
        """
        values = (
            comment["ticket_id"],
            comment["comment_id"],
            comment["user_id"],
            comment["tech"],
            comment["num_devices"],
            comment["created_at"],
            "intake",
        )
        cursor.execute(query, values)


def insert_invoice_numbers(cursor, invoices_created):
    """Insert invoice creates into DB"""
    for invoice in invoices_created:
        query = """
            INSERT employee_output
            (ticket_id, invoice_id, employee_id, username, invoices, datetime, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            notes = values(notes)            
        """
        values = (
            invoice["ticket_id"],
            invoice["invoice_id"],
            invoice["user_id"],
            invoice["username"],
            invoice["num_devices"],
            invoice["created_at"],
            "invoice",
        )
        cursor.execute(query, values)


def insert_regex_comments(logger, cursor, data):
    """Insert employee output data into DB"""
    for comment in data:
        production_info = comment["code"]
        job_type = ""
        count = 0
        valid = 1
        notes = comment["code"]
        original_comment = comment["body"]
        userid = 0
        rework = 0
        ticket_id = comment["ticket_id"]
        cursor.execute("SELECT num_devices FROM tickets WHERE id = %s", (ticket_id,))
        ticket_data = cursor.fetchone()
        num_devices = ticket_data[0] if ticket_data else 1

        delimiters = [":", ";", "::", ";;", '"']
        for delimiter in delimiters:
            parts = [part.strip() for part in production_info.split(delimiter)]
            if len(parts) == 1:
                job_type = parts[0]
                if job_type == "rework":
                    rework = 1
                else:
                    valid = 0

            if len(parts) == 2:
                job_type, count = parts
                if count.isdigit():
                    count = int(count)
                    break
                else:
                    count = 0
                    valid = 0

            if len(parts) == 3:
                job_type, count, userid = parts
                if count.isdigit() and userid.isdigit():
                    count = int(count)
                    userid = int(userid)
                    break
                elif job_type.lower() == "r" or "d" or "qc" or "mb" or "mbp" or "mbf":
                    if count.isdigit() and userid is None:
                        count = int(count)
                        break
                    elif userid.isdigit() and count is None:
                        count = int(userid)
                        break
                else:
                    count = 0
                    valid = 0
                    logger.error(
                        "Production data with no number on ticket"
                        + " removed for now%s",
                        comment["ticket_id"],
                        extra={"tags": {"output errors": "no number"}},
                    )
        # In case of bad data, set defaults null
        quality_control_rejected_person = ""
        repairs = 0
        repair_rejects = 0
        board_repair_pass = 0
        board_repair_fail = 0
        diagnostics = 0
        quality_control = 0
        quality_control_rejects = 0
        reworked = 0
        wfh = 0

        if isinstance(count, str):
            valid = 0
            notes += " - Not a number"
        elif count == 0:
            valid = 0
            notes += " - No number"
        elif job_type.lower() == "r":
            if count <= num_devices:
                repairs = count
            else:
                valid = 0
                notes += " - More repairs than devices"
        elif job_type.lower() == "d":
            if count <= num_devices:
                diagnostics = count
            else:
                valid = 0
                notes += " - More diagnostics than devices"
        elif job_type.lower() == "qc":
            if count <= num_devices:
                quality_control = count
            else:
                valid = 0
                notes += " - More quality control than devices"
        elif job_type.lower() == "mbp" or job_type.lower() == "mb":
            if count <= num_devices:
                board_repair_pass = count
            else:
                valid = 0
                notes += " - More board repairs than devices"
        elif job_type.lower() == "mbf":
            if count <= num_devices:
                board_repair_fail = count
            else:
                valid = 0
                notes += " - More board repairs than devices"
        elif job_type.lower() == "qcr":
            if count <= num_devices:
                quality_control = count
                quality_control_rejects = count
            else:
                valid = 0
                notes += " - More quality control rejects than devices"
        else:
            valid = 0

        created_at_time = comment["created_at"].time()
        if created_at_time < datetime.time(7, 45) or created_at_time > datetime.time(18, 30):
            wfh = 1
        else:
            wfh = 0

        # Insert data into the employee_output table
        query = """
            INSERT employee_output
            (
                ticket_id, 
                comment_id, 
                employee_id, 
                username, 
                repairs, 
                repair_rejects, 
                board_repair_pass, 
                board_repair_fail, 
                diagnostics, 
                quality_control, 
                quality_control_rejects, 
                quality_control_rejected_person, 
                datetime, 
                valid, 
                reworked, 
                wfh, 
                original_comment, 
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                ticket_id = values(ticket_id),
                comment_id = values(comment_id),
                employee_id = values(employee_id),
                username = values(username),
                repairs = values(repairs),
                repair_rejects = values(repair_rejects),
                board_repair_pass = values(board_repair_pass),
                board_repair_fail = values(board_repair_fail),
                diagnostics = values(diagnostics),
                quality_control = values(quality_control),
                quality_control_rejects = values(quality_control_rejects),
                quality_control_rejected_person = values(quality_control_rejected_person),
                datetime = values(datetime),
                valid = values(valid),
                reworked = values(reworked),
                wfh = values(wfh),
                original_comment = values(original_comment),
                notes = values(notes)
        """
        values = (
            comment["ticket_id"],
            comment["comment_id"],
            comment["user_id"],
            comment["tech"],
            repairs,
            repair_rejects,
            board_repair_pass,
            board_repair_fail,
            diagnostics,
            quality_control,
            quality_control_rejects,
            quality_control_rejected_person,
            comment["created_at"],
            valid,
            reworked,
            wfh,
            original_comment,
            notes,
        )
        cursor.execute(query, values)

        if job_type.lower() == "qcr":
            query = """SELECT wfh 
                    FROM employee_output 
                    WHERE ticket_id = %s AND repairs > 0 
                    ORDER BY datetime DESC 
                    LIMIT 1"""
            cursor.execute(query, (ticket_id,))
            entries = cursor.fetchall()
            if entries:
                wfh = entries[0][0]
            else:
                valid = 0
                notes += "No repair found to reject"

            query = """SELECT id from employee_output WHERE comment_id = %s"""
            cursor.execute(query, (comment["comment_id"],))
            result = cursor.fetchone()

            if result and isinstance(count, int):
                reject_user = ""
                cursor.execute(
                    "SELECT name FROM users WHERE id = %s",
                    (userid,),
                )
                reject_user = cursor.fetchone()
                if reject_user:
                    reject_user = reject_user[0]
                    repair_rejects = count
                else:
                    reject_user = "Unknown"

                query = """
                    INSERT employee_output
                    (
                        ticket_id, 
                        comment_id, 
                        employee_id, 
                        username, 
                        repairs, 
                        repair_rejects, 
                        board_repair_pass, 
                        board_repair_fail, 
                        diagnostics, 
                        quality_control, 
                        quality_control_rejects, 
                        quality_control_rejected_person, 
                        datetime, 
                        valid, 
                        reworked, 
                        wfh, 
                        original_comment, 
                        notes
                    )
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        ticket_id = values(ticket_id),
                        comment_id = values(comment_id),
                        employee_id = values(employee_id),
                        username = values(username),
                        repairs = values(repairs),
                        repair_rejects = values(repair_rejects),
                        board_repair_pass = values(board_repair_pass),
                        board_repair_fail = values(board_repair_fail),
                        diagnostics = values(diagnostics),
                        quality_control = values(quality_control),
                        quality_control_rejects = values(quality_control_rejects),
                        quality_control_rejected_person = values(quality_control_rejected_person),
                        datetime = values(datetime),
                        valid = values(valid),
                        reworked = values(reworked),
                        wfh = values(wfh),
                        original_comment = values(original_comment),
                        notes = values(notes)                  
                """
                notes += (
                    " rejected by "
                    + comment["tech"]
                    + " original comment id "
                    + str(comment["comment_id"])
                )
                values = (
                    ticket_id,
                    comment["ticket_id"],
                    userid,
                    reject_user,
                    -count,
                    count,
                    board_repair_pass,
                    board_repair_fail,
                    diagnostics,
                    0,
                    0,
                    comment["tech"],
                    comment["created_at"],
                    valid,
                    reworked,
                    wfh,
                    original_comment,
                    notes,
                )
                cursor.execute(query, values)


def insert_part_comments(cursor, data):
    """Insert part comments into the part_comments table"""
    for comment in data:
        ticket_id = comment["ticket_id"]
        comment_id = comment["comment_id"]
        user_id = comment["user_id"]
        body = comment["body"]
        created_at = comment["created_at"]

        # Extract part numbers and notes from the comment body
        match = re.search(r'\[(p|P):(.*?)\](.*)', body, re.IGNORECASE | re.DOTALL)
        if not match:
            # Skip comments that don't match the specific format
            continue

        part_numbers_str = match.group(2).strip()
        part_numbers = re.findall(r'\b\w+\b', part_numbers_str)
        parts = [p.strip() for p in part_numbers]
        notes = match.group(3).strip()

        query = """
        INSERT INTO part_comments (ticket_id, comment_id, employee_id, parts, notes, datetime)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ticket_id = VALUES(ticket_id),
            employee_id = VALUES(employee_id),
            parts = VALUES(parts),
            notes = VALUES(notes),
            datetime = VALUES(datetime)
        """
        values = (ticket_id, comment_id, user_id, json.dumps(parts), notes, created_at)
        cursor.execute(query, values)
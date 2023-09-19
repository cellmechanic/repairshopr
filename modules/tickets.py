"""ticket with parameterized lookback"""
import time
from library import env_library
from library.api_requests import get_tickets
from library.db_create import (
    create_comments_table_if_not_exists,
    create_tickets_table_if_not_exists,
)
from library.db_delete import (
    move_deleted_comments_to_deleted_table,
    move_deleted_tickets_to_deleted_table,
)
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_comments, insert_tickets
from library.timestamp_files import check_last_ran, update_last_ran


def tickets(logger, full_run=False, lookback_days=14):
    """main script for the ticket module"""
    # Load timestamp
    timestamp_folder = "last-runs"
    timestamp_file = f"{timestamp_folder}/last_run_tickets_days.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_tickets_table_if_not_exists(cursor)
    create_comments_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    all_data = []

    if not full_run:
        # Get 1st Page, then check to make sure not null
        data = get_tickets(logger, current_page, lookback_days)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting ticket data",
                extra={"tags": {"service": "tickets"}},
            )

        logger.info(
            "Total pages: %s",
            total_pages,
            extra={"tags": {"service": "tickets"}},
        )

        # total_pages + 1
        for page in range(1, total_pages + 1):
            data = get_tickets(logger, page, lookback_days)
            if data is not None:
                all_data.extend(data["tickets"])
                logger.info(
                    "Added in page # %s",
                    page,
                    extra={"tags": {"service": "tickets"}},
                )
            else:
                logger.error(
                    "Error getting ticket data",
                    extra={"tags": {"service": "tickets"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Total tickets: %s",
            len(all_data),
            extra={"tags": {"service": "tickets"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "tickets"}},
        )

        insert_tickets(logger, cursor, all_data, last_run_timestamp_unix)
        insert_comments(logger, cursor, all_data, last_run_timestamp_unix)

    if full_run:
        # Get 1st Page, then check to make sure not null
        data = get_tickets(current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting ticket data",
                extra={"tags": {"service": "tickets"}},
            )

        for page in range(1, total_pages + 1):
            data = get_tickets(page)
            if data is not None:
                all_data.extend(data["tickets"])
                logger.info(
                    "Added in page # %s",
                    page,
                    extra={"tags": {"service": "tickets"}},
                )
            else:
                logger.error(
                    "Error getting ticket data",
                    extra={"tags": {"service": "tickets"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Total pages: %s",
            total_pages,
            extra={"tags": {"service": "tickets"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "tickets"}},
        )

        insert_tickets(logger, cursor, all_data, last_run_timestamp_unix)
        comments_data = insert_comments(
            logger, cursor, all_data, last_run_timestamp_unix
        )

        # Check ID sums to see if any comment was deleted
        deleted = compare_id_sums(logger, cursor, comments_data, "comments")

        if not deleted:
            move_deleted_comments_to_deleted_table(logger, cursor, connection, all_data)

        # Validate data / totals
        query = "SELECT COUNT(*) FROM comments"
        cursor.execute(query)
        db_rows = cursor.fetchone()
        if db_rows is not None:
            db_rows = db_rows[0]
        else:
            db_rows = 0

        # Check if the API and DB totals match
        if db_rows == len(comments_data):
            logger.info(
                "ALL Good -- Ticket Comments API Rows: %s, DB Rows: %s",
                len(comments_data),
                db_rows,
                extra={"tags": {"service": "tickets", "finished": "full"}},
            )
        else:
            logger.error(
                "Ticket Comments API Rows: %s, DB Rows: %s",
                len(comments_data),
                db_rows,
                extra={"tags": {"service": "tickets"}},
            )

        # Again for tickets
        deleted = compare_id_sums(logger, cursor, all_data, "tickets")

        if not deleted:
            move_deleted_tickets_to_deleted_table(logger, cursor, connection, all_data)

        # Validate data / totals
        query = "SELECT COUNT(*) FROM tickets"
        cursor.execute(query)
        db_rows = cursor.fetchone()
        if db_rows is not None:
            db_rows = db_rows[0]
        else:
            db_rows = 0

        # Check if the API and DB totals match
        if db_rows == len(all_data):
            logger.info(
                "ALL Good -- Ticket API Rows: %s, DB Rows: %s",
                len(all_data),
                db_rows,
                extra={"tags": {"service": "tickets", "finished": "full"}},
            )
        else:
            logger.error(
                "Ticket API Rows: %s, DB Rows: %s",
                len(all_data),
                db_rows,
                extra={"tags": {"service": "tickets"}},
            )

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

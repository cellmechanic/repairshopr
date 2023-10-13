"""ticket with parameterized lookback"""
import time
from library import env_library
from library.api_requests import get_date_for_header, get_tickets
from library.db_create import (
    create_comments_table_if_not_exists,
    create_tickets_table_if_not_exists,
)
from library.db_delete import (
    move_deleted_comments_to_deleted_table,
    move_deleted_comments_to_deleted_table_frequent_only,
    move_deleted_tickets_to_deleted_table,
)
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_comments, insert_tickets


def tickets(logger, full_run=False, lookback_days=14):
    """main script for the ticket module"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_tickets_table_if_not_exists(cursor)
    create_comments_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    api_page = 1
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
                api_page = page
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

        insert_tickets(logger, cursor, all_data)
        comments = insert_comments(logger, cursor, all_data)
        if api_page == total_pages:
            all_sourced = True
        else:
            all_sourced = False
        if all_sourced:
            in_range_comments = []
            for comment in comments:
                if comment["created_at"] >= get_date_for_header(lookback_days):
                    in_range_comments.append(comment)
            # Check ID sums to see if any comment was deleted
            # Fetch the column names from the cursor description
            query = "SELECT * from comments WHERE created_at >= %s"
            cursor.execute(query, (get_date_for_header(lookback_days),))
            db_comments = cursor.fetchall()
            if db_comments and cursor.description:
                row = cursor.fetchone()
                column_names = [desc[0] for desc in cursor.description]
                db_comments_data = [
                    dict(zip(column_names, comment)) for comment in db_comments
                ]

                query = "SELECT SUM(id) FROM comments WHERE created_at >= %s"
                cursor.execute(query, (get_date_for_header(lookback_days),))
                cursor.fetchall()
                if row:
                    db_id_sum = row[0]
                else:
                    db_id_sum = 0
                # Extract the "id" values and filter out non-integer values

                api_sum = sum(comment["id"] for comment in in_range_comments)
                if api_sum != db_id_sum:
                    move_deleted_comments_to_deleted_table_frequent_only(
                        logger, cursor, connection, in_range_comments, db_comments_data
                    )
                # Validate data / totals
                query = "SELECT COUNT(*) FROM tickets WHERE updated_at >= %s"
                cursor.execute(query, (get_date_for_header(lookback_days),))
                db_rows = cursor.fetchone()
                cursor.fetchall()
                if db_rows is not None:
                    db_rows = db_rows[0]
                else:
                    db_rows = 0
                # Check if the API and DB totals match
                if db_rows != len(all_data):
                    logger.error(
                        "Data Mismatch -- Ticket API Rows: %s, DB Rows: %s",
                        len(all_data),
                        db_rows,
                        extra={"tags": {"service": "tickets", "finished": "yes"}},
                    )

    if full_run:
        # Get 1st Page, then check to make sure not null
        data = get_tickets(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting ticket data",
                extra={"tags": {"service": "tickets"}},
            )

        for page in range(1, total_pages + 1):
            data = get_tickets(logger, page)
            if data is not None:
                all_data.extend(data["tickets"])
                api_page = page
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

        insert_tickets(logger, cursor, all_data)
        comments_data = insert_comments(logger, cursor, all_data)
        if api_page == total_pages:
            all_sourced = True
        else:
            all_sourced = False
        if all_sourced:
            # Check ID sums to see if any comment was deleted
            deleted = compare_id_sums(logger, cursor, comments_data, "comments")
            if not deleted:
                move_deleted_comments_to_deleted_table(
                    logger, cursor, connection, all_data
                )
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
                    "Data Mismatch -- Ticket Comments API Rows: %s, DB Rows: %s",
                    len(comments_data),
                    db_rows,
                    extra={"tags": {"service": "tickets"}},
                )
            # Again for tickets
            deleted = compare_id_sums(logger, cursor, all_data, "tickets")
            if not deleted:
                move_deleted_tickets_to_deleted_table(
                    logger, cursor, connection, all_data
                )
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
                    "Data Mismatch -- Ticket API Rows: %s, DB Rows: %s",
                    len(all_data),
                    db_rows,
                    extra={"tags": {"service": "tickets", "finished": "full"}},
                )
        elif not all_sourced:
            logger.error(
                "Can't check for deletes, problem with ticket / ticket comment API data",
                extra={"tags": {"service": "tickets", "finished": "full"}},
            )

    connection.commit()
    connection.close()

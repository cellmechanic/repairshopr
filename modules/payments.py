"""module to get payments data"""

import time
from library import env_library
from library.api_requests import get_date_for_header, get_payments
from library.db_create import create_payments_table_if_not_exists
from library.db_delete import move_deleted_payments_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_payments
from library.timestamp_files import check_last_ran, update_last_ran


def payments(logger, full_run=False, lookback_days=14):
    """main function to get payments data"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_payments_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    all_data = []

    # Load timestamp
    timestamp_folder = "last-runs"
    timestamp_file = f"{timestamp_folder}/last_run_payments.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    if not full_run:
        # Get 1st Page, then check to make sure not null
        data = get_payments(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting payment data",
                extra={"tags": {"service": "payments"}},
            )

        lookback_date_formatted = get_date_for_header(lookback_days)
        found_page = 0
        found_older = False

        for page in range(1, total_pages + 1):
            if found_older:
                break
            data = get_payments(logger, page)
            if data is not None and "payments" in data:
                found_older = any(
                    item["created_at"] < lookback_date_formatted
                    for item in data["payments"]
                )
                if not found_older:
                    break

                all_data.extend(data["payments"])
                logger.info(
                    "Added in page # %s", page, extra={"tags": {"service": "payments"}}
                )
                found_page = page
            else:
                logger.error(
                    "Error getting payment data",
                    extra={"tags": {"service": "payments"}},
                )
                break
            time.sleep(rate_limit())

            logger.info(
                "Adding in : %s / %s",
                found_page,
                total_pages,
                extra={"tags": {"service": "payments"}},
            )
            logger.info(
                "Number of entries to consider for DB: %s",
                len(all_data),
                extra={"tags": {"service": "payments"}},
            )

        insert_payments(logger, cursor, all_data, last_run_timestamp_unix)

    if full_run:
        data = get_payments(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting payment data",
                extra={"tags": {"service": "payments"}},
            )

        for page in range(1, total_pages + 1):
            data = get_payments(logger, page)
            if data is not None:
                all_data.extend(data["payments"])
                logger.info(
                    "Added in page # %s", page, extra={"tags": {"service": "payments"}}
                )
            else:
                logger.error(
                    "Error getting payment data",
                    extra={"tags": {"service": "payments"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Received all data, %s page(s)",
            total_pages,
            extra={"tags": {"service": "payments"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "payments"}},
        )

        insert_payments(logger, cursor, all_data, last_run_timestamp_unix)

        deleted = compare_id_sums(logger, cursor, all_data, "payments")
        if not deleted:
            move_deleted_payments_to_deleted_table(logger, cursor, connection, all_data)

        # Validate data / totals
        query = "SELECT COUNT(*) FROM payments"
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            db_rows = result[0]
        else:
            db_rows = 0

        # Check if the total entries match the expected count
        if db_rows == len(all_data):
            logger.info(
                "All Good -- Payment API Rows: %s, DB Rows: %s",
                len(all_data),
                db_rows,
                extra={"tags": {"service": "payments", "finished": "full"}},
            )
        else:
            logger.error(
                "Data mismatch -- Payment Meta Rows: %s, DB Rows: %s",
                len(all_data),
                db_rows,
                extra={"tags": {"service": "payments"}},
            )

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

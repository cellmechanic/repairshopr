"""module to get payments data"""

import time
from library import env_library
from library.api_requests_library import get_date_for_header, get_payments
from library.db_create import create_payments_table_if_not_exists
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_payments
from library.loki_library import start_loki
from library.timestamp_files import check_last_ran, update_last_ran


def payments(full_run=False, lookback_days=14):
    """main function to get payments data"""

    logger = start_loki("__payments__")

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
        data = get_payments(current_page)
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
            data = get_payments(page)
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

        insert_payments(cursor, all_data, last_run_timestamp_unix)

    if full_run:
        data = get_payments(current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting payment data",
                extra={"tags": {"service": "payments"}},
            )

        for page in range(1, total_pages + 1):
            data = get_payments(page)
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

        insert_payments(cursor, all_data, last_run_timestamp_unix)

        deleted = compare_id_sums(cursor, all_data, "payments")
        # if not deleted:
        # move_deleted_payments_to_deleted_table(cursor, connection, all_data)

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

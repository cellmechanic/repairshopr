""" getting invoices """
import time

from library import env_library
from library.api_requests import (
    get_invoices,
    get_date_for_header,
)
from library.db_create import create_invoices_table_if_not_exists
from library.db_delete import move_deleted_invoices_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_invoices


def invoices(logger, full_run=False, lookback_days=14):
    """Get invoices from the API and insert into the database"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_invoices_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    api_page = 1
    all_data = []

    if not full_run:
        # Get 1st Page, then check to make sure not null
        lookback_date = get_date_for_header(lookback_days)
        data = get_invoices(logger, current_page, lookback_date)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting invoice data",
                extra={"tags": {"service": "invoices"}},
            )

        # total_pages + 1
        for page in range(1, 5): #changed for testing total_pages + 1
            data = get_invoices(logger, page, lookback_date)
            if data is not None:
                all_data.extend(data["invoices"])
                logger.info(
                    "Added in page # %s",
                    page,
                    extra={"tags": {"service": "invoices"}},
                )
            else:
                logger.error(
                    "Error getting invoice data",
                    extra={"tags": {"service": "invoices"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Adding in : %s",
            total_pages,
            extra={"tags": {"service": "invoices"}},
        )

        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "invoices"}},
        )
        insert_invoices(logger, cursor, all_data)

    if full_run:
        # Get 1st Page, then check to make sure not null
        data = get_invoices(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting invoice data",
                extra={"tags": {"service": "invoices"}},
            )
        logger.info(
            "Total Pages: %s",
            total_pages,
            extra={"tags": {"service": "invoices"}},
        )
        # total_pages + 1
        for page in range(1, total_pages + 1):
            data = get_invoices(logger, page)
            api_page = page
            if data is not None:
                all_data.extend(data["invoices"])
                logger.info(
                    "Added in page # %s",
                    page,
                    extra={"tags": {"service": "invoices"}},
                )
            else:
                logger.error(
                    "Error getting invoice data",
                    extra={"tags": {"service": "invoices"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "invoices"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "invoices"}},
        )
        insert_invoices(logger, cursor, all_data)
        if api_page == total_pages:
            all_sourced = True
        else:
            all_sourced = False
        if all_sourced:
            deleted = compare_id_sums(logger, cursor, all_data, "invoices")
            if not deleted:
                move_deleted_invoices_to_deleted_table(logger, cursor, connection, all_data)
            # Validate data / totals
            query = "SELECT COUNT(*) FROM invoices"
            cursor.execute(query)
            result = cursor.fetchone()
            if result is not None:
                db_rows = result[0]
            else:
                db_rows = 0
            # Check if the total entries match the expected count
            if db_rows == len(all_data):
                logger.info(
                    "All Good -- Invoice API Rows: %s, DB Rows: %s",
                    len(all_data),
                    db_rows,
                    extra={"tags": {"service": "invoices", "finished": "full"}},
                )
            else:
                logger.error(
                    "Data mismatch -- Invoice API Rows: %s, DB Rows: %s",
                    len(all_data),
                    db_rows,
                    extra={"tags": {"service": "invoices", "finished": "full"}},
                )
        elif not all_sourced:
            logger.error(
                "Can't check for deletes, problem with invoice API data",
                extra={"tags": {"service": "invoices", "finished": "full"}},
            )

    connection.commit()
    connection.close()

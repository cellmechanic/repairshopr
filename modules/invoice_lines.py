"""getting RS invoice line items"""
import time
from library.db_create import create_invoice_items_table_if_not_exists
from library.db_delete import move_deleted_lines_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_invoice_lines
import library.env_library as env_library
from library.api_requests_library import (
    get_invoice_lines,
)
from library.fix_date_time_library import rs_to_unix_timestamp
from library.timestamp_files import update_last_ran, check_last_ran


def invoice_lines(logger, full_run=False):
    """main script for the invoice lines module"""

    # Load timestamp
    timestamp_folder = "last-runs"
    timestamp_file = f"{timestamp_folder}/last_run_invoice_lines_updates.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_invoice_items_table_if_not_exists(cursor)

    # Start fetching line items from the end
    total_pages = 0
    current_page = 1
    all_data = []
    found_last_updated_row = False

    if not full_run:
        # Get 1st Page, then check to make sure not null
        data = get_invoice_lines(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
            current_page = total_pages
        else:
            logger.error(
                "Error getting invoice line item data",
                extra={"tags": {"service": "invoice_lines"}},
            )
        logger.info(
            "Total pages: %s",
            total_pages,
            extra={"tags": {"service": "invoice_lines"}},
        )

        # work backwards from last page
        for page in range(total_pages, 0, -1):
            data = get_invoice_lines(logger, page)
            if data is not None:
                if (
                    rs_to_unix_timestamp(data["line_items"][-1]["updated_at"])
                    < last_run_timestamp_unix
                ):
                    found_last_updated_row = True
                    logger.info(
                        "Found last updated row in page %s",
                        page,
                        extra={"tags": {"service": "invoice_lines"}},
                    )
                    break

                all_data.extend(data["line_items"])
                logger.info(
                    "Added in page # %s",
                    page,
                    extra={"tags": {"service": "invoice_lines"}},
                )
            else:
                logger.error(
                    "Error getting invoice line item data",
                    extra={"tags": {"service": "invoice_lines"}},
                )
                break
            time.sleep(rate_limit())

        if not found_last_updated_row:
            logger.info(
                "No invoice lines updated since last run",
                extra={"tags": {"service": "invoice_lines", "updates": "yes"}},
            )

        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "invoice_lines"}},
        )
        insert_invoice_lines(logger, cursor, all_data, last_run_timestamp_unix)

    if full_run:
        data = get_invoice_lines(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting invoice line item data",
                extra={"tags": {"service": "invoice_lines"}},
            )

        for page in range(1, total_pages + 1):
            data = get_invoice_lines(logger, page)
            if data is not None:
                all_data.extend(data["line_items"])
                logger.info(
                    "Added in page # %s",
                    page,
                    extra={"tags": {"service": "invoice_lines"}},
                )
            else:
                logger.error(
                    "Error getting invoice line item data",
                    extra={"tags": {"service": "invoice_lines"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Received all data, %s page(s)",
            total_pages,
            extra={"tags": {"service": "invoice_lines"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "invoice_lines"}},
        )

        insert_invoice_lines(logger, cursor, all_data, last_run_timestamp_unix)

        deleted = compare_id_sums(logger, cursor, all_data, "invoice_items")

        if not deleted:
            move_deleted_lines_to_deleted_table(logger, cursor, connection, all_data)

        # Validate data / totals
        query = "SELECT COUNT(*) FROM invoice_items"
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            db_rows = result[0]
        else:
            db_rows = 0

        # Check if the total entries match the expected count
        if db_rows == len(all_data):
            logger.info(
                "All Good -- Invoice Line API Rows: %s, DB Rows %s",
                len(all_data),
                db_rows,
                extra={"tags": {"service": "invoice_lines", "finished": "full"}},
            )

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

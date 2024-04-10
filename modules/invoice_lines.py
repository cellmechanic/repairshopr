"""getting RS invoice line items"""
import time
from library.db_create import create_invoice_items_table_if_not_exists
from library.db_delete import move_deleted_lines_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_invoice_lines
import library.env_library as env_library
from library.api_requests import (
    get_invoice_lines,
)
from library.fix_date_time import rs_to_unix_timestamp


def invoice_lines(logger, full_run=False):
    """main script for the invoice lines module"""
    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_invoice_items_table_if_not_exists(cursor)

    # Start fetching line items from the end
    total_pages = 0
    current_page = 1
    api_page = 1
    all_data = []
    found_last_updated_row = False

    if not full_run:
        # Get invoice lines
        cursor.execute(
            "SELECT MAX(updated_at) FROM invoice_items WHERE invoice_id IS NOT NULL"
        )
        result = cursor.fetchone()
        if result:
            most_recent_update = result[0]
        else:
            most_recent_update = 0
        data = get_invoice_lines(logger, current_page, "invoice")
        if data:
            total_pages = data["meta"]["total_pages"]
            current_page = total_pages
        else:
            logger.error(
                "Error getting invoice line item data",
                extra={"tags": {"service": "invoice_lines"}},
            )

        # work backwards from last page
        for page in range(total_pages, 0, -1):
            data = get_invoice_lines(logger, page, "invoice")
            if data:
                for line_item in reversed(data["line_items"]):
                    if rs_to_unix_timestamp(
                        line_item["updated_at"]
                    ) < rs_to_unix_timestamp(most_recent_update):
                        found_last_updated_row = True
                        logger.info(
                            "Found last updated row in invoice lines page %s",
                            page,
                            extra={"tags": {"service": "invoice_lines"}},
                        )
                        break
                    else:
                        all_data.append(line_item)

                if found_last_updated_row:
                    break

                logger.info(
                    "Added in invoice line page # %s out of %s",
                    page,
                    total_pages,
                    extra={"tags": {"service": "invoice_lines"}},
                )
            else:
                logger.error(
                    "Error getting invoice line item data",
                    extra={"tags": {"service": "invoice_lines"}},
                )
                break
            # time.sleep(rate_limit())

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
        insert_invoice_lines(logger, cursor, all_data)

        # Clear out data and flags before moving to estimate lines
        all_data = []
        found_last_updated_row = False
        most_recent_update = 0

        # Get estimate lines
        cursor.execute(
            "SELECT MAX(updated_at) FROM invoice_items WHERE invoice_id IS NULL"
        )
        result = cursor.fetchone()
        if result:
            most_recent_update = result[0]
        else:
            most_recent_update = 0
        data = get_invoice_lines(logger, current_page, "estimate")
        if data:
            total_pages = data["meta"]["total_pages"]
            current_page = total_pages
        else:
            logger.error(
                "Error getting estimate line item data",
                extra={"tags": {"service": "invoice_lines"}},
            )

        # work backwards from last page
        for page in range(total_pages, 0, -1):
            data = get_invoice_lines(logger, page, "estimate")
            if data:
                for line_item in reversed(data["line_items"]):
                    if rs_to_unix_timestamp(
                        line_item["updated_at"]
                    ) < rs_to_unix_timestamp(most_recent_update):
                        found_last_updated_row = True
                        logger.info(
                            "Found last updated estimate row in page %s",
                            page,
                            extra={"tags": {"service": "invoice_lines"}},
                        )
                        break
                    else:
                        all_data.append(line_item)

                if found_last_updated_row:
                    break

                logger.info(
                    "Added in estimate lines page # %s out of %s",
                    page,
                    total_pages,
                    extra={"tags": {"service": "invoice_lines"}},
                )
            else:
                logger.error(
                    "Error getting estimate line item data",
                    extra={"tags": {"service": "invoice_lines"}},
                )
                break
            time.sleep(rate_limit())

        if not found_last_updated_row:
            logger.info(
                "No estimate lines updated since last run",
                extra={"tags": {"service": "invoice_lines", "updates": "yes"}},
            )

        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "invoice_lines"}},
        )
        insert_invoice_lines(logger, cursor, all_data)

    if full_run:
        data = get_invoice_lines(logger, current_page, "invoice")
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting invoice line item data",
                extra={"tags": {"service": "invoice_lines"}},
            )

        for page in range(1, total_pages + 1):
            data = get_invoice_lines(logger, page, "invoice")
            if data is not None:
                all_data.extend(data["line_items"])
                api_page = page
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

        insert_invoice_lines(logger, cursor, all_data)
        if api_page == total_pages:
            all_sourced = True
        else:
            all_sourced = False
        if all_sourced:
            deleted = compare_id_sums(logger, cursor, all_data, "invoice_items")
            if not deleted:
                move_deleted_lines_to_deleted_table(
                    logger, cursor, connection, all_data
                )
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
            else:
                logger.error(
                    "Data Mismatch -- API Rows: %s, DB Rows: %s",
                    len(all_data),
                    db_rows,
                    extra={"tags": {"service": "invoice_lines", "finished": "full"}},
                )
        elif not all_sourced:
            logger.error(
                "Can't check for deletes, problem with invoice lines API data",
                extra={"tags": {"service": "invoice_lines", "finished": "full"}},
            )

    connection.commit()
    connection.close()

"""getting RS invoice line items"""
import time
import library.env_library as env_library
from library.fix_date_time_library import log_ts
from library.db_requests_library import (
    create_invoice_items_table_if_not_exists,
    insert_invoice_lines,
    rs_to_unix_timestamp,
    rate_limit,
)
from library.api_requests_library import (
    get_invoice_lines,
)
from library.loki_library import start_loki
from library.timestamp_files import update_last_ran, check_last_ran


def invoice_lines_update():
    """main script for the invoice lines module"""

    logger = start_loki("__invoice_lines__")

    # Load timestamp
    timestamp_folder = "last runs"
    timestamp_file = f"{timestamp_folder}last_run_invoice_lines_updates.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    # Database configurations
    config = env_library.config
    connection = None
    cursor, connection = create_invoice_items_table_if_not_exists(config)

    # Start fetching line items from the end
    total_pages = 0
    current_page = 1
    all_data = []
    found_last_updated_row = False

    # Get 1st Page, then check to make sure not null
    data = get_invoice_lines(current_page)
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
        data = get_invoice_lines(page)
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
        print(f"{log_ts()} No updates since last run")
        logger.info(
            "No invoice lines updated since last run",
            extra={"tags": {"service": "invoice_lines", "updates": "yes"}},
        )

    logger.info(
        "Number of entries to consider for DB: %s",
        len(all_data),
        extra={"tags": {"service": "invoice_lines"}},
    )
    insert_invoice_lines(cursor, all_data, last_run_timestamp_unix)
    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

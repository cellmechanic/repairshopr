"""Customer backup module"""
import time
from library import env_library
from library.api_requests_library import get_customers
from library.db_create import create_customer_table_if_not_exists
from library.db_delete import move_deleted_customers_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_customers
from library.loki_library import start_loki
from library.timestamp_files import check_last_ran, update_last_ran


def customers():
    """main script for the customer module"""

    logger = start_loki("__customers__")

    # Load timestamp
    timestamp_folder = "last-runs"
    timestamp_file = f"{timestamp_folder}/last_run_customers.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_customer_table_if_not_exists(cursor)

    # Start fetching contacts from page 1
    current_page = 1
    total_pages = 0
    total_entries = 0
    db_rows = 0
    all_data = []

    # Get 1st Page, then check to make sure not null
    data = get_customers(current_page)
    if data is not None:
        total_pages = data["meta"]["total_pages"]
        total_entries = data["meta"]["total_entries"]
    else:
        logger.error(
            "Error getting contact data",
            extra={"tags": {"service": "contacts"}},
        )

    # Iterate through all the pages
    for page in range(1, total_pages + 1):
        data = get_customers(page)
        if data is not None:
            all_data.extend(data["customers"])
            logger.info(
                "Added in page # %s",
                page,
                extra={"tags": {"service": "contacts"}},
            )
        else:
            logger.error(
                "Error getting contact data",
                extra={"tags": {"service": "contacts"}},
            )
            break
        time.sleep(rate_limit())

    logger.info(
        "Received all data, %s page(s)",
        total_pages,
        extra={"tags": {"service": "contacts"}},
    )
    logger.info(
        "Total rows in all_data: %s",
        len(all_data),
        extra={"tags": {"service": "contacts"}},
    )

    insert_customers(cursor, all_data, last_run_timestamp_unix)

    # Check ID sums to see if anything was deleted
    deleted = compare_id_sums(cursor, all_data, "customers")

    if not deleted:
        move_deleted_customers_to_deleted_table(cursor, connection, all_data)

    # Validate data / totals
    query = "SELECT COUNT(*) FROM customers"
    cursor.execute(query)
    result = cursor.fetchone()
    if result is not None:
        db_rows = result[0]

    # Check if the total entries match the expected count
    if db_rows == total_entries:
        logger.info(
            "All Good -- Customer API Rows: %s, DB Rows: %s",
            total_entries,
            db_rows,
            extra={"tags": {"service": "contacts", "finished": "full"}},
        )
    else:
        logger.error(
            "Row Mismatch",
            extra={"tags": {"service": "contacts"}},
        )

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

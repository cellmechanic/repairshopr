"""Customer backup module"""
import time
from library import env_library
from library.api_requests import get_customers
from library.db_create import create_customer_table_if_not_exists
from library.db_delete import move_deleted_customers_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_customers


def customers(logger, full_run=False):
    """main script for the customer module"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_customer_table_if_not_exists(cursor)

    # Start fetching customers from page 1
    current_page = 1
    total_pages = 0
    total_entries = 0
    db_rows = 0
    all_data = []

    # Get 1st Page, then check to make sure not null
    data = get_customers(logger, current_page)
    if data is not None:
        total_pages = data["meta"]["total_pages"]
        total_entries = data["meta"]["total_entries"]
    else:
        logger.error(
            "Error getting customer data",
            extra={"tags": {"service": "customers"}},
        )

    # Iterate through all the pages
    for page in range(1, total_pages + 1):
        data = get_customers(logger, page)
        if data is not None:
            all_data.extend(data["customers"])
            logger.info(
                "Added in page # %s",
                page,
                extra={"tags": {"service": "customers"}},
            )
        else:
            logger.error(
                "Error getting contact data",
                extra={"tags": {"service": "customers"}},
            )
            break
        time.sleep(rate_limit())

    logger.info(
        "Received all data, %s page(s)",
        total_pages,
        extra={"tags": {"service": "customers"}},
    )
    logger.info(
        "Total rows in all_data: %s",
        len(all_data),
        extra={"tags": {"service": "customers"}},
    )

    insert_customers(logger, cursor, all_data)
    if len(all_data) == total_entries:
        all_sourced = True
    else:
        all_sourced = False
    if all_sourced:
        deleted = compare_id_sums(logger, cursor, all_data, "customers")
        if not deleted:
            move_deleted_customers_to_deleted_table(logger, cursor, connection, all_data)
        # Validate data / totals
        query = "SELECT COUNT(*) FROM customers"
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            db_rows = result[0]
        # Check if the total entries match the expected count
        if db_rows == total_entries and full_run:
            logger.info(
                "All Good -- Customer API Rows: %s, DB Rows: %s",
                total_entries,
                db_rows,
                extra={"tags": {"service": "customers", "finished": "full"}},
            )
        if db_rows != total_entries and full_run:
            logger.error(
                "Data Mismatch -- Customer API Rows: %s, DB Rows: %s",
                total_entries,
                db_rows,
                extra={"tags": {"service": "customers", "finished": "full"}},
            )
    elif not all_sourced:
        logger.error(
            "Can't check for deletes, problem with customers API data",
            extra={"tags": {"service": "customers", "finished": "full"}},
        )


    connection.commit()
    connection.close()

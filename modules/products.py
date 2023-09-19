"""products module"""

import time
from library import env_library
from library.api_requests_library import get_products
from library.db_create import create_products_table_if_not_exists
from library.db_delete import move_deleted_products_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_products


def products(logger, full_run=False):
    """main script for the products module"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_products_table_if_not_exists(cursor)

    # Start fetching products from page 1
    current_page = 1
    total_pages = 0
    total_entries = 0
    db_rows = 0
    all_data = []

    # Get 1st Page, then check to make sure not null
    data = get_products(logger, current_page)
    if data is not None:
        total_pages = data["meta"]["total_pages"]
        total_entries = data["meta"]["total_entries"]
    else:
        logger.error(
            "Error getting product data",
            extra={"tags": {"service": "products"}},
        )

    # Iterate through all the pages
    for page in range(1, total_pages + 1):
        data = get_products(logger, page)
        if data is not None:
            all_data.extend(data["products"])
            logger.info(
                "Added in page # %s",
                page,
                extra={"tags": {"service": "products"}},
            )
        else:
            logger.error(
                "Error getting product data",
                extra={"tags": {"service": "products"}},
            )
            break
        time.sleep(rate_limit())

    logger.info(
        "Received all data, %s page(s)",
        total_pages,
        extra={"tags": {"service": "products"}},
    )

    logger.info(
        "Total rows in all_data: %s",
        len(all_data),
        extra={"tags": {"service": "products"}},
    )

    insert_products(logger, cursor, all_data)

    # Check ID sums to see if anything was deleted
    deleted = compare_id_sums(logger, cursor, all_data, "products")
    if not deleted:
        move_deleted_products_to_deleted_table(logger, cursor, connection, all_data)

    # Validate data / totals
    query = "SELECT COUNT(*) FROM products"
    cursor.execute(query)
    result = cursor.fetchone()
    if result is not None:
        db_rows = result[0]

    # Check if total entries match the expected count
    if db_rows == total_entries and full_run:
        logger.info(
            "All Good -- Product API Rows: %s, DB Rows: %s",
            total_entries,
            db_rows,
            extra={"tags": {"service": "products", "finished": "full"}},
        )
    elif db_rows != total_entries:
        logger.error(
            "Product Meta Rows: %s, DB Rows: %s",
            total_entries,
            db_rows,
            extra={"tags": {"service": "products"}},
        )

    connection.commit()
    connection.close()

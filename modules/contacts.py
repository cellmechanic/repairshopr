"""Getting RS Contacts"""
import time
from library.db_create import create_contact_table_if_not_exists
from library.db_delete import move_deleted_contacts_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_contacts
import library.env_library as env_library
from library.api_requests import (
    get_contacts,
)

def contacts(logger, full_run=False):
    """main script for the contact module"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_contact_table_if_not_exists(cursor)

    # Start fetching contacts from page 1
    current_page = 1
    total_pages = 0
    total_entries = 0
    db_rows = 0
    all_data = []

    # Get 1st Page, then check to make sure not null
    data = get_contacts(logger, current_page)
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
        data = get_contacts(logger, page)
        if data is not None:
            all_data.extend(data["contacts"])
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

    insert_contacts(logger, cursor, all_data)

    if len(all_data) == total_entries:
        all_sourced = True
    else:
        all_sourced = False

    # Check ID sums to see if anything was deleted
    if all_sourced:
        deleted = compare_id_sums(logger, cursor, all_data, "contacts")
        if not deleted:
            move_deleted_contacts_to_deleted_table(logger, cursor, connection, all_data)
        # Validate data / totals
        query = "SELECT COUNT(*) FROM contacts"
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            db_rows = result[0]
        else:
            db_rows = 0

        # Check if the total entries match the expected count
        if db_rows == total_entries and full_run:
            logger.info(
                "All Good -- Contact API Rows: %s, DB Rows: %s",
                total_entries,
                db_rows,
                extra={"tags": {"service": "contacts", "finished": "full"}},
            )
    elif not all_sourced:
        logger.error(
            "Can't check for deletes, problem with contacts API data",
            extra={"tags": {"service": "contacts"}},
        )

    connection.commit()
    connection.close()

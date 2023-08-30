"""Getting RS Contacts"""
import time
import library.env_library as env_library
from library.fix_date_time_library import log_ts
from library.api_requests_library import (
    get_contacts,
)
from library.db_requests_library import (
    create_contact_table_if_not_exists,
    connect_to_db,
    compare_id_sums,
    move_deleted_contacts_to_deleted_table,
    rate_limit,
    insert_contacts,
)
from library.timestamp_files import update_last_ran, check_last_ran
from library.loki_library import start_loki


def contacts():
    """main script for the contact module"""
    logger = start_loki("__contacts__")

    # Load timestamp
    timestamp_folder = "last runs"
    timestamp_file = f"{timestamp_folder}/last_run_contacts.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    # Database configurations
    config = env_library.config
    connection = None
    cursor, connection = connect_to_db(config)
    create_contact_table_if_not_exists(cursor)

    # Start fetching contacts from page 1
    current_page = 1
    total_pages = 0
    total_entries = 0
    db_rows = 0
    all_data = []

    # Get 1st Page, then check to make sure not null
    data = get_contacts(current_page)
    if data is not None:
        total_pages = data["meta"]["total_pages"]
        total_entries = data["meta"]["total_entries"]
    else:
        logger.error(
            "%s Error getting contact data",
            log_ts(),
            extra={"tags": {"service": "contacts"}},
        )

    # Iterate through all the pages
    for page in range(1, total_pages + 1):
        data = get_contacts(page)
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

    # Check ID sums to see if anything was deleted
    deleted = compare_id_sums(cursor, all_data, "contacts")
    if not deleted:
        move_deleted_contacts_to_deleted_table(cursor, connection, all_data)

    insert_contacts(cursor, all_data, last_run_timestamp_unix)

    # Validate data / totals
    query = "SELECT COUNT(*) FROM contacts"
    cursor.execute(query)
    result = cursor.fetchone()
    if result is not None:
        db_rows = result[0]

    # Check if the total entries match the expected count
    if db_rows == total_entries:
        logger.info(
            "Meta Rows: %s",
            total_entries,
            extra={"tags": {"service": "contacts"}},
        )
        logger.info(
            "Row Count from DB is: %s",
            db_rows,
            extra={"tags": {"service": "contacts"}},
        )
    else:
        logger.error(
            "Row Mismatch",
            extra={"tags": {"service": "contacts"}},
        )

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)

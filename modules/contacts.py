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
    logger = start_loki("__contacts__")

    # Load timestamp
    TIMESTAMP_FILE = "last_run_contacts.txt"
    last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)

    # Database configurations
    config = env_library.config
    CONNECTION = None
    cursor, CONNECTION = connect_to_db(config)
    create_contact_table_if_not_exists(cursor)

    # Fetch data from the API
    headers = {"Authorization": f"Bearer {env_library.api_key_contact}"}

    # Start fetching contacts from page 1
    CURRENT_PAGE = 1
    TOTAL_PAGES = 0
    TOTAL_ENTRIES = 0
    DB_ROWS = 0
    ALL_DATA = []


    # Get 1st Page, then check to make sure not null
    data = get_contacts(CURRENT_PAGE)
    if data is not None:
        TOTAL_PAGES = data["meta"]["total_pages"]
        TOTAL_ENTRIES = data["meta"]["total_entries"]
    else:
        logger.error(
            "%s Error getting contact data",
            log_ts(),
            extra={"tags": {"service": "contacts"}},
        )

    # Iterate through all the pages
    for page in range(1, TOTAL_PAGES + 1):
        data = get_contacts(page)
        if data is not None:
            ALL_DATA.extend(data["contacts"])
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
        TOTAL_PAGES,
        extra={"tags": {"service": "contacts"}},
    )

    logger.info(
        "Total rows in ALL_DATA: %s",
        len(ALL_DATA),
        extra={"tags": {"service": "contacts"}},
    )

    # Check ID sums to see if anything was deleted
    deleted = compare_id_sums(cursor, ALL_DATA, "contacts")
    if not deleted:
        move_deleted_contacts_to_deleted_table(cursor, CONNECTION, ALL_DATA)

    insert_contacts(cursor, ALL_DATA, last_run_timestamp_unix)

    # Validate data / totals
    QUERY = "SELECT COUNT(*) FROM contacts"
    cursor.execute(QUERY)
    result = cursor.fetchone()
    if result is not None:
        DB_ROWS = result[0]

    # Check if the total entries match the expected count
    if DB_ROWS == TOTAL_ENTRIES:
        logger.info(
            "Meta Rows: %s",
            TOTAL_ENTRIES,
            extra={"tags": {"service": "contacts"}},
        )
        logger.info(
            "Row Count from DB is: %s",
            DB_ROWS,
            extra={"tags": {"service": "contacts"}},
        )
    else:
        logger.error(
            "Row Mismatch",
            extra={"tags": {"service": "contacts"}},
        )

    CONNECTION.commit()
    CONNECTION.close()
    update_last_ran(TIMESTAMP_FILE)

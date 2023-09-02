from library import env_library
from library.api_requests_library import get_customers
from library.db_requests_library import (
    connect_to_db,
    create_customer_table_if_not_exists,
    compare_id_sums,
    insert_customers,
    move_deleted_customers_to_deleted_table,
)
from library.fix_date_time_library import log_ts
from library.loki_library import start_loki
from library.timestamp_files import check_last_ran, update_last_ran


def customers():
    """main script for the customer module"""

    logger = start_loki("__customers__")

    TIMESTAMP_FILE = "last_run_customers.txt"
    last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)
    print(f"{log_ts()} Last ran: {last_run_timestamp_unix}")

    # Database configurations
    config = env_library.config
    cursor, CONNECTION = connect_to_db(config)
    create_customer_table_if_not_exists(cursor)

    # Start fetching contacts from page 1
    CURRENT_PAGE = 1
    TOTAL_PAGES = 0
    TOTAL_ENTRIES = 0
    DB_ROWS = 0
    ALL_DATA = []

    # Get 1st Page, then check to make sure not null
    data = get_customers(CURRENT_PAGE)
    if data is not None:
        TOTAL_PAGES = data["meta"]["total_pages"]
        TOTAL_ENTRIES = data["meta"]["total_entries"]
    else:
        print(f"{log_ts()} Error getting contact data")

    # Iterate through all the pages
    for page in range(1, TOTAL_PAGES + 1):
        data = get_customers(page)
        if data is not None:
            ALL_DATA.extend(data["customers"])
            print(f"{log_ts()} Added in page # {page}")
        else:
            print(f"{log_ts()} Error getting tickets data")
            break

    print(f"{log_ts()} Received all data, {TOTAL_PAGES} page(s)")
    print(f"{log_ts()} Total rows in ALL_DATA: {len(ALL_DATA)}")

    # Check ID sums to see if anything was deleted
    deleted = compare_id_sums(cursor, ALL_DATA, "customers")
    if not deleted:
        move_deleted_customers_to_deleted_table(cursor, CONNECTION, ALL_DATA)

    insert_customers(cursor, ALL_DATA, last_run_timestamp_unix)

    # Validate data / totals
    QUERY = "SELECT COUNT(*) FROM customers"
    cursor.execute(QUERY)
    result = cursor.fetchone()
    if result is not None:
        DB_ROWS = result[0]

    # Check if the total entries match the expected count
    if DB_ROWS == TOTAL_ENTRIES:
        print(f"{log_ts()} Meta Rows: {TOTAL_ENTRIES}.")
        print(f"{log_ts()} Row Count from DB is: {DB_ROWS}")
    else:
        print(f"{log_ts()} ROW MISMATCH")

    CONNECTION.commit()
    CONNECTION.close()
    update_last_ran(TIMESTAMP_FILE)
    print(f"{log_ts()} Database connection closed.")

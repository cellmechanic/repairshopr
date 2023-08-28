""" getting invoices """
import sys
import time

from library import env_library
from library.api_requests_library import (
    get_invoices,
    get_date_for_header,
)
from library.db_requests_library import (
    compare_id_sums,
    connect_to_db,
    insert_invoices,
    create_invoices_table_if_not_exists,
    move_deleted_invoices_to_deleted_table,
    rate_limit,
)
from library.fix_date_time_library import log_ts
from library.timestamp_files import check_last_ran


def invoices(full_run=False, lookback_days=14):
    """Get invoices from the API and insert into the database"""
    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_invoices_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    all_data = []

    # Load timestamp
    timestamp_file = "last_run_invoices.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    if not full_run:
        # Get 1st Page, then check to make sure not null
        lookback_date = get_date_for_header(lookback_days)
        data = get_invoices(current_page, lookback_date)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            print(f"{log_ts()} Error getting invoice data")

        # total_pages + 1
        for page in range(1, total_pages + 1):
            data = get_invoices(page, lookback_date)
            if data is not None:
                all_data.extend(data["invoices"])
                print(f"{log_ts()} Added in page # {page}")
            else:
                print(f"{log_ts()} Error getting invoice data")
                break
            # time.sleep(rate_limit())

        print(f"{log_ts()} Adding in : {total_pages}")
        print(f"{log_ts()} Number of entries to consider for DB: {len(all_data)}")
        insert_invoices(cursor, all_data, last_run_timestamp_unix)

    if full_run:
        # Get 1st Page, then check to make sure not null
        data = get_invoices(current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            print(f"{log_ts()} Error getting invoice data")
        print(f"{log_ts()} Total Pages: {total_pages}")
        # total_pages + 1
        for page in range(1, total_pages + 1):
            data = get_invoices(page)
            if data is not None:
                all_data.extend(data["invoices"])
                print(f"{log_ts()} Added in page # {page}")
            else:
                print(f"{log_ts()} Error getting invoice data")
                break
            time.sleep(rate_limit())

        print(f"{log_ts()} Adding in : {total_pages}")
        print(f"{log_ts()} Number of entries to consider for DB: {len(all_data)}")
        insert_invoices(cursor, all_data, last_run_timestamp_unix)

        deleted = compare_id_sums(cursor, all_data, "invoices")

        if not deleted:
            print(f"{log_ts()} There is an id mismatch, we need to look for deletes")
            move_deleted_invoices_to_deleted_table(cursor, connection, all_data)
        else:
            print(f"{log_ts()} No deletes found in invoices, moving on...")

 
    connection.commit()
    connection.close()


if len(sys.argv) > 1:
    ARG = sys.argv[1]
    if ARG == "full":
        invoices(True)
    elif ARG.isdigit() and int(ARG) > 0:  # Ensure it's a positive number
        DAYS_TO_LOOK_BACK = int(ARG)
        invoices(False, DAYS_TO_LOOK_BACK)
    else:
        print(f"{log_ts()} Invalid Response")
else:
    invoices(False)

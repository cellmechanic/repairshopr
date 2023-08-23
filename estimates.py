import sys
import time

from library import env_library
from library.api_requests_library import get_estimates, get_date_for_header
from library.db_requests_library import (
    connect_to_db,
    create_estimates_table_if_not_exists,
    insert_estimates,
    rate_limit,
    compare_id_sums,
    move_deleted_estimates_to_deleted_table,
)
from library.fix_date_time_library import log_ts
from library.timestamp_files import check_last_ran, update_last_ran


def estimates(full_run=False, lookback_days=365):
    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_estimates_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    all_data = []

    # Load timestamp
    timestamp_file = "last_run_estimates.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    if not full_run:
        # Get 1st Page, then check to make sure not null
        data = get_estimates(current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            print(f"{log_ts()} Error getting estimate data")

        lookback_date_formatted = get_date_for_header(lookback_days)
        found_page = 0
        # total_pages + 1
        for page in range(1, total_pages + 1):
            data = get_estimates(page)
            if data is not None and "estimates" in data:
                found_older = any(
                    item["created_at"] < lookback_date_formatted
                    for item in data["estimates"]
                )
                if found_older:
                    print(f"{log_ts()} Found older than {lookback_days} days")
                    break

                all_data.extend(data["estimates"])
                print(f"{log_ts()} Added in page # {page}")
                found_page = page
            else:
                print(f"{log_ts()} Error getting estimates data")
                break
            # time.sleep(rate_limit())

        print(f"{log_ts()} Adding in : {found_page} / {total_pages}")
        print(f"{log_ts()} Number of entries to consider for DB: {len(all_data)}")

        insert_estimates(cursor, all_data, last_run_timestamp_unix)

    if full_run:
        data = get_estimates(current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            print(f"{log_ts()} Error getting estimate data")

        for page in range(1, total_pages + 1):
            data = get_estimates(page)
            if data is not None:
                all_data.extend(data["estimates"])
                print(f"{log_ts()} Added in page # {page}")
            else:
                print(f"{log_ts()} Error getting estimates data")
                break
            time.sleep(rate_limit())

        print(f"{log_ts()} Total pages: {total_pages}")
        print(f"{log_ts()} Number of entries to consider for DB: {len(all_data)}")
        insert_estimates(cursor, all_data, last_run_timestamp_unix)

        deleted = compare_id_sums(cursor, all_data, "estimates")
        if not deleted:
            print(f"{log_ts()} There is an id mismatch, we need to look for deletes")
            move_deleted_estimates_to_deleted_table(cursor, connection, all_data)
        else:
            print(f"{log_ts()} No deletes found in estimates, moving on...")

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)


if len(sys.argv) > 1:
    arg = sys.argv[1]
    if arg == "full":
        estimates(True)
    elif arg.isdigit() and int(arg) > 0:  # Ensure it's a positive number
        days_to_look_back = int(arg)
        estimates(False, days_to_look_back)
    else:
        print(f"{log_ts()} Invalid Response")
else:
    estimates(False)

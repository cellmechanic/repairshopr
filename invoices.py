import sys

from library import env_library
from library.api_requests_library import (
    get_estimates,
    get_invoices,
)
from library.db_requests_library import (
    connect_to_db,
    insert_invoices,
    create_invoices_table_if_not_exists,
)
from library.fix_date_time_library import log_ts
from library.timestamp_files import check_last_ran


def invoices(full_run=False, lookback_days=14):
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
        data = get_invoices(current_page, lookback_days)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            print(f"{log_ts()} Error getting invoice data")

        # total_pages + 1
        for page in range(1, total_pages + 1):
            data = get_invoices(page, lookback_days)
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


if len(sys.argv) > 1:
    arg = sys.argv[1]
    if arg == "full":
        invoices(True)
    elif arg.isdigit() and int(arg) > 0:  # Ensure it's a positive number
        days_to_look_back = int(arg)
        invoices(False, days_to_look_back)
    else:
        print(f"{log_ts()} Invalid Response")
else:
    invoices(False)

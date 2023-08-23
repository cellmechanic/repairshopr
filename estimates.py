import sys
import time

from library import env_library
from library.api_requests_library import get_estimates
from library.db_requests_library import connect_to_db, create_estimates_table_if_not_exists, insert_estimates, \
    rate_limit
from library.fix_date_time_library import log_ts
from library.timestamp_files import check_last_ran, update_last_ran


def estimates(full_run=False):

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

        # total_pages + 1
        for page in range(1, 5):
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

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)


if len(sys.argv) > 1:
    if sys.argv[1] == 'full':
        estimates(True)
    else:
        print(f"{log_ts()} Invalid Response")
else:
    estimates(False)

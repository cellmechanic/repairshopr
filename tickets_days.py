import sys
from library import env_library
from library.api_requests_library import get_tickets
from library.db_requests_library import (
    connect_to_db,
    create_tickets_table_if_not_exists,
    create_comments_table_if_not_exists,
    insert_tickets, insert_comments,
)
from library.fix_date_time_library import log_ts
from library.timestamp_files import check_last_ran, update_last_ran


def ticket_days(lookback_days=60):
    # Load timestamp
    timestamp_file = "last_run_tickets_days.txt"
    last_run_timestamp_unix = check_last_ran(timestamp_file)

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_tickets_table_if_not_exists(cursor)
    create_comments_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    all_data = []

    # Get 1st Page, then check to make sure not null
    data = get_tickets(current_page, lookback_days)
    if data is not None:
        total_pages = data["meta"]["total_pages"]
    else:
        print(f"{log_ts()} Error getting invoice line item data")

    print(f"{log_ts()} Total pages: {total_pages}")

    # total_pages + 1
    for page in range(1, total_pages + 1):
        data = get_tickets(page)
        if data is not None:
            all_data.extend(data["tickets"])
            print(f"{log_ts()} Added in page # {page}")
        else:
            print(f"{log_ts()} Error getting tickets data")
            break
        # time.sleep(rate_limit())

    print(f"{log_ts()} Total pages: {total_pages}")
    print(f"{log_ts()} Number of entries to consider for DB: {len(all_data)}")
    insert_tickets(cursor, all_data, last_run_timestamp_unix)
    insert_comments(cursor, all_data, last_run_timestamp_unix)

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)
    

if len(sys.argv) > 1:
    try:
        days = int(sys.argv[1])
        ticket_days(days)
    except ValueError:
        print(f"{log_ts()} Invalid Number of Days")
else:
    ticket_days()

"""ticket with parameterized lookback"""
from library import env_library
from library.api_requests_library import get_tickets
from library.db_requests_library import (
    connect_to_db,
    create_tickets_table_if_not_exists,
    create_comments_table_if_not_exists,
    insert_tickets,
    insert_comments,
)
from library.loki_library import start_loki
from library.timestamp_files import check_last_ran, update_last_ran


def ticket_days(lookback_days):
    """main script for the ticket module"""
    logger = start_loki("__ticket_days__")

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
        logger.error(
            "Error getting ticket data",
            extra={"tags": {"service": "tickets"}},
        )

    logger.info(
        "Total pages: %s",
        total_pages,
        extra={"tags": {"service": "tickets"}},
    )

    # total_pages + 1
    for page in range(1, total_pages + 1):
        data = get_tickets(page, lookback_days)
        if data is not None:
            all_data.extend(data["tickets"])
            logger.info(
                "Added in page # %s",
                page,
                extra={"tags": {"service": "tickets"}},
            )
        else:
            logger.error(
                "Error getting ticket data",
                extra={"tags": {"service": "tickets"}},
            )
            break
        # time.sleep(rate_limit())

    logger.info(
        "Total tickets: %s",
        len(all_data),
        extra={"tags": {"service": "tickets"}},
    )
    logger.info(
        "Number of entries to consider for DB: %s",
        len(all_data),
        extra={"tags": {"service": "tickets"}},
    )

    insert_tickets(cursor, all_data, last_run_timestamp_unix)
    insert_comments(cursor, all_data, last_run_timestamp_unix)

    connection.commit()
    connection.close()
    update_last_ran(timestamp_file)


# if len(sys.argv) > 1:
#     try:
#         DAYS = int(sys.argv[1])
#         ticket_days(DAYS)
#     except ValueError:
#         print(f"{log_ts()} Invalid Number of Days")
# else:
#     ticket_days()

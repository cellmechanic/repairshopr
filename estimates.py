
def estimates(full_run = False):
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
"""get all tickets from the beginning"""
from datetime import datetime
import time
import library.env_library as env_library
from library.db_requests_library import (
    create_tickets_table_if_not_exists,
    create_comments_table_if_not_exists,
    connect_to_db,
)
from library.api_requests_library import (
    check_last_ran,
    update_last_ran,
    get_tickets,
    insert_tickets,
)

# Load timestamp
TIMESTAMP_FILE = "last_run_tickets_full.txt"
last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)

# Database configurations
config = env_library.config
cursor, CONNECTION = connect_to_db(config)
create_tickets_table_if_not_exists(cursor)
create_comments_table_if_not_exists(cursor)

# Fetch data from the env file for API
headers = {"Authorization": f"Bearer {env_library.api_key_tickets}"}
update_last_ran(TIMESTAMP_FILE)

# Start fetching line items from page 1
CURRENT_PAGE = 1
TOTAL_PAGES = 0
ENTRY_COUNT = 0
DB_ROWS = 0
ALL_DATA = []

# Get 1st Page, then check to make sure not null
data = get_tickets(CURRENT_PAGE)
if data is not None:
    TOTAL_PAGES = data["meta"]["total_pages"]
else:
    print("Error getting invoice line item data")

# TOTAL_PAGES + 1
for page in range(1, 3):
    data = get_tickets(page)
    if data is not None:
        ALL_DATA.extend(data["tickets"])
        print(f"{datetime.now()} : Added in page # {page}")
    else:
        print("Error getting tickets data")
        break
    time.sleep(8 / 128)

print(f"Total pages: {TOTAL_PAGES}")

#     for tickets in ALL_DATA:
#     created_at_str = tickets["created_at"]
#     formatted_created_at = format_date_fordb(created_at_str)
#     tickets["created_at"] = formatted_created_at

#     updated_at_str = tickets["updated_at"]
#     updated_at_unix = int(
#         datetime.strptime(updated_at_str, get_timestamp_code()).timestamp()
#     )
#     formatted_updated_at = format_date_fordb(updated_at_str)
#     tickets["updated_at"] = formatted_updated_at

print(f"Number of entries to consider for DB: {len(ALL_DATA)}")
insert_tickets(cursor, ALL_DATA, last_run_timestamp_unix)
CONNECTION.commit()
CONNECTION.close()

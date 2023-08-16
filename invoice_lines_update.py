"""getting RS invoice line items"""
from datetime import datetime
import time
import library.env_library as env_library
from library.fix_date_time_library import format_date_fordb, get_timestamp_code
from library.db_requests_library import (
    create_invoice_items_table_if_not_exists,
    insert_invoice_lines,
)
from library.api_requests_library import (
    get_invoice_lines,
)
from library.timestamp_files import update_last_ran, check_last_ran

# Load timestamp
TIMESTAMP_FILE = "last_run_invoice_lines_updates.txt"
last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)

# Database configurations
config = env_library.config
CONNECTION = None
cursor, CONNECTION = create_invoice_items_table_if_not_exists(config)

# Fetch data from the env file for API
headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}
update_last_ran(TIMESTAMP_FILE)

# Start fetching line items from the end
TOTAL_PAGES = 0
CURRENT_PAGE = 1
TOTAL_ENTRIES = 0
ENTRY_COUNT = 0
DB_ROWS = 0
ALL_DATA = []
FOUND_LAST_UPDATED_ROW = False

# Get 1st Page, then check to make sure not null
data = get_invoice_lines(CURRENT_PAGE)
if data is not None:
    TOTAL_PAGES = data["meta"]["total_pages"]
    TOTAL_ENTRIES = data["meta"]["total_entries"]
    CURRENT_PAGE = TOTAL_PAGES
else:
    print("Error getting invoice line item data")

print(f"Total pages: {TOTAL_PAGES}")
print("Created invoice_items table")

# TOTAL_PAGES + 1
for page in range(TOTAL_PAGES, 0, -1):
    data = get_invoice_lines(page)
    if data is not None:
        if (
            int(
                datetime.strptime(
                    data["line_items"][-1]["updated_at"], get_timestamp_code()
                ).timestamp()
            )
            < last_run_timestamp_unix
        ):
            FOUND_LAST_UPDATED_ROW = True
            print(f"Found last updated row in page {page}")
            break

        ALL_DATA.extend(data["line_items"])
        print(f"{datetime.now()} : Added in page # {page}")
    else:
        print("Error getting line items data")
        break
    time.sleep(4 / 128)

if not FOUND_LAST_UPDATED_ROW:
    print("No updates since last run")


for line_items in ALL_DATA:
    created_at_str = line_items["created_at"]
    formatted_created_at = format_date_fordb(created_at_str)
    line_items["created_at"] = formatted_created_at

    updated_at_str = line_items["updated_at"]
    updated_at_unix = int(
        datetime.strptime(updated_at_str, get_timestamp_code()).timestamp()
    )
    formatted_updated_at = format_date_fordb(updated_at_str)
    line_items["updated_at"] = formatted_updated_at

print(f"Number of entries to consider for DB: {len(ALL_DATA)}")
insert_invoice_lines(cursor, ALL_DATA, last_run_timestamp_unix)
CONNECTION.commit()
CONNECTION.close()

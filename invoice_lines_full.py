"""getting RS invoice line items"""
# 855 355 5777
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
from library.timestamp_files import check_last_ran, update_last_ran

# Load timestamp
TIMESTAMP_FILE = "last_run_invoice_lines.txt"
last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)

# Database configurations
config = env_library.config
CONNECTION = None
cursor, CONNECTION = create_invoice_items_table_if_not_exists(config)

# Fetch data from the env file for API
headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}
update_last_ran(TIMESTAMP_FILE)

# Start fetching line items from page 1
CURRENT_PAGE = 1
TOTAL_PAGES = 0
TOTAL_ENTRIES = 0
ENTRY_COUNT = 0
DB_ROWS = 0
ALL_DATA = []

# Get 1st Page, then check to make sure not null
data = get_invoice_lines(CURRENT_PAGE)
if data is not None:
    TOTAL_PAGES = data["meta"]["total_pages"]
    TOTAL_ENTRIES = data["meta"]["total_entries"]
else:
    print("Error getting invoice line item data")

print(f"Total pages: {TOTAL_PAGES}")
print("Created invoice_items table")
# TOTAL_PAGES + 1
for page in range(1, TOTAL_PAGES + 1):
    data = get_invoice_lines(page)
    if data is not None:
        ALL_DATA.extend(data["line_items"])
        print(f"{datetime.now()} : Added in page # {page}")
    else:
        print("Error getting line items data")
        break
    time.sleep(8 / 128)

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

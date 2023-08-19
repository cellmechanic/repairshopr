"""getting RS invoice line items"""
import time
import library.env_library as env_library
from library.fix_date_time_library import log_ts
from library.db_requests_library import (
    create_invoice_items_table_if_not_exists,
    insert_invoice_lines,
    rs_to_unix_timestamp,
    rate_limit,
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

# Start fetching line items from the end
TOTAL_PAGES = 0
CURRENT_PAGE = 1
TOTAL_ENTRIES = 0
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
    print(f"{log_ts()} Error getting invoice line item data")

print(f"{log_ts()} Total pages: {TOTAL_PAGES}")

# work backwards from last page
for page in range(TOTAL_PAGES, 0, -1):
    data = get_invoice_lines(page)
    if data is not None:
        if (
            rs_to_unix_timestamp(data["line_items"][-1]["updated_at"])
            < last_run_timestamp_unix
        ):
            FOUND_LAST_UPDATED_ROW = True
            print(f"{log_ts()} Found last updated row in page {page}")
            break

        ALL_DATA.extend(data["line_items"])
        print(f"{log_ts()} Added in page # {page}")
    else:
        print(f"{log_ts()} Error getting line items data")
        break
    time.sleep(rate_limit())

if not FOUND_LAST_UPDATED_ROW:
    print(f"{log_ts()} No updates since last run")

print(f"{log_ts()} Number of entries to consider for DB: {len(ALL_DATA)}")
insert_invoice_lines(cursor, ALL_DATA, last_run_timestamp_unix)
CONNECTION.commit()
CONNECTION.close()
update_last_ran(TIMESTAMP_FILE)

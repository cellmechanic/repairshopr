"""getting RS invoice line items"""
import time
import library.env_library as env_library
from library.fix_date_time_library import log_ts
from library.db_requests_library import (
    create_invoice_items_table_if_not_exists,
    insert_invoice_lines,
    rate_limit,
    compare_id_sums,
    move_deleted_lines_to_deleted_table,
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

# Start fetching line items from page 1
CURRENT_PAGE = 1
TOTAL_PAGES = 0
TOTAL_ENTRIES = 0
ALL_DATA = []

# Get 1st Page, then check to make sure not null
data = get_invoice_lines(CURRENT_PAGE)
if data is not None:
    TOTAL_PAGES = data["meta"]["total_pages"]
    TOTAL_ENTRIES = data["meta"]["total_entries"]
else:
    print(f"{log_ts()} Error getting invoice line item data")

print(f"{log_ts()} Total pages: {TOTAL_PAGES}")

# TOTAL_PAGES + 1
for page in range(1, TOTAL_PAGES + 1):
    data = get_invoice_lines(page)
    if data is not None:
        ALL_DATA.extend(data["line_items"])
        print(f"{log_ts()} Added in page # {page}")
    else:
        print(f"{log_ts()} Error getting line items data")
        break
    time.sleep(rate_limit())

insert_invoice_lines(cursor, ALL_DATA, last_run_timestamp_unix)

# Check ID sums to see if anything was deleted
deleted = compare_id_sums(cursor, ALL_DATA, "invoice_items")
if not deleted:
    print(f"{log_ts()} There is an id mismatch, we need to look for del")
    move_deleted_lines_to_deleted_table(cursor, CONNECTION, ALL_DATA)



CONNECTION.commit()
CONNECTION.close()
update_last_ran(TIMESTAMP_FILE)

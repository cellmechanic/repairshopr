"""Getting RS Contacts"""

from datetime import datetime
import time
import mysql.connector
import library.env_library as env_library
from library.fix_date_time_library import format_date_fordb, get_timestamp_code, log_ts
from library.api_requests_library import (
    get_contacts,
)
from library.db_requests_library import (
    create_contact_table_if_not_exists,
    connect_to_db,
    compare_id_sums,
    move_deleted_contacts_to_deleted_table,
    rate_limit,
    insert_contacts,
)
from library.timestamp_files import update_last_ran, check_last_ran

# Load timestamp
TIMESTAMP_FILE = "last_run_contacts.txt"
last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)

# Database configurations
config = env_library.config
CONNECTION = None
cursor, CONNECTION = connect_to_db(config)
create_contact_table_if_not_exists(cursor)

# Fetch data from the API
headers = {"Authorization": f"Bearer {env_library.api_key_contact}"}

# Start fetching contacts from page 1
CURRENT_PAGE = 1
TOTAL_PAGES = 0
TOTAL_ENTRIES = 0
DB_ROWS = 0
ALL_DATA = []

# Get 1st Page, then check to make sure not null
data = get_contacts(CURRENT_PAGE)
if data is not None:
    TOTAL_PAGES = data["meta"]["total_pages"]
    TOTAL_ENTRIES = data["meta"]["total_entries"]
else:
    print(f"{log_ts()} Error getting contact data")

# Iterate through all the pages
for page in range(1, TOTAL_PAGES + 1):
    data = get_contacts(page)
    if data is not None:
        ALL_DATA.extend(data["contacts"])
        print(f"{log_ts()} Added in page # {page}")
    else:
        print(f"{log_ts()} Error getting tickets data")
        break
    time.sleep(rate_limit())

print(f"{log_ts()} Recieved all data, {TOTAL_PAGES} page(s)")
print(f"{log_ts()} Total rows in ALL_DATA: {len(ALL_DATA)}")

# Check ID sums to see if anything was deleted
DELETED = False
deleted = compare_id_sums(cursor, ALL_DATA)
if not deleted:
    move_deleted_contacts_to_deleted_table(cursor, CONNECTION, ALL_DATA)

insert_contacts(cursor, ALL_DATA, last_run_timestamp_unix)

# Validate data / totals
QUERY = "SELECT COUNT(*) FROM contacts"
cursor.execute(QUERY)
result = cursor.fetchone()
if result is not None:
    DB_ROWS = result[0]

# Check if the total entries match the expected count
if DB_ROWS == TOTAL_ENTRIES:
    print(f"{log_ts()} Meta Rows: {TOTAL_ENTRIES}.")
    print(f"{log_ts()} Row Count from DB is: {DB_ROWS}")
else:
    print(f"{log_ts()} ROW MISMATCH")

CONNECTION.commit()
CONNECTION.close()
update_last_ran(TIMESTAMP_FILE)
print(f"{log_ts()} Database connection closed.")

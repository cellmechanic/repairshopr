"""Getting RS Contacts"""
from datetime import datetime
import mysql.connector
import env_library
from fix_dates_library import format_date_fordb
from api_requests_library import get_contacts
from api_requests_library import update_last_ran
from api_requests_library import check_last_ran
from api_requests_library import get_timestamp_code
from db_requests_library import create_contact_table_if_not_exists
# from api_requests_library import compare_db_to_rs

# Load timestamp
TIMESTAMP_FILE = 'last_run_contacts.txt'
last_run_timestamp_unix = check_last_ran(TIMESTAMP_FILE)

# Database configuration
config = env_library.config
CONNECTION = None

# Connect to the database
try:
    cursor, CONNECTION = create_contact_table_if_not_exists(config)
    # Fetch data from the API
    headers = {'Authorization': f'Bearer {env_library.api_key_contact}'}

    # Start fetching contacts from page 1
    PAGE = 1
    TOTAL_PAGES = 0
    TOTAL_ENTRIES = 0
    ENTRY_COUNT = 0
    DB_ROWS = 0
    ALL_DATA = []

    # Get 1st Page, then check to make sure not null
    data = get_contacts(PAGE)
    if data is not None:
        TOTAL_PAGES = data['meta']['total_pages']
        TOTAL_ENTRIES = data['meta']['total_entries']
    else:
        print("Error getting contact data")

    # Iterate through the pages
    while PAGE <= TOTAL_PAGES:
        if data is not None:
            ALL_DATA.extend(data['contacts'])
            print(f'Added in page # {PAGE}')
            PAGE += 1
    print(f'Recieved all data, {TOTAL_PAGES} page(s)')

    for contact in ALL_DATA:
            #for contact in data['contacts']:
                # fix dates for the DB
        created_at_str = contact['created_at']
        formatted_created_at = format_date_fordb(created_at_str)
        updated_at_str = contact['updated_at']
        updated_at_unix = int(datetime.strptime(
            updated_at_str, get_timestamp_code()).timestamp())
        formatted_updated_at = format_date_fordb(updated_at_str)

        if updated_at_unix > last_run_timestamp_unix:
            print(
                f"Contact {contact['id']} has been updated since last run.")
            cursor.execute('''
                INSERT INTO contacts (id, name, address1, address2, city, state, zip, email, phone, mobile, latitude, longitude, customer_id, 
                        account_id, notes, created_at, updated_at, vendor_id, title, opt_out, extension, processed_phone, 
                        processed_mobile, ticket_matching_emails)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                address1 = VALUES(address1),
                address2 = VALUES(address2),
                city = VALUES(city),
                state = VALUES(state),
                zip = VALUES(zip),
                email = VALUES(email),
                phone = VALUES(phone),
                mobile = VALUES(mobile),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                customer_id = VALUES(customer_id),
                account_id = VALUES(account_id),
                notes = VALUES(notes),
                created_at = VALUES(created_at),
                updated_at = VALUES(updated_at),
                vendor_id = VALUES(vendor_id),
                title = VALUES(title),
                opt_out = VALUES(opt_out),
                extension = VALUES(extension),
                processed_phone = VALUES(processed_phone),
                processed_mobile = VALUES(processed_mobile),
                ticket_matching_emails = VALUES(ticket_matching_emails)
                ''', (
                contact['id'], contact['name'], contact['address1'], contact['address2'],
                contact['city'], contact['state'], contact['zip'], contact['email'],
                contact['phone'], contact['mobile'], contact['latitude'],
                contact['longitude'], contact['customer_id'],
                contact['account_id'], contact['notes'],
                formatted_created_at, formatted_updated_at, contact['vendor_id'],
                contact['properties']['title'] if 'title' in contact['properties']
                else None,
                contact['opt_out'], contact['extension'], contact['processed_phone'],
                contact['processed_mobile'], contact['ticket_matching_emails']
            ))
            CONNECTION.commit()
            ENTRY_COUNT += 1
           
    print(f"All data received from {TOTAL_PAGES} page(s)")
    QUERY = "SELECT COUNT(*) FROM contacts"
    cursor.execute(QUERY)
    result = cursor.fetchone()
    if result is not None:
        DB_ROWS = result[0]
               
    # Check if the total entries match the expected count
    if ENTRY_COUNT != TOTAL_ENTRIES:
        print(
            f'Warning: Made changes to {ENTRY_COUNT} entries but found, '
            f'Meta Rows: {TOTAL_ENTRIES}.')
        print(f'Row Count from DB is: {DB_ROWS}')

    print("Contacts successfully inserted into the database.")
    print(f"Made: {TOTAL_ENTRIES} updates")
    print("Comparing DB to RS")
    # try:
    #    compare_db_to_rs(cursor,CONNECTION)
    # except IOError as error:
    #    print("error")

except mysql.connector.Error as err:
    print(f"Database error {err}")
finally:
    if CONNECTION and CONNECTION.is_connected():
        CONNECTION.close()
        update_last_ran(TIMESTAMP_FILE)

print("Database connection closed.")

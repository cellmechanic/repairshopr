"""Getting RS Contacts"""
import mysql.connector
import env_library
from fix_dates_library import format_date_fordb
from api_requests_library import get_contacts

# Database configuration
config = env_library.config

# Connect to the database
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Create the contacts table if not exists
cursor.execute('''CREATE TABLE IF NOT EXISTS contacts (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    address1 VARCHAR(255),
    address2 VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255),
    mobile VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    customer_id INT,
    account_id INT,
    notes TEXT,
    created_at DATETIME,
    updated_at DATETIME,
    vendor_id INT,
    title VARCHAR(255),
    opt_out BOOLEAN,
    extension VARCHAR(50),
    processed_phone VARCHAR(255),
    processed_mobile VARCHAR(255),
    ticket_matching_emails VARCHAR(255)
)
''')

# Fetch data from the API
headers = {'Authorization': f'Bearer {env_library.api_key_contact}'}

# Start fetching contacts from page 1
PAGE = 1
data = get_contacts(PAGE)
total_pages = data['meta']['total_pages']
total_entries = data['meta']['total_entries']
ENTRY_COUNT = 0

# Iterate through the pages
# Iterate through the pages
while PAGE <= total_pages:
    for contact in data['contacts']:
        #fix dates for the DB
        created_at_str = contact['created_at']
        formatted_created_at = format_date_fordb(created_at_str)
        updated_at_str = contact['updated_at']
        formatted_updated_at = format_date_fordb(updated_at_str)

        cursor.execute('''
        INSERT INTO contacts (id, name, address1, address2, city, state, zip, email, phone, mobile, latitude, longitude, customer_id, 
                       account_id, notes, created_at, updated_at, vendor_id, title, opt_out, extension, processed_phone, processed_mobile, ticket_matching_emails)
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
            contact['phone'], contact['mobile'], contact['latitude'], contact['longitude'],
            contact['customer_id'], contact['account_id'], contact['notes'],
            formatted_created_at, formatted_updated_at, contact['vendor_id'],
            contact['properties']['title'] if 'title' in contact['properties'] else None,
            contact['opt_out'], contact['extension'], contact['processed_phone'],
            contact['processed_mobile'], contact['ticket_matching_emails']
        ))
        connection.commit()
        ENTRY_COUNT += 1

    print(f'Page {PAGE} / {total_pages} processed.')
    PAGE += 1
    if PAGE <= total_pages:
        data = get_contacts(PAGE)

# Check if the total entries match the expected count
if ENTRY_COUNT != total_entries:
    print(f'Warning: Expected {total_entries} entries but found {ENTRY_COUNT}.')

print("Contacts successfully inserted into the database.")

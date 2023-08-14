"""library for diff API requests"""
import time
from datetime import datetime
import requests
from library import env_library


def get_contacts(page):
    """api request"""
    url = f"{env_library.api_url_contact}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_contact}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Error fetching contacts on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"Failed to get data for page {page}: {str(error)}")
        return None


def get_invoice_lines(page):
    """api request"""
    url = f"{env_library.api_url_invoice}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Error fetching invoice line items on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"Failed to get data for page {page}: {str(error)}")
        return None


def insert_invoice_lines(cursor, items, last_run_timestamp_unix):
    """insert invoice lines"""
    added = 0
    for item in items:
        updated_at_str = item["updated_at"]
        # print(updated_at_str)
        updated_at_unix = datetime.strptime(updated_at_str, "%Y-%m-%d %H:%M:%S")
        updated_at_unix = int(updated_at_unix.timestamp())
        # print(updated_at_unix)

        print(f"Processing item {item['id']}")  # Debugging print
        print(
            f"Item timestamp: {updated_at_unix}, Last run timestamp: {last_run_timestamp_unix}"
        )  # Debugging print

        # Check if the record exists and get the current updated_at value
        cursor.execute(
            "SELECT updated_at FROM invoice_items WHERE id = %s", (item["id"],)
        )
        existing_record = cursor.fetchone()

        if existing_record:
            existing_timestamp = int(existing_record[0].timestamp())
            print(f"existing_timestamp: {existing_timestamp}")

            if updated_at_unix > existing_timestamp:
                # If record exists and updated_at is greater, update it
                print(f"line item {item['id']} has been updated since last run.")
                sql = """
                    UPDATE invoice_items SET
                        created_at = %s,
                        updated_at = %s,
                        invoice_id = %s,
                        item = %s,
                        name = %s,
                        cost = %s,
                        price = %s,
                        quantity = %s,
                        product_id = %s,
                        taxable = %s,
                        discount_percent = %s,
                        position = %s,
                        invoice_bundle_id = %s,
                        discount_dollars = %s,
                        product_category = %s
                    WHERE id = %s
                """
                values = (
                    item["id"],
                    item["created_at"],
                    item["updated_at"],
                    item["invoice_id"],
                    item["item"],
                    item["name"],
                    item["cost"],
                    item["price"],
                    item["quantity"],
                    item["product_id"],
                    item["taxable"],
                    item["discount_percent"],
                    item["position"],
                    item["invoice_bundle_id"],
                    item["discount_dollars"],
                    item["product_category"],
                )
                cursor.execute(sql, values)
        else:
            # If record doesn't exist, insert it
            added += 1
            print(f"Inserting new line item {item['id']}.")
            sql = """
                INSERT INTO invoice_items (
                    id, created_at, updated_at, invoice_id, item, name,
                    cost, price, quantity, product_id, taxable, discount_percent,
                    position, invoice_bundle_id, discount_dollars, product_category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["created_at"],
                item["updated_at"],
                item["invoice_id"],
                item["item"],
                item["name"],
                item["cost"],
                item["price"],
                item["quantity"],
                item["product_id"],
                item["taxable"],
                item["discount_percent"],
                item["position"],
                item["invoice_bundle_id"],
                item["discount_dollars"],
                item["product_category"],
            )
            print(f"Inserting new line item with ID: {item['id']}.")
            print("Values tuple:", values)
            cursor.execute(sql, values)
    print(f"Added {added} new line items.")


def update_last_ran(timestamp_file):
    """update the last time the script ran file, pass in the file"""
    try:
        with open(timestamp_file, "w", encoding="utf-8") as file:
            file.write(str(int(time.time())))
        print("Timestamp updated successfully to: ", int(time.time()))
    except IOError as error:
        print(f"Error updating timestamp: {error}")


def check_last_ran(timestamp_file):
    """check the passed timestamp file to see the last time this ran"""
    try:
        with open(timestamp_file, "r", encoding="utf-8") as file:
            last_run_timestamp_str = file.read().strip()
            if last_run_timestamp_str:
                last_run_timestamp = int(last_run_timestamp_str)
                print(f"Last ran @ {last_run_timestamp_str} in unix")
                last_run_time_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(last_run_timestamp)
                )
                print(f"Last ran @ {last_run_time_str}")
                return int(last_run_timestamp_str)
            else:
                print("no timestamp found, using 0 as time")
                return int(0)
    except FileNotFoundError:
        print("no timestamp file found, using now as time")
        return int(time.time())


def get_timestamp_code():
    """return the correct timestamp code to stay uniform"""
    return "%Y-%m-%dT%H:%M:%S.%f%z"


def compare_db_to_rs(cursor, connection):
    """take each id and check RS API to make sure it exists"""
    cursor.execute("SELECT * FROM contacts")
    contacts = cursor.fetchall()

    # Iterate through the contacts
    for contact in contacts:
        # Assuming the ID is the first element in the tuple
        contact_id = contact[0]
        url = f"https://cellmechanic.repairshopr.com/api/v1/contacts/{contact_id}"

        response = requests.get(url, timeout=10)

        if response.status_code == 404:
            # Contact not found; move to deleted_contacts
            placeholders = ", ".join(["%s"] * len(contact))
            cursor.execute(
                f"INSERT INTO deleted_contacts VALUES ({placeholders})", contact
            )
            cursor.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))
            print(f"Moved contact {contact_id} to deleted_contacts table")

    # Commit changes
    connection.commit()


def compare_id_sums(cursor, data):
    """compare the id sums to make sure they match"""
    cursor.execute("SELECT SUM(id) FROM contacts")
    contacts_sum = cursor.fetchone()[0]

    sum_of_ids_api = sum(contact["id"] for contact in data)
    print(f"Sum of IDs from API: {sum_of_ids_api}")
    print(f"Sum of IDs from DB: {contacts_sum}")

    if sum_of_ids_api == contacts_sum:
        print("The sum of IDs matches.")
    else:
        print("The sum of IDs does not match.")

    return sum_of_ids_api == contacts_sum


def move_deleted_contacts_to_deleted_table(cursor, connection, data):
    """compare the id sums, and move any entries not
    in the data array to the deleted_contacts table"""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM contacts")
    contacts_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(contact["id"] for contact in data)
    print(f"Sum of IDs from API: {sum_of_ids_api}")
    print(f"Sum of IDs from DB: {contacts_sum}")

    if sum_of_ids_api != contacts_sum:
        print("The sum of IDs does not match. Identifying deleted contacts...")

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_contacts (
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
    """
        )

        # Get the set of IDs from the API data
        api_ids = {contact["id"] for contact in data}

        # Query all IDs from the contacts table
        cursor.execute("SELECT id FROM contacts")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                print(f"Moving contact with ID {db_id} to deleted_contacts table...")

                # Copy the row to the deleted_contacts table
                cursor.execute(
                    """INSERT INTO deleted_contacts SELECT
                                * FROM contacts WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the contacts table
                cursor.execute("DELETE FROM contacts WHERE id = %s", (db_id,))

                connection.commit()

        print("Operation completed successfully.")
    else:
        print("The sum of IDs matches.")

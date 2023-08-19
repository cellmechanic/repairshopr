"""library for db connection functions"""
import json
import mysql.connector
from library.fix_date_time_library import (
    rs_to_unix_timestamp,
    format_date_fordb,
    log_ts,
)


def rate_limit():
    """current rate limit setting"""
    return 30 / 128


def create_contact_table_if_not_exists(cursor):
    """create the contact db if it desn't already exist"""
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS contacts (
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


def create_invoice_items_table_if_not_exists(config):
    """Create the invoice_items table if it doesn't already exist."""
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    # Create the invoice_items table if not exists
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS invoice_items (
            id INT PRIMARY KEY,
            created_at DATETIME,
            updated_at DATETIME,
            invoice_id INT,
            item VARCHAR(1024),
            name VARCHAR(4096),
            cost DECIMAL(10, 2),
            price DECIMAL(10, 2),
            quantity DECIMAL(10, 2),
            product_id INT,
            taxable BOOLEAN,
            discount_percent DECIMAL(5, 2),
            position INT,
            invoice_bundle_id INT,
            discount_dollars DECIMAL(10, 2),
            product_category VARCHAR(255)
        )
        """
    )
    return cursor, connection


def create_tickets_table_if_not_exists(cursor):
    """Create the ticket table if it doesn't already exist."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tickets (
            id INT PRIMARY KEY,
            number INT,
            subject VARCHAR(255),
            created_at DATETIME,
            customer_id INT,
            customer_business_then_name VARCHAR(255),
            due_date DATETIME,
            resolved_at DATETIME,
            start_at DATETIME,
            end_at DATETIME,
            location_id INT,
            problem_type VARCHAR(255),
            status VARCHAR(255),
            ticket_type_id INT,
            properties TEXT,
            user_id INT,
            updated_at DATETIME,
            pdf_url TEXT,
            priority VARCHAR(255),
            comments TEXT
        )
        """
    )


def create_comments_table_if_not_exists(cursor):
    """create the comments table if it's not already made"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS comments (
            id INT PRIMARY KEY,
            created_at DATETIME,
            updated_at DATETIME,
            ticket_id INT,
            subject VARCHAR(255),
            body TEXT,
            tech VARCHAR(255),
            hidden BOOLEAN,
            user_id INT,
            FOREIGN KEY (ticket_id) REFERENCES tickets(id)
        )
        """
    )


def connect_to_db(config):
    """connect to the db"""
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    return cursor, connection


def insert_invoice_lines(cursor, items, last_run_timestamp_unix):
    """insert invoice lines"""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute(
            "SELECT updated_at FROM invoice_items WHERE id = %s", (item["id"],)
        )
        existing_record = cursor.fetchone()

        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                # If record exists and updated_at is greater, update it
                print(
                    f"{log_ts()} Line item {item['id']} has been updated since last run."
                )
                updated += 1
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
                    format_date_fordb(item["created_at"]),
                    format_date_fordb(item["updated_at"]),
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
            print(f"{log_ts()} Inserting new line item {item['id']}.")
            sql = """
                INSERT INTO invoice_items (
                    id, created_at, updated_at, invoice_id, item, name,
                    cost, price, quantity, product_id, taxable, discount_percent,
                    position, invoice_bundle_id, discount_dollars, product_category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                format_date_fordb(item["created_at"]),
                format_date_fordb(item["updated_at"]),
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
            print(f"{log_ts()} Inserting new line item with ID: {item['id']}.")
            print(f"{log_ts()} Values tuple:", values)
            cursor.execute(sql, values)
    print(f"{log_ts()} Added {added} new line items, updated {updated} line items.")


def insert_tickets(cursor, items, last_run_timestamp_unix):
    """Insert or update tickets based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM tickets WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()
        print(f"{log_ts()} Processing ticket {item['number']}")
        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                # If record exists and updated_at is greater, update it
                updated += 1
                sql = """
                    UPDATE tickets SET
                        number = %s,
                        subject = %s,
                        created_at = %s,
                        customer_id = %s,
                        customer_business_then_name = %s,
                        due_date = %s,
                        resolved_at = %s,
                        start_at = %s,
                        end_at = %s,
                        location_id = %s,
                        problem_type = %s,
                        status = %s,
                        ticket_type_id = %s,
                        properties = %s,
                        user_id = %s,
                        updated_at = %s,
                        pdf_url = %s,
                        priority = %s,
                        comments = %s
                    WHERE id = %s
                """
                values = (
                    item["number"],
                    item["subject"],
                    format_date_fordb(item["created_at"]),
                    item["customer_id"],
                    item["customer_business_then_name"],
                    format_date_fordb(item["due_date"]),
                    format_date_fordb(item["resolved_at"]),
                    item["start_at"],
                    item["end_at"],
                    item["location_id"],
                    item["problem_type"],
                    item["status"],
                    item["ticket_type_id"],
                    json.dumps(item.get("properties", {})),
                    item["user_id"],
                    format_date_fordb(item["updated_at"]),
                    item["pdf_url"],
                    item["priority"],
                    json.dumps(item.get("comments", {})),
                    item["id"],
                )
                cursor.execute(sql, values)
        else:
            # If record doesn't exist, insert it
            added += 1
            sql = """
                INSERT INTO tickets (
                    id, number, subject, created_at, customer_id, 
                    customer_business_then_name, due_date, resolved_at, start_at,
                    end_at, location_id, problem_type, status, ticket_type_id,
                    properties, user_id, updated_at, pdf_url, priority, comments
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["number"],
                item["subject"],
                format_date_fordb(item["created_at"]),
                item["customer_id"],
                item["customer_business_then_name"],
                format_date_fordb(item["due_date"]),
                format_date_fordb(item["resolved_at"]),
                item["start_at"],
                item["end_at"],
                item["location_id"],
                item["problem_type"],
                item["status"],
                item["ticket_type_id"],
                json.dumps(item.get("properties", {})),
                item["user_id"],
                format_date_fordb(item["updated_at"]),
                item["pdf_url"],
                item["priority"],
                json.dumps(item.get("comments", {})),
            )
            print(log_ts(), values)
            cursor.execute(sql, values)

    print(f"{log_ts()} Added {added} new tickets, updated {updated} existing tickets.")


def insert_contacts(cursor, items, last_run_timestamp_unix):
    """Insert of update contacts based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM contacts WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()
        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                print(
                    f"{log_ts()} Contact {item['name']} has been updated since last run."
                )
                updated += 1
                sql = """
                    UPDATE contacts SET 
                        id = %s, 
                        name = %s, 
                        address1 = %s, 
                        address2 = %s, 
                        city = %s, 
                        state = %s, 
                        zip = %s, 
                        email = %s, 
                        phone = %s, 
                        mobile = %s, 
                        latitude = %s, 
                        longitude = %s, 
                        customer_id = %s,
                        account_id = %s, 
                        notes = %s, 
                        created_at = %s, 
                        updated_at = %s, 
                        vendor_id = %s, 
                        title = %s, 
                        opt_out = %s, 
                        extension = %s, 
                        processed_phone = %s,
                        processed_mobile = %s, 
                        ticket_matching_emails = %s
                    WHERE id = %s
                    """
                values = (
                    item["id"],
                    item["name"],
                    item["address1"],
                    item["address2"],
                    item["city"],
                    item["state"],
                    item["zip"],
                    item["email"],
                    item["phone"],
                    item["mobile"],
                    item["latitude"],
                    item["longitude"],
                    item["customer_id"],
                    item["account_id"],
                    item["notes"],
                    format_date_fordb(item["created_at"]),
                    format_date_fordb(item["updated_at"]),
                    item["vendor_id"],
                    item["properties"]["title"]
                    if "title" in item["properties"]
                    else None,
                    item["opt_out"],
                    item["extension"],
                    item["processed_phone"],
                    item["processed_mobile"],
                    item["ticket_matching_emails"],
                    item["id"],
                )

                cursor.execute(sql, values)
                print(f"{log_ts()} All data received from {len(items)%25} page(s)")
        else:
            added += 1
            sql = """
                INSERT INTO contacts (
                    id, name, address1, address2, city, state, zip, email, phone, mobile, latitude, longitude, customer_id,
                    account_id, notes, created_at, updated_at, vendor_id, title, opt_out, extension, processed_phone,
                    processed_mobile, ticket_matching_emails
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["name"],
                item["address1"],
                item["address2"],
                item["city"],
                item["state"],
                item["zip"],
                item["email"],
                item["phone"],
                item["mobile"],
                item["latitude"],
                item["longitude"],
                item["customer_id"],
                item["account_id"],
                item["notes"],
                format_date_fordb(item["created_at"]),
                format_date_fordb(item["updated_at"]),
                item["vendor_id"],
                item["properties"]["title"] if "title" in item["properties"] else None,
                item["opt_out"],
                item["extension"],
                item["processed_phone"],
                item["processed_mobile"],
                item["ticket_matching_emails"],
            )
            cursor.execute(sql, values)
    print(
        f"{log_ts()} Added {added} new contacts, updated {updated} existing contacts."
    )


def insert_comments(cursor, items, last_run_timestamp_unix):
    """Insert or update comments based on the items provided."""

    added = 0
    updated = 0
    comments_data = []

    for item in items:
        comments_data.extend(item.get("comments", "[]"))

    for comment in comments_data:
        # Check if the comment exists and get the current updated_at value
        cursor.execute(
            "SELECT updated_at FROM comments WHERE id = %s", (comment["id"],)
        )
        existing_comment = cursor.fetchone()
        ticket_id = comment["ticket_id"]

        if existing_comment:
            if rs_to_unix_timestamp(comment["updated_at"]) > last_run_timestamp_unix:
                # If comment exists and updated_at is greater, update it
                updated += 1
                sql = """
                    UPDATE comments SET
                        created_at = %s,
                        updated_at = %s,
                        ticket_id = %s,
                        subject = %s,
                        body = %s,
                        tech = %s,
                        hidden = %s,
                        user_id = %s
                    WHERE id = %s
                """
                values = (
                    format_date_fordb(comment["created_at"]),
                    format_date_fordb(comment["updated_at"]),
                    ticket_id,
                    comment["subject"],
                    comment["body"],
                    comment["tech"],
                    comment["hidden"],
                    comment["user_id"],
                    comment["id"],
                )
                cursor.execute(sql, values)
        else:
            # If comment doesn't exist, insert it
            added += 1
            sql = """
                INSERT INTO comments (
                    id, created_at, updated_at, ticket_id, subject,
                    body, tech, hidden, user_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                comment["id"],
                format_date_fordb(comment["created_at"]),
                format_date_fordb(comment["updated_at"]),
                ticket_id,
                comment["subject"],
                comment["body"],
                comment["tech"],
                comment["hidden"],
                comment["user_id"],
            )
            cursor.execute(sql, values)

    print(
        f"{log_ts()} Added {added} new comments, updated {updated} existing comments."
    )
    return comments_data


def compare_id_sums(cursor, data, table_name):
    """compare the id sums to make sure they match"""
    if table_name == "contacts":
        cursor.execute("SELECT SUM(id) FROM contacts")
        contacts_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(contact["id"] for contact in data)
        print(f"{log_ts()} Sum of IDs from API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from DB: {contacts_sum}")

        if sum_of_ids_api == contacts_sum:
            print(f"{log_ts()} Both ID sums are matching.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")

        return sum_of_ids_api == contacts_sum

    if table_name == "invoice_items":
        cursor.execute("SELECT SUM(id) FROM invoice_items")
        line_items_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(invoice_item["id"] for invoice_item in data)
        print(f"{log_ts()} Sum of IDs from API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from DB: {line_items_sum}")

        if sum_of_ids_api == line_items_sum:
            print(f"{log_ts()} Both ID sums are matching.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")

        print(log_ts(), sum_of_ids_api == line_items_sum)
        return sum_of_ids_api == line_items_sum

    if table_name == "tickets":
        cursor.execute("SELECT SUM(id) FROM tickets")
        tickets_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(ticket["id"] for ticket in data)
        print(f"{log_ts()} Sum of IDs from tickets API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from tickets DB: {tickets_sum}")

        if sum_of_ids_api == tickets_sum:
            print(f"{log_ts()} Ticket ID's match.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")
        return sum_of_ids_api == tickets_sum

    if table_name == "comments":
        cursor.execute("SELECT SUM(id) FROM comments")
        comments_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(comment["id"] for comment in data)
        print(f"{log_ts()} Sum of IDs from comments API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from comments DB: {comments_sum}")

        if sum_of_ids_api == comments_sum:
            print(f"{log_ts()} Comment ID's match.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")
        return sum_of_ids_api == comments_sum


def move_deleted_contacts_to_deleted_table(cursor, connection, data):
    """compare the id sums, and move any entries not
    in the data array to the deleted_contacts table"""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM contacts")
    contacts_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(contact["id"] for contact in data)
    print(f"{log_ts()} Sum of IDs from API: {sum_of_ids_api}")
    print(f"{log_ts()} Sum of IDs from DB: {contacts_sum}")

    if sum_of_ids_api != contacts_sum:
        deleted = 0
        print(
            f"{log_ts()} The sum of IDs does not match. Identifying deleted contacts..."
        )

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
                print(
                    f"{log_ts()} Moving contact with ID {db_id} to deleted_contacts table..."
                )

                # Copy the row to the deleted_contacts table
                cursor.execute(
                    """INSERT INTO deleted_contacts SELECT
                                * FROM contacts WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the contacts table
                cursor.execute("DELETE FROM contacts WHERE id = %s", (db_id,))
                deleted += 1

                connection.commit()

        print(f"{log_ts()} Operation completed successfully.")
        print(f"{log_ts()} Deleted {deleted} contacts.")
    else:
        print(f"{log_ts()} The sum of IDs matches.")


def move_deleted_lines_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_invoice_items table"""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM invoice_items")
    line_items_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(invoice_item["id"] for invoice_item in data)
    print(f"{log_ts()} Sum of IDs from API: {sum_of_ids_api}")
    print(f"{log_ts()} Sum of IDs from DB: {line_items_sum}")

    if sum_of_ids_api != line_items_sum:
        deleted = 0
        print(
            f"{log_ts()} The sum of IDs does not match. Identifying deleted invoice items..."
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_invoice_items (
                id INT PRIMARY KEY,
            created_at DATETIME,
            updated_at DATETIME,
            invoice_id INT,
            item VARCHAR(1024),
            name VARCHAR(4096),
            cost DECIMAL(10, 2),
            price DECIMAL(10, 2),
            quantity DECIMAL(10, 2),
            product_id INT,
            taxable BOOLEAN,
            discount_percent DECIMAL(5, 2),
            position INT,
            invoice_bundle_id INT,
            discount_dollars DECIMAL(10, 2),
            product_category VARCHAR(255)
        )
        """
        )

        # Get the set of IDs from the API data
        api_ids = {invoice_item["id"] for invoice_item in data}

        # Query all IDs from the invoice_items table
        cursor.execute("SELECT id FROM invoice_items")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                print(
                    f"{log_ts()} Moving invoice item with ID {db_id}"
                    "to deleted_invoice_items table..."
                )

                # Copy the row to the deleted_invoice_items table
                cursor.execute(
                    """INSERT INTO deleted_invoice_items SELECT
                                * FROM invoice_items WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the invoice_items table
                cursor.execute("DELETE FROM invoice_items WHERE id = %s", (db_id,))
                deleted += 1
                connection.commit()

        print(f"{log_ts()} Operation completed successfully.")
        print(f"{log_ts()} Deleted {deleted} invoice items.")
    else:
        print(f"{log_ts()} The sum of IDs matches.")


def move_deleted_tickets_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_tickets table"""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM tickets")
    tickets_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(ticket["id"] for ticket in data)
    print(f"{log_ts()} Sum of IDs from API: {sum_of_ids_api}")
    print(f"{log_ts()} Sum of IDs from DB: {tickets_sum}")

    if sum_of_ids_api != tickets_sum:
        deleted = 0
        print(
            f"{log_ts()} The sum of IDs does not match. Identifying deleted tickets..."
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_tickets (
                id INT PRIMARY KEY,
            number INT,
            subject VARCHAR(255),
            created_at DATETIME,
            customer_id INT,
            customer_business_then_name VARCHAR(255),
            due_date DATETIME,
            resolved_at DATETIME,
            start_at DATETIME,
            end_at DATETIME,
            location_id INT,
            problem_type VARCHAR(255),
            status VARCHAR(255),
            ticket_type_id INT,
            properties TEXT,
            user_id INT,
            updated_at DATETIME,
            pdf_url TEXT,
            priority VARCHAR(255),
            comments TEXT
        )
        """
        )

        # Get the set of IDs from the API data
        api_ids = {ticket["id"] for ticket in data}

        # Query all IDs from the tickets table
        cursor.execute("SELECT id FROM tickets")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                print(
                    f"{log_ts()} Moving ticket with ID {db_id}"
                    " and it's comments to deleted_tickets table,"
                    " and it's comments to deleted_comments table..."
                )

                # Copy the row to the deleted_tickets table
                # Assuming db_id is the ticket ID you're about to delete or move
                cursor.execute(
                    "INSERT INTO deleted_comments SELECT * FROM comments WHERE ticket_id = %s",
                    (db_id,),
                )
                affected_rows = cursor.rowcount
                print(f"Moved {affected_rows} comments to deleted_comments for ticket_id {db_id}.")

                cursor.execute("DELETE FROM comments WHERE ticket_id = %s", (db_id,))

                cursor.execute(
                    """INSERT INTO deleted_tickets SELECT
                                * FROM tickets WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the tickets table
                cursor.execute("DELETE FROM tickets WHERE id = %s", (db_id,))
                deleted += 1
                connection.commit()

        print(f"{log_ts()} Operation completed successfully.")
        print(f"{log_ts()} {deleted} tickets were deleted.")
    else:
        print(f"{log_ts()} The sum of IDs matches.")


def move_deleted_comments_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_comments table"""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM comments")
    comments_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(comment["id"] for comment in data)
    print(f"{log_ts()} Sum of IDs from API: {sum_of_ids_api}")
    print(f"{log_ts()} Sum of IDs from DB: {comments_sum}")

    if sum_of_ids_api != comments_sum:
        deleted = 0
        print(
            f"{log_ts()} The sum of IDs does not match. Identifying deleted comments..."
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_comments (
            id INT PRIMARY KEY,
            created_at DATETIME,
            updated_at DATETIME,
            ticket_id INT,
            subject VARCHAR(255),
            body TEXT,
            tech VARCHAR(255),
            hidden BOOLEAN,
            user_id INT
        )
        """
        )

        all_comments = []
        for item in data:
            ticket_id = item["id"]
            comments_data = item.get("comments", [])
            for comment in comments_data:
                comment[
                    "ticket_id"
                ] = ticket_id  # Attach ticket_id to each comment for later use
            all_comments.extend(comments_data)

        # Get the set of IDs from the API data
        api_ids = {comment["id"] for comment in all_comments}

        # Query all IDs from the comments table
        cursor.execute("SELECT id FROM comments")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                print(
                    f"{log_ts()} Moving comment with ID {db_id}"
                    " to deleted_comments table..."
                )

                # Copy the row to the deleted_comments table
                cursor.execute(
                    """INSERT INTO deleted_comments SELECT
                                * FROM comments WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the comments table
                cursor.execute("DELETE FROM comments WHERE id = %s", (db_id,))
                deleted += 1
                connection.commit()

        print(f"{log_ts()} Operation completed successfully.")
        print(f"{log_ts()} {deleted} comments were deleted.")
    else:
        print(f"{log_ts()} The sum of IDs matches.")

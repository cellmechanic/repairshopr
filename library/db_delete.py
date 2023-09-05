"""DB delete functions"""
from library.loki_library import start_loki


def move_deleted_contacts_to_deleted_table(cursor, connection, data):
    """compare the id sums, and move any entries not
    in the data array to the deleted_contacts table"""
    logger = start_loki("__move_deleted_contacts_to_deleted_table__")
    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM contacts")
    contacts_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(contact["id"] for contact in data)
    logger.warning(
        "Sum of IDs from contacts API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_contacts_to_deleted_table"}},
    )
    logger.warning(
        "Sum of IDs from contacts DB: %s",
        contacts_sum,
        extra={"tags": {"service": "move_deleted_contacts_to_deleted_table"}},
    )

    if sum_of_ids_api != contacts_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted contacts...",
            extra={"tags": {"service": "move_deleted_contacts_to_deleted_table"}},
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
                logger.info(
                    "Moving contact with ID %s to deleted_contacts table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_contacts_to_deleted_table",
                            "finished": "yes",
                        }
                    },
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

        logger.warning(
            "Deleted %s contacts.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_contacts_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
    else:
        logger.info(
            "The sum of contact IDs matches.",
            extra={"tags": {"service": "move_deleted_contacts_to_deleted_table"}},
        )


def move_deleted_customers_to_deleted_table(cursor, connection, data):
    """compare the id sums, and move any entries not
    in the data array to the deleted_customers table"""

    logger = start_loki("__move_deleted_customers_to_deleted_table__")

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM customers")
    customers_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(customer["id"] for customer in data)
    logger.info(
        "Sum of customer IDs from DB: %s",
        customers_sum,
        extra={"tags": {"service": "move_deleted_customers_to_deleted_table"}},
    )
    logger.info(
        "Sum of customer IDs from API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_customers_to_deleted_table"}},
    )

    if sum_of_ids_api != customers_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted customers...",
            extra={"tags": {"service": "move_deleted_customers_to_deleted_table"}},
        )

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_customers (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            fullname VARCHAR(255),
            business_name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(255),
            mobile VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME,
            pdf_url TEXT,
            address VARCHAR(255),
            address_2 VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            zip VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            notes TEXT,
            get_sms BOOLEAN,
            opt_out BOOLEAN,
            disabled BOOLEAN,
            no_email BOOLEAN,
            location_name VARCHAR(255),
            location_id INT,
            properties JSON,
            online_profile_url TEXT,
            tax_rate_id INT,
            notification_email VARCHAR(255),
            invoice_cc_emails VARCHAR(255),
            invoice_term_id INT,
            referred_by VARCHAR(255),
            ref_customer_id INT,
            business_and_full_name VARCHAR(255),
            business_then_name VARCHAR(255),
            contacts JSON
    )
    """
        )

        # Get the set of IDs from the API data
        api_ids = {customer["id"] for customer in data}

        # Query all IDs from the customers table
        cursor.execute("SELECT id FROM customers")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                logger.warning(
                    "Moving customer with ID %s to deleted_customers table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_customers_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )

                # Copy the row to the deleted_contacts table
                cursor.execute(
                    """INSERT INTO deleted_customers SELECT
                                * FROM customers WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the contacts table
                cursor.execute("DELETE FROM customers WHERE id = %s", (db_id,))
                deleted += 1

                connection.commit()

        logger.warning(
            "Deleted %s customers.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_customers_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
    else:
        logger.info(
            "The sum of customer IDs matches.",
            extra={"tags": {"service": "move_deleted_customers_to_deleted_table"}},
        )


def move_deleted_lines_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_invoice_items table"""

    logger = start_loki("__move_deleted_lines_to_deleted_table__")

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM invoice_items")
    line_items_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(invoice_item["id"] for invoice_item in data)
    logger.info(
        "Sum of line_items IDs from DB: %s",
        line_items_sum,
        extra={"tags": {"service": "move_deleted_lines_to_deleted_table"}},
    )
    logger.info(
        "Sum of line_items IDs from API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_lines_to_deleted_table"}},
    )

    if sum_of_ids_api != line_items_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted invoice items...",
            extra={"tags": {"service": "move_deleted_lines_to_deleted_table"}},
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
                logger.warning(
                    "Moving invoice item with ID %s to deleted_invoice_items table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_lines_to_deleted_table",
                            "finished": "yes",
                        }
                    },
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

        logger.warning(
            "Deleted %s invoice items.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_lines_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
    else:
        logger.info(
            "The sum of line_items IDs matches.",
            extra={"tags": {"service": "move_deleted_lines_to_deleted_table"}},
        )


def move_deleted_tickets_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_tickets table"""

    logger = start_loki("__move_deleted_tickets_to_deleted_table__")

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM tickets")
    tickets_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(ticket["id"] for ticket in data)
    logger.info(
        "Sum of ticket IDs from DB: %s",
        tickets_sum,
        extra={"tags": {"service": "move_deleted_tickets_to_deleted_table"}},
    )
    logger.info(
        "Sum of ticket IDs from API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_tickets_to_deleted_table"}},
    )

    if sum_of_ids_api != tickets_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted tickets...",
            extra={"tags": {"service": "move_deleted_tickets_to_deleted_table"}},
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
                logger.warning(
                    "Moving ticket with ID %s to deleted_tickets table...,"
                    " and it's comments to deleted_comments table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_tickets_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )
                # Copy the row to the deleted_tickets table
                # Assuming db_id is the ticket ID you're about to delete or move
                cursor.execute(
                    "INSERT INTO deleted_comments SELECT * FROM comments WHERE ticket_id = %s",
                    (db_id,),
                )
                affected_rows = cursor.rowcount
                logger.warning(
                    "Moved %s comments to deleted_comments for ticket_id %s.",
                    affected_rows,
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_tickets_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )

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

        logger.warning(
            "Deleted %s tickets.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_tickets_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
    else:
        logger.info(
            "The sum of ticket IDs matches.",
            extra={"tags": {"service": "move_deleted_tickets_to_deleted_table"}},
        )


def move_deleted_comments_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_comments table"""

    logger = start_loki("__move_deleted_comments_to_deleted_table__")

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM comments")
    comments_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(comment["id"] for comment in data)
    logger.info(
        "Sum of comment IDs from DB: %s",
        comments_sum,
        extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
    )
    logger.info(
        "Sum of comment IDs from API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
    )

    if sum_of_ids_api != comments_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted comments...",
            extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
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
                logger.warning(
                    "Moving comment with ID %s to deleted_comments table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_comments_to_deleted_table",
                            "finished": "yes",
                        }
                    },
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

        logger.warning(
            "Deleted %s comments.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_comments_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
    else:
        logger.info(
            "The sum of comment IDs matches.",
            extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
        )


def move_deleted_estimates_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_estimates table"""

    logger = start_loki("__move_deleted_estimates_to_deleted_table__")

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM estimates")
    estimates_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(item["id"] for item in data)
    logger.info(
        "Sum of IDs from estimates DB: %s",
        estimates_sum,
        extra={"tags": {"service": "move_deleted_estimates_to_deleted_table"}},
    )
    logger.info(
        "Sum of IDs from estimates API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_estimates_to_deleted_table"}},
    )

    if sum_of_ids_api != estimates_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted estimates...",
            extra={"tags": {"service": "move_deleted_estimates_to_deleted_table"}},
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_estimates (
            id INT PRIMARY KEY,
            customer_id INT,
            customer_business_then_name VARCHAR(255),
            number VARCHAR(255),
            status VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME,
            date DATE,
            subtotal FLOAT,
            total FLOAT,
            tax FLOAT,
            ticket_id INT,
            pdf_url VARCHAR(255),
            location_id INT,
            invoice_id INT,
            employee VARCHAR(255)
        )
        """
        )

        # Get the set of IDs from the API data
        api_ids = {item["id"] for item in data}

        # Query all IDs from the estimates table
        cursor.execute("SELECT id FROM estimates")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                logger.warning(
                    "Moving estimate with ID %s to deleted_estimates table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_estimates_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )

                # Copy the row to the deleted_estimates table
                cursor.execute(
                    """INSERT INTO deleted_estimates SELECT
                                * FROM estimates WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the estimates table
                cursor.execute("DELETE FROM estimates WHERE id = %s", (db_id,))
                deleted += 1
                connection.commit()

        logger.warning(
            "Deleted %s estimates.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_estimates_to_deleted_table",
                    "finished": "yes",
                }
            },
        )


def move_deleted_invoices_to_deleted_table(cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_invoices table"""

    logger = start_loki("__move_deleted_invoices_to_deleted_table__")

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM invoices")
    invoices_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(item["id"] for item in data)
    logger.info(
        "Sum of IDs from invoices DB: %s",
        invoices_sum,
        extra={"tags": {"service": "move_deleted_invoices_to_deleted_table"}},
    )
    logger.info(
        "Sum of IDs from invoices API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_invoices_to_deleted_table"}},
    )

    if sum_of_ids_api != invoices_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted invoices...",
            extra={"tags": {"service": "move_deleted_invoices_to_deleted_table"}},
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_invoices (
            id INT PRIMARY KEY,
            customer_id INT,
            customer_business_then_name VARCHAR(255),
            number VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME,
            date DATE,
            due_date DATE,
            subtotal FLOAT,
            total FLOAT,
            tax FLOAT,
            verified_paid BOOLEAN,
            tech_marked_paid BOOLEAN,
            ticket_id INT,
            user_id INT,
            pdf_url VARCHAR(255),
            is_paid BOOLEAN,
            location_id INT,
            po_number VARCHAR(255),
            contact_id INT,
            note TEXT,
            hardwarecost FLOAT
        )
        """
        )

        # Get the set of IDs from the API data
        api_ids = {item["id"] for item in data}

        # Query all IDs from the invoices table
        cursor.execute("SELECT id FROM invoices")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                logger.warning(
                    "Moving invoice with ID %s to deleted_invoices table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_invoices_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )

                # Copy the row to the deleted_invoices table
                cursor.execute(
                    """INSERT INTO deleted_invoices SELECT
                                * FROM invoices WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the invoices table
                cursor.execute("DELETE FROM invoices WHERE id = %s", (db_id,))
                deleted += 1
                connection.commit()

        logger.warning(
            "Deleted %s invoices.",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_invoices_to_deleted_table",
                    "finished": "yes",
                }
            },
        )

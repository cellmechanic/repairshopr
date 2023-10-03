#! /usr/bin/env python # pylint: disable=C0302
"""DB delete functions"""


def move_deleted_contacts_to_deleted_table(logger, cursor, connection, data):
    """compare the id sums, and move any entries not
    in the data array to the deleted_contacts table"""

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
            "Deleted %s contact(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_contacts_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s contact(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_contacts_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of contact IDs matches.",
            extra={"tags": {"service": "move_deleted_contacts_to_deleted_table"}},
        )


def move_deleted_customers_to_deleted_table(logger, cursor, connection, data):
    """compare the id sums, and move any entries not
    in the data array to the deleted_customers table"""

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
            "Deleted %s customer(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_customers_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s customer(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_customers_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of customer IDs matches.",
            extra={"tags": {"service": "move_deleted_customers_to_deleted_table"}},
        )


def move_deleted_lines_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_invoice_items table"""
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
            "Deleted %s invoice item(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_lines_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s invoice item(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_lines_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of line_items IDs matches.",
            extra={"tags": {"service": "move_deleted_lines_to_deleted_table"}},
        )


def move_deleted_tickets_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_tickets table"""

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
            comments TEXT,
            num_devices INT
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

        logger.warning(
            "Deleted %s ticket(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_tickets_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s ticket(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_tickets_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of ticket IDs matches.",
            extra={"tags": {"service": "move_deleted_tickets_to_deleted_table"}},
        )
    connection.commit()


def move_deleted_comments_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_comments table"""

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
                # Copy the row to the deleted_comments table
                cursor.execute(
                    """INSERT INTO deleted_comments SELECT
                                * FROM comments WHERE id = %s""",
                    (db_id,),
                )
                # Delete the row from the comments table
                cursor.execute("DELETE FROM comments WHERE id = %s", (db_id,))
                if cursor.rowcount > 0:
                    logger.warning(
                        "Removing comment with ID %s from comments table...",
                        db_id,
                        extra={
                            "tags": {
                                "service": "move_deleted_comments_to_deleted_table",
                                "finished": "yes",
                            }
                        },
                    )
                cursor.execute("DELETE FROM employee_output WHERE comment_id = %s", (db_id,))
                if cursor.rowcount > 0:
                    logger.warning(
                        "Removing comment with ID %s from employee_output table...",
                        db_id,
                        extra={
                            "tags": {
                                "service": "move_deleted_comments_to_deleted_table",
                                "finished": "yes",
                            }
                        },
                    )
                cursor.execute("DELETE FROM employee_output WHERE linked_comment_id = %s", (db_id,))
                if cursor.rowcount > 0:
                    logger.warning(
                        "Removing comment with ID %s from employee_output table, linked column...",
                        db_id,
                        extra={
                            "tags": {
                                "service": "move_deleted_comments_to_deleted_table",
                                "finished": "yes",
                            }
                        },
                    )
                deleted += 1

        logger.warning(
            "Deleted %s comment(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_comments_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s comment(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_comments_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of comment IDs matches.",
            extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
        )
    connection.commit()


def move_deleted_comments_to_deleted_table_frequent_only(
    logger, cursor, connection, api_data, db_data
):
    """Compare the comments between API and database data, and move any entries not
    in the API data to the deleted_comments table"""

    # Extract comment IDs from the API data
    sum_of_ids_api = sum(comment["id"] for comment in api_data)

    # Extract comment IDs from the DB data
    sum_of_ids_db = sum(comment["id"] for comment in db_data)

    logger.info(
        "Sum of comment IDs from DB: %s",
        sum_of_ids_db,
        extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
    )
    logger.info(
        "Sum of comment IDs from API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_comments_to_deleted_table"}},
    )

    if sum_of_ids_api != sum_of_ids_db:
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

        # Get the set of IDs from the API data
        api_ids = {comment["id"] for comment in api_data}

        # Query all IDs from the comments table
        db_ids = [comment["id"] for comment in db_data]

        # Check for IDs that are in the DB but not in the API data
        for db_comment_id in db_ids:
            if db_comment_id not in api_ids:
                logger.warning(
                    "Moving comment with ID %s to deleted_comments table...",
                    db_comment_id,
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
                    (db_comment_id,),
                )
                # Delete the row from the comments table
                cursor.execute("DELETE FROM comments WHERE id = %s", (db_comment_id,))
                if cursor.rowcount > 0:
                    logger.warning(
                        "Removing comment with ID %s from comments table...",
                        db_comment_id,
                        extra={
                            "tags": {
                                "service": "move_deleted_comments_to_deleted_table",
                                "finished": "yes",
                            }
                        },
                    )
                cursor.execute("DELETE FROM employee_output WHERE comment_id = %s", (db_comment_id,))
                if cursor.rowcount > 0:
                    logger.warning(
                        "Removing comment with ID %s from employee_output table...",
                        db_comment_id,
                        extra={
                            "tags": {
                                "service": "move_deleted_comments_to_deleted_table",
                                "finished": "yes",
                            }
                        },
                    )
                cursor.execute("DELETE FROM employee_output WHERE linked_comment_id = %s", (db_comment_id,))
                if cursor.rowcount > 0:
                    logger.warning(
                        "Removing comment with ID %s from employee_output table, linked column...",
                        db_comment_id,
                        extra={
                            "tags": {
                                "service": "move_deleted_comments_to_deleted_table",
                                "finished": "yes",
                            }
                        },
                    )
                deleted += 1

        logger.warning(
            "Deleted %s comment(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_comments_to_deleted_table",
                    "finished": "yes",
                }
            },
        )

        connection.commit()


def move_deleted_estimates_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_estimates table"""

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
                logger.info(
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
            "Deleted %s estimate(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_estimates_to_deleted_table",
                    "finished": "yes",
                }
            },
        )

        logger.warning(
            "Deleted %s estimate(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_estimates_to_deleted_table",
                    "finished": "full",
                }
            },
        )


def move_deleted_invoices_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_invoices table"""

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
            "Deleted %s invoice(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_invoices_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s invoice(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_invoices_to_deleted_table",
                    "finished": "full",
                }
            },
        )


def move_deleted_products_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums, and move any entries not
    in the data array to the deleted_products table."""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM products")
    products_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(product["id"] for product in data)
    logger.warning(
        "Sum of IDs from products API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_products_to_deleted_table"}},
    )
    logger.warning(
        "Sum of IDs from products DB: %s",
        products_sum,
        extra={"tags": {"service": "move_deleted_products_to_deleted_table"}},
    )

    if sum_of_ids_api != products_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted products...",
            extra={"tags": {"service": "move_deleted_products_to_deleted_table"}},
        )

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_products (
                id INT PRIMARY KEY,
                price_cost FLOAT,
                price_retail FLOAT,
                `condition` VARCHAR(255),
                description TEXT,
                maintain_stock BOOLEAN,
                name VARCHAR(255),
                quantity INT,
                warranty TEXT,
                sort_order INT,
                reorder_at INT,
                disabled BOOLEAN,
                taxable BOOLEAN,
                product_category VARCHAR(255),
                category_path VARCHAR(255),
                upc_code VARCHAR(255),
                discount_percent FLOAT,
                warranty_template_id INT,
                qb_item_id INT,
                desired_stock_level INT,
                price_wholesale FLOAT,
                notes TEXT,
                tax_rate_id INT,
                physical_location TEXT,
                serialized BOOLEAN,
                vendor_ids JSON,
                long_description TEXT,
                location_quantities JSON,
                photos JSON,
                hash VARCHAR(32)
            )
            """
        )

        # Get the set of IDs from the API data
        api_ids = {product["id"] for product in data}

        # Query all IDs from the products table
        cursor.execute("SELECT id FROM products")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                logger.info(
                    "Moving product with ID %s to deleted_products table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_products_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )

                # Copy the row to the deleted_products table
                cursor.execute(
                    """INSERT INTO deleted_products SELECT
                                * FROM products WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the products table
                cursor.execute("DELETE FROM products WHERE id = %s", (db_id,))
                deleted += 1

                connection.commit()

        logger.warning(
            "Deleted %s product(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_products_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s product(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_products_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of product IDs matches.",
            extra={"tags": {"service": "move_deleted_products_to_deleted_table"}},
        )


def move_deleted_payments_to_deleted_table(logger, cursor, connection, data):
    """Compare the id sums and move any entries not
    in the data array to the deleted_payments table."""

    # Get the sum of the IDs from the database
    cursor.execute("SELECT SUM(id) FROM payments")
    payments_sum = cursor.fetchone()[0]

    # Get the sum of the IDs from the API data
    sum_of_ids_api = sum(payment["id"] for payment in data)
    logger.warning(
        "Sum of IDs from payments API: %s",
        sum_of_ids_api,
        extra={"tags": {"service": "move_deleted_payments_to_deleted_table"}},
    )
    logger.warning(
        "Sum of IDs from payments DB: %s",
        payments_sum,
        extra={"tags": {"service": "move_deleted_payments_to_deleted_table"}},
    )

    if sum_of_ids_api != payments_sum:
        deleted = 0
        logger.warning(
            "The sum of IDs does not match. Identifying deleted payments...",
            extra={"tags": {"service": "move_deleted_payments_to_deleted_table"}},
        )

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_payments (
                id INT PRIMARY KEY,
                created_at DATETIME,
                updated_at DATETIME,
                success BOOLEAN,
                payment_amount FLOAT,
                invoice_ids JSON,
                ref_num VARCHAR(255),
                applied_at DATE,
                payment_method VARCHAR(255),
                transaction_response TEXT,
                signature_date DATE,
                customer JSON,
                customer_id INT,
                business_and_full_name VARCHAR(510)
            )
            """
        )

        # Get the set of IDs from the API data
        api_ids = {payment["id"] for payment in data}

        # Query all IDs from the payments table
        cursor.execute("SELECT id FROM payments")
        db_ids = cursor.fetchall()

        # Check for IDs that are in the DB but not in the API data
        for (db_id,) in db_ids:
            if db_id not in api_ids:
                logger.info(
                    "Moving payment with ID %s to deleted_payments table...",
                    db_id,
                    extra={
                        "tags": {
                            "service": "move_deleted_payments_to_deleted_table",
                            "finished": "yes",
                        }
                    },
                )

                # Copy the row to the deleted_payments table
                cursor.execute(
                    """INSERT INTO deleted_payments SELECT
                                * FROM payments WHERE id = %s""",
                    (db_id,),
                )

                # Delete the row from the payments table
                cursor.execute("DELETE FROM payments WHERE id = %s", (db_id,))
                deleted += 1

                connection.commit()

        logger.warning(
            "Deleted %s payment(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_payments_to_deleted_table",
                    "finished": "yes",
                }
            },
        )
        logger.warning(
            "Deleted %s payment(s).",
            deleted,
            extra={
                "tags": {
                    "service": "move_deleted_payments_to_deleted_table",
                    "finished": "full",
                }
            },
        )
    else:
        logger.info(
            "The sum of payment IDs matches.",
            extra={"tags": {"service": "move_deleted_payments_to_deleted_table"}},
        )

"""DB insert functions"""
import json
from library.db_general import extract_devices
from library.db_hash import compute_hash
from library.fix_date_time import (
    rs_to_unix_timestamp,
    format_date_fordb,
)


def insert_invoice_lines(logger, cursor, items, last_run_timestamp_unix):
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
                logger.info(
                    "Line item %s has been updated since last run.",
                    item["id"],
                    extra={"tags": {"service": "invoice_lines"}},
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
            logger.info(
                "Inserting new line item with ID: %s",
                item["id"],
                extra={"tags": {"service": "invoice_lines"}},
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new line items, updated %s existing line items.",
        added,
        updated,
        extra={"tags": {"service": "invoice_lines", "finished": "yes"}},
    )


def insert_tickets(logger, cursor, items, last_run_timestamp_unix):
    """Insert or update tickets based on the items provided."""
    added = 0
    updated = 0

    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM tickets WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()
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
                        comments = %s,
                        num_devices = %s
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
                    extract_devices(item["subject"]),
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
                    properties, user_id, updated_at, pdf_url, priority, comments, num_devices
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                extract_devices(item["subject"])
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new tickets, updated %s existing tickets.",
        added,
        updated,
        extra={"tags": {"service": "tickets", "finished": "yes"}},
    )


def insert_estimates(logger, cursor, items, last_run_timestamp_unix):
    """Insert or update estimates based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM estimates WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()

        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                # If record exists and updated_at is greater, update it
                updated += 1
                sql = """
                    UPDATE estimates SET
                        customer_id = %s,
                        customer_business_then_name = %s,
                        number = %s,
                        status = %s,
                        created_at = %s,
                        updated_at = %s,
                        date = %s,
                        subtotal = %s,
                        total = %s,
                        tax = %s,
                        ticket_id = %s,
                        pdf_url = %s,
                        location_id = %s,
                        invoice_id = %s,
                        employee = %s
                    WHERE id = %s
                """
                values = (
                    item["customer_id"],
                    item["customer_business_then_name"],
                    item["number"],
                    item["status"],
                    format_date_fordb(item["created_at"]),
                    format_date_fordb(item["updated_at"]),
                    format_date_fordb(item["date"]),
                    item["subtotal"],
                    item["total"],
                    item["tax"],
                    item["ticket_id"],
                    item["pdf_url"],
                    item["location_id"],
                    item["invoice_id"],
                    item["employee"],
                    item["id"],
                )
                cursor.execute(sql, values)
        else:
            # If record doesn't exist, insert it
            added += 1
            sql = """
                INSERT INTO estimates (
                    id, customer_id, customer_business_then_name, number, status,
                    created_at, updated_at, date, subtotal, total, tax, ticket_id,
                    pdf_url, location_id, invoice_id, employee
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["customer_id"],
                item["customer_business_then_name"],
                item["number"],
                item["status"],
                format_date_fordb(item["created_at"]),
                format_date_fordb(item["updated_at"]),
                format_date_fordb(item["date"]),
                item["subtotal"],
                item["total"],
                item["tax"],
                item["ticket_id"],
                item["pdf_url"],
                item["location_id"],
                item["invoice_id"],
                item["employee"],
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new estimates, updated %s existing estimates.",
        added,
        updated,
        extra={"tags": {"service": "estimates", "finished": "yes"}},
    )


def insert_payments(logger, cursor, items, last_run_timestamp_unix):
    """Insert or update payments based on the items provided."""
    added = 0
    updated = 0

    for item in items:
        # Pull customer ID out
        customer_id = item["customer"].get("id")
        business_and_full_name = item["customer"].get("business_and_full_name")

        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM payments WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()

        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                # If record exists and updated_at is greater, update it
                updated += 1
                sql = """
                    UPDATE payments SET
                        created_at = %s,
                        updated_at = %s,
                        success = %s,
                        payment_amount = %s,
                        invoice_ids = %s,
                        ref_num = %s,
                        applied_at = %s,
                        payment_method = %s,
                        transaction_response = %s,
                        signature_date = %s,
                        customer = %s,
                        customer_id = %s,
                        business_and_full_name = %s
                    WHERE id = %s
                """
                values = (
                    format_date_fordb(item["created_at"]),
                    format_date_fordb(item["updated_at"]),
                    item["success"],
                    item["payment_amount"],
                    json.dumps(item["invoice_ids"]),
                    item["ref_num"],
                    format_date_fordb(item["applied_at"]),
                    item["payment_method"],
                    item["transaction_response"],
                    format_date_fordb(item["signature_date"]),
                    json.dumps(item["customer"]),
                    customer_id,
                    business_and_full_name,
                    item["id"],
                )
                cursor.execute(sql, values)
        else:
            # If record doesn't exist, insert it
            added += 1
            sql = """
                INSERT INTO payments (
                    id, created_at, updated_at, success, payment_amount, invoice_ids, 
                    ref_num, applied_at, payment_method, transaction_response, 
                    signature_date, customer, customer_id, business_and_full_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                format_date_fordb(item["created_at"]),
                format_date_fordb(item["updated_at"]),
                item["success"],
                item["payment_amount"],
                json.dumps(item["invoice_ids"]),
                item["ref_num"],
                format_date_fordb(item["applied_at"]),
                item["payment_method"],
                item["transaction_response"],
                format_date_fordb(item["signature_date"]),
                json.dumps(item["customer"]),
                customer_id,
                business_and_full_name,
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new payments, updated %s existing payments.",
        added,
        updated,
        extra={"tags": {"service": "payments", "finished": "yes"}},
    )


def insert_contacts(logger, cursor, items, last_run_timestamp_unix):
    """Insert of update contacts based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM contacts WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()
        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                logger.info(
                    "Contact %s has been updated since last run.",
                    item["name"],
                    extra={"tags": {"service": "insert_contacts", "finished": "yes"}},
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
                logger.info(
                    "All data received from %s page(s)",
                    len(items) % 25,
                    extra={"tags": {"service": "insert_contacts", "finished": "yes"}},
                )
        else:
            added += 1
            sql = """
                INSERT INTO contacts (
                    id, name, address1, address2, city, state, zip, 
                    email, phone, mobile, latitude, longitude, customer_id,
                    account_id, notes, created_at, updated_at, vendor_id, title, 
                    opt_out, extension, processed_phone,
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
    logger.info(
        "Added %s new contacts, updated %s existing contacts.",
        added,
        updated,
        extra={"tags": {"service": "insert_contacts", "finished": "yes"}},
    )


def insert_customers(logger, cursor, items, last_run_timestamp_unix):
    """Insert or update customers based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM customers WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()
        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                updated += 1
                sql = """
                    UPDATE customers SET 
                        firstname = %s,
                        lastname = %s,
                        fullname = %s,
                        business_name = %s,
                        email = %s,
                        phone = %s,
                        mobile = %s,
                        created_at = %s,
                        updated_at = %s,
                        address = %s,
                        address_2 = %s,
                        city = %s,
                        state = %s,
                        zip = %s,
                        latitude = %s,
                        longitude = %s,
                        contacts = %s,
                        notes = %s,
                        get_sms = %s,
                        opt_out = %s,
                        disabled = %s,
                        no_email = %s,
                        properties = %s,
                        referred_by = %s
                    WHERE id = %s
                """
                values = (
                    item["firstname"],
                    item["lastname"],
                    item["fullname"],
                    item["business_name"],
                    item["email"],
                    item["phone"],
                    item["mobile"],
                    format_date_fordb(item["created_at"]),
                    format_date_fordb(item["updated_at"]),
                    item["address"],
                    item["address_2"],
                    item["city"],
                    item["state"],
                    item["zip"],
                    item["latitude"],
                    item["longitude"],
                    json.dumps(
                        item["contacts"]
                    ),  # Convert contacts list to JSON string
                    item["notes"],
                    item["get_sms"],
                    item["opt_out"],
                    item["disabled"],
                    item["no_email"],
                    json.dumps(item["properties"]),
                    item["referred_by"],
                    item["id"],
                )
                cursor.execute(sql, values)
        else:
            added += 1
            sql = """
                INSERT INTO customers (
                    id, firstname, lastname, fullname, business_name, email, phone, mobile, 
                    created_at, updated_at, address, address_2, city, state, zip, latitude, longitude, 
                    contacts, notes, get_sms, opt_out, disabled, no_email, properties, referred_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["firstname"],
                item["lastname"],
                item["fullname"],
                item["business_name"],
                item["email"],
                item["phone"],
                item["mobile"],
                format_date_fordb(item["created_at"]),
                format_date_fordb(item["updated_at"]),
                item["address"],
                item["address_2"],
                item["city"],
                item["state"],
                item["zip"],
                item["latitude"],
                item["longitude"],
                json.dumps(item["contacts"]),  # Convert contacts list to JSON string
                item["notes"],
                item["get_sms"],
                item["opt_out"],
                item["disabled"],
                item["no_email"],
                json.dumps(item["properties"]),
                item["referred_by"],
            )
            cursor.execute(sql, values)
    logger.info(
        "Added %s new customers, updated %s existing customers.",
        added,
        updated,
        extra={"tags": {"service": "insert_customers", "finished": "yes"}},
    )


def insert_comments(logger, cursor, items, last_run_timestamp_unix):
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

    logger.info(
        "Added %s new comments, updated %s existing comments.",
        added,
        updated,
        extra={"tags": {"service": "insert_comments", "finished": "yes"}},
    )
    return comments_data


def insert_invoices(logger, cursor, items, last_run_timestamp_unix):
    """Insert or update invoices based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Check if the record exists and get the current updated_at value
        cursor.execute("SELECT updated_at FROM invoices WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()

        if existing_record:
            if rs_to_unix_timestamp(item["updated_at"]) > last_run_timestamp_unix:
                # If record exists and updated_at is greater, update it
                updated += 1
                sql = """
                    UPDATE invoices SET
                        customer_id = %s,
                        customer_business_then_name = %s,
                        number = %s,
                        created_at = %s,
                        updated_at = %s,
                        date = %s,
                        due_date = %s,
                        subtotal = %s,
                        total = %s,
                        tax = %s,
                        verified_paid = %s,
                        tech_marked_paid = %s,
                        ticket_id = %s,
                        user_id = %s,
                        pdf_url = %s,
                        is_paid = %s,
                        location_id = %s,
                        po_number = %s,
                        contact_id = %s,
                        note = %s,
                        hardwarecost = %s
                    WHERE id = %s
                """
                values = (
                    item["customer_id"],
                    item["customer_business_then_name"],
                    item["number"],
                    format_date_fordb(item["created_at"]),
                    format_date_fordb(item["updated_at"]),
                    format_date_fordb(item["date"]),
                    format_date_fordb(item["due_date"]),
                    item["subtotal"],
                    item["total"],
                    item["tax"],
                    item["verified_paid"],
                    item["tech_marked_paid"],
                    item["ticket_id"],
                    item["user_id"],
                    item["pdf_url"],
                    item["is_paid"],
                    item["location_id"],
                    item["po_number"],
                    item["contact_id"],
                    item["note"],
                    item["hardwarecost"],
                    item["id"],
                )
                cursor.execute(sql, values)
        else:
            # If record doesn't exist, insert it
            added += 1
            sql = """
                INSERT INTO invoices (
                    id, customer_id, customer_business_then_name, number, created_at, updated_at,
                    date, due_date, subtotal, total, tax, verified_paid, tech_marked_paid, ticket_id,
                    user_id, pdf_url, is_paid, location_id, po_number, contact_id, note, hardwarecost
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["customer_id"],
                item["customer_business_then_name"],
                item["number"],
                format_date_fordb(item["created_at"]),
                format_date_fordb(item["updated_at"]),
                format_date_fordb(item["date"]),
                format_date_fordb(item["due_date"]),
                item["subtotal"],
                item["total"],
                item["tax"],
                item["verified_paid"],
                item["tech_marked_paid"],
                item["ticket_id"],
                item["user_id"],
                item["pdf_url"],
                item["is_paid"],
                item["location_id"],
                item["po_number"],
                item["contact_id"],
                item["note"],
                item["hardwarecost"],
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new invoices, updated %s existing invoices.",
        added,
        updated,
        extra={"tags": {"service": "insert_invoices", "finished": "yes"}},
    )


def insert_products(logger, cursor, items):
    """Insert or update products based on the items provided."""
    added = 0
    updated = 0
    for item in items:
        # Hash current item
        current_hash = compute_hash(item)

        # Check existing record, hash, compare
        cursor.execute("SELECT hash FROM products WHERE id = %s", (item["id"],))
        existing_record = cursor.fetchone()

        if existing_record:
            if existing_record[0] != current_hash:
                # If record exists and has a different hash, update
                updated += 1
                sql = """
                        UPDATE products SET
                            price_cost = %s,
                            price_retail = %s,
                            `condition` = %s,
                            description = %s,
                            maintain_stock = %s,
                            name = %s,
                            quantity = %s,
                            warranty = %s,
                            sort_order = %s,
                            reorder_at = %s,
                            disabled = %s,
                            taxable = %s,
                            product_category = %s,
                            category_path = %s,
                            upc_code = %s,
                            discount_percent = %s,
                            warranty_template_id = %s,
                            qb_item_id = %s,
                            desired_stock_level = %s,
                            price_wholesale = %s,
                            notes = %s,
                            tax_rate_id = %s,
                            physical_location = %s,
                            serialized = %s,
                            vendor_ids = %s,
                            long_description = %s,
                            location_quantities = %s,
                            photos = %s,
                            hash = %s
                        WHERE id = %s
                    """
                values = (
                    item["price_cost"],
                    item["price_retail"],
                    item.get(
                        "condition", ""
                    ),  # Using get() for optional fields to provide a default
                    item["description"],
                    item["maintain_stock"],
                    item["name"],
                    item["quantity"],
                    item.get("warranty", None),
                    item.get("sort_order", None),
                    item.get("reorder_at", None),
                    item["disabled"],
                    item["taxable"],
                    item["product_category"],
                    item["category_path"],
                    item.get("upc_code", ""),
                    item.get("discount_percent", None),
                    item.get("warranty_template_id", None),
                    item.get("qb_item_id", None),
                    item.get("desired_stock_level", None),
                    item["price_wholesale"],
                    item.get("notes", ""),
                    item.get("tax_rate_id", None),
                    item.get("physical_location", ""),
                    item["serialized"],
                    json.dumps(item["vendor_ids"]),  # Convert list to JSON string
                    item.get("long_description", ""),
                    json.dumps(
                        item["location_quantities"]
                    ),  # Convert list to JSON string
                    json.dumps(item["photos"]),  # Convert list to JSON string
                    current_hash,
                    item["id"],
                )
                cursor.execute(sql, values)
        else:
            # If record doesn't exist, insert it
            added += 1
            sql = """
                INSERT INTO products (
                    id, price_cost, price_retail, `condition`, description, maintain_stock, 
                    name, quantity, warranty, sort_order, reorder_at, disabled, taxable, 
                    product_category, category_path, upc_code, discount_percent, warranty_template_id, 
                    qb_item_id, desired_stock_level, price_wholesale, notes, tax_rate_id, 
                    physical_location, serialized, vendor_ids, long_description, location_quantities, photos, hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item["id"],
                item["price_cost"],
                item["price_retail"],
                item.get("condition", ""),
                item["description"],
                item["maintain_stock"],
                item["name"],
                item["quantity"],
                item.get("warranty", None),
                item.get("sort_order", None),
                item.get("reorder_at", None),
                item["disabled"],
                item["taxable"],
                item["product_category"],
                item["category_path"],
                item.get("upc_code", ""),
                item.get("discount_percent", None),
                item.get("warranty_template_id", None),
                item.get("qb_item_id", None),
                item.get("desired_stock_level", None),
                item["price_wholesale"],
                item.get("notes", ""),
                item.get("tax_rate_id", None),
                item.get("physical_location", ""),
                item["serialized"],
                json.dumps(item["vendor_ids"]),
                item.get("long_description", ""),
                json.dumps(item["location_quantities"]),
                json.dumps(item["photos"]),
                current_hash,
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new products, updated %s existing products.",
        added,
        updated,
        extra={"tags": {"service": "insert_products", "finished": "yes"}},
    )


def insert_users(logger, cursor, items):
    """Insert or update users based on the items provided."""
    added = 0

    user_list = items.get("users", [])

    for item in user_list:
        # Check if the user exists
        user_id = item[0]
        user_name = item[1]
        cursor.execute("SELECT id FROM users WHERE id = %s", (user_id,))
        existing_record = cursor.fetchone()
        if not existing_record:
            added += 1
            sql = """
                INSERT INTO users (id, name) VALUES (%s, %s)"""
            values = (
                user_id,
                user_name,
            )
            cursor.execute(sql, values)

    logger.info(
        "Added %s new users.",
        added,
        extra={"tags": {"service": "insert_users", "finished": "yes"}},
    )

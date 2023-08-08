"""library for db connection functions"""
import mysql.connector


def create_contact_table_if_not_exists(config):
    """create the contact db if it desn't already exist"""
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    # Create the contacts table if not exists
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
    return cursor, connection


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
            name VARCHAR(1024),
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

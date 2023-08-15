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

"""Create DB Table Functions"""


def create_contact_table_if_not_exists(cursor):
    """create the contact db if it doesn't already exist"""
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


def create_customer_table_if_not_exists(cursor):
    """Create the customer db table if it doesn't already exist"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
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


def create_invoices_table_if_not_exists(cursor):
    """create the invoices db if it doesn't already exist"""
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS invoices (
            id INT PRIMARY KEY,
            customer_id INT,
            customer_business_then_name VARCHAR(255),
            number VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME,
            date DATE,
            due_date DATE,
            subtotal DECIMAL(10, 2),
            total DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            verified_paid BOOLEAN,
            tech_marked_paid BOOLEAN,
            ticket_id INT,
            user_id INT,
            pdf_url TEXT,
            is_paid BOOLEAN,
            location_id INT,
            po_number VARCHAR(255),
            contact_id INT,
            note TEXT,
            hardwarecost DECIMAL(10, 2)
        )
        """
    )


def create_invoice_items_table_if_not_exists(cursor):
    """Create the invoice_items table if it doesn't already exist."""

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


def create_estimates_table_if_not_exists(cursor):
    """Create the estimates table if it doesn't already exist."""
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS estimates (
            id INT PRIMARY KEY,
            customer_id INT,
            customer_business_then_name VARCHAR(4096),
            number VARCHAR(255),
            status VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME,
            date DATETIME,
            subtotal DECIMAL(10, 2),
            total DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            ticket_id INT,
            pdf_url VARCHAR(2048),
            location_id INT,
            invoice_id INT,
            employee VARCHAR(255)
        )
        """
    )


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


def create_payments_table_if_not_exists(cursor):
    """Create the payments db table if it doesn't already exist"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
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


def create_products_table_if_not_exists(cursor):
    """Create the products db table if it doesn't already exist"""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
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

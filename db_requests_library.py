'''library for db connection functions'''
import mysql.connector


def create_contact_table_if_not_exists(config):
    '''create the contact db if it desn't already exist'''
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
    return cursor, connection


"""Library for volume data"""
from datetime import datetime
import time


def create_volume_table_if_not_exists(cursor):
    """Create the volume table if it doesn't already exist"""

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS volume (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp INT,
        date DATETIME,
        new_contacts INT DEFAULT 0,
        updated_contacts INT DEFAULT 0,
        deleted_contacts INT DEFAULT 0,
        new_customers INT DEFAULT 0,
        updated_customers INT DEFAULT 0,
        deleted_customers INT DEFAULT 0,
        new_estimates INT DEFAULT 0,
        updated_estimates INT DEFAULT 0,
        deleted_estimates INT DEFAULT 0,
        new_line_items INT DEFAULT 0,
        updated_line_items INT DEFAULT 0,
        deleted_line_items INT DEFAULT 0,
        new_invoices INT DEFAULT 0,
        updated_invoices INT DEFAULT 0,
        deleted_invoices INT DEFAULT 0,
        new_payments INT DEFAULT 0,
        updated_payments INT DEFAULT 0,
        deleted_payments INT DEFAULT 0,
        new_tickets INT DEFAULT 0,
        updated_tickets INT DEFAULT 0,
        deleted_tickets INT DEFAULT 0,
        new_comments INT DEFAULT 0,
        updated_comments INT DEFAULT 0,
        deleted_comments INT DEFAULT 0,
        new_products INT DEFAULT 0,
        updated_products INT DEFAULT 0,
        deleted_products INT DEFAULT 0,
        new_users INT DEFAULT 0,
        updated_users INT DEFAULT 0   
        )
        """
    )


def insert_volume(cursor, logger, new=0, updated=0, deleted=0, table_name=None):
    """Insert volume data into the volume table"""

    timestamp = int(time.time())
    date = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    if table_name is None:
        logger.error(
            "table_name not passed to insert_volume",
            extra={"tags": {"service": "volume"}},
        )

    cursor.execute(
        f"""
        INSERT INTO volume (
            timestamp,
            date,
            new_{table_name},
            updated_{table_name},
            deleted_{table_name}
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (timestamp, date, new, updated, deleted),
    )

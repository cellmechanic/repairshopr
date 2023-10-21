"""General DB utilities"""
from calendar import c
import re
import mysql.connector


def rate_limit():
    """current rate limit setting"""
    return 60 / 180


def connect_to_db(config):
    """connect to the db"""
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    return cursor, connection


def compare_id_sums(logger, cursor, data, table_name):
    """compare the id sums to make sure they match"""

    if table_name == "contacts":
        cursor.execute("SELECT SUM(id) FROM contacts")
        contacts_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(contact["id"] for contact in data)
        logger.info(
            "Sum of IDs from contacts API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from contacts table in DB: %s",
            contacts_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )

        if sum_of_ids_api == contacts_sum:
            logger.info(
                "Both ID sums are matching for contacts",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for contacts",
                extra={"tags": {"service": "compare_id_sums"}},
            )

        return sum_of_ids_api == contacts_sum

    if table_name == "invoice_items":
        cursor.execute("SELECT SUM(id) FROM invoice_items")
        line_items_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(invoice_item["id"] for invoice_item in data)
        logger.info(
            "Sum of IDs from invoice_items table in DB: %s",
            line_items_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from invoice_items API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        if sum_of_ids_api == line_items_sum:
            logger.info(
                "Both ID sums are matching for invoice_items",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for invoice_items",
                extra={"tags": {"service": "compare_id_sums"}},
            )

        return sum_of_ids_api == line_items_sum

    if table_name == "tickets":
        cursor.execute("SELECT SUM(id) FROM tickets")
        tickets_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(ticket["id"] for ticket in data)
        logger.info(
            "Sum of IDs from tickets table in DB: %s",
            tickets_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from tickets API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        if sum_of_ids_api == tickets_sum:
            logger.info(
                "Both ID sums are matching for tickets",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for tickets",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        return sum_of_ids_api == tickets_sum

    if table_name == "comments":
        cursor.execute("SELECT SUM(id) FROM comments")
        comments_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(comment["id"] for comment in data)
        logger.info(
            "Sum of IDs from comments table in DB: %s",
            comments_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from comments API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        if sum_of_ids_api == comments_sum:
            logger.info(
                "Both ID sums are matching for comments",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for comments",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        return sum_of_ids_api == comments_sum

    if table_name == "customers":
        cursor.execute("SELECT SUM(id) FROM customers")
        customers_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(customer["id"] for customer in data)
        logger.info(
            "Sum of IDs from customers table in DB: %s",
            customers_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from customers API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        if sum_of_ids_api == customers_sum:
            logger.info(
                "Both ID sums are matching for customers",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for customers",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        return sum_of_ids_api == customers_sum

    if table_name == "estimates":
        cursor.execute("SELECT SUM(id) FROM estimates")
        estimates_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(estimate["id"] for estimate in data)
        logger.info(
            "Sum of IDs from estimates table in DB: %s",
            estimates_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from estimates API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        if sum_of_ids_api == estimates_sum:
            logger.info(
                "Both ID sums are matching for estimates",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for estimates",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        return sum_of_ids_api == estimates_sum

    if table_name == "invoices":
        cursor.execute("SELECT SUM(id) FROM invoices")
        invoices_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(invoice["id"] for invoice in data)
        logger.info(
            "Sum of IDs from invoices table in DB: %s",
            invoices_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from invoices API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )

        if sum_of_ids_api == invoices_sum:
            logger.info(
                "Both ID sums are matching for invoices",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for invoices",
                extra={"tags": {"service": "compare_id_sums"}},
            )

    if table_name == "payments":
        cursor.execute("SELECT SUM(id) FROM payments")
        payments_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(payment["id"] for payment in data)
        logger.info(
            "Sum of IDs from payments table in DB: %s",
            payments_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from payments API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )

        if sum_of_ids_api == payments_sum:
            logger.info(
                "Both ID sums are matching for payments",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for payments",
                extra={"tags": {"service": "compare_id_sums"}},
            )

        return sum_of_ids_api == payments_sum

    if table_name == "products":
        cursor.execute("SELECT SUM(id) FROM products")
        products_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(product["id"] for product in data)
        logger.info(
            "Sum of IDs from products table in DB: %s",
            products_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from products API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )

        if sum_of_ids_api == products_sum:
            logger.info(
                "Both ID sums are matching for products",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match for products",
                extra={"tags": {"service": "compare_id_sums"}},
            )

        return sum_of_ids_api == products_sum


def extract_devices(subject):
    """Use the regex to extract the devices from the subject"""

    pattern = r"\((\d+)\)"

    match = re.search(pattern, subject)

    if match:
        return int(match.group(1))
    else:
        return 1

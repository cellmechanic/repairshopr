"""General DB utilities"""
import mysql.connector
from library.fix_date_time_library import log_ts
from library.loki_library import start_loki


def rate_limit():
    """current rate limit setting"""
    return 30 / 128


def connect_to_db(config):
    """connect to the db"""
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    return cursor, connection


def compare_id_sums(cursor, data, table_name):
    """compare the id sums to make sure they match"""
    logger = start_loki("__compare_id_sums__")
    if table_name == "contacts":
        cursor.execute("SELECT SUM(id) FROM contacts")
        contacts_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(contact["id"] for contact in data)
        logger.info(
            "Sum of IDs from API: %s",
            sum_of_ids_api,
            extra={"tags": {"service": "compare_id_sums"}},
        )
        logger.info(
            "Sum of IDs from DB: %s",
            contacts_sum,
            extra={"tags": {"service": "compare_id_sums"}},
        )

        if sum_of_ids_api == contacts_sum:
            logger.info(
                "Both ID sums are matching",
                extra={"tags": {"service": "compare_id_sums"}},
            )
        else:
            logger.warning(
                "The sum of IDs does not match",
                extra={"tags": {"service": "compare_id_sums"}},
            )

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

    if table_name == "customers":
        cursor.execute("SELECT SUM(id) FROM customers")
        customers_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(customer["id"] for customer in data)
        print(f"{log_ts()} Sum of IDs from customers API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from customers DB: {customers_sum}")

        if sum_of_ids_api == customers_sum:
            print(f"{log_ts()} Customer ID's match.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")
        return sum_of_ids_api == customers_sum

    if table_name == "estimates":
        cursor.execute("SELECT SUM(id) FROM estimates")
        estimates_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(estimate["id"] for estimate in data)
        print(f"{log_ts()} Sum of IDs from estimates API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from estimates DB: {estimates_sum}")

        if sum_of_ids_api == estimates_sum:
            print(f"{log_ts()} Estimate ID's match.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")
        return sum_of_ids_api == estimates_sum

    if table_name == "invoices":
        cursor.execute("SELECT SUM(id) FROM invoices")
        invoices_sum = cursor.fetchone()[0]

        sum_of_ids_api = sum(invoice["id"] for invoice in data)
        print(f"{log_ts()} Sum of IDs from invoices API: {sum_of_ids_api}")
        print(f"{log_ts()} Sum of IDs from invoices DB: {invoices_sum}")

        if sum_of_ids_api == invoices_sum:
            print(f"{log_ts()} Invoice ID's match.")
        else:
            print(f"{log_ts()} The sum of IDs does not match.")

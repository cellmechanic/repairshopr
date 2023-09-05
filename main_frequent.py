""" main script for the frequent modules """
from library.loki_library import start_loki
from modules.customers import customers
from modules.invoices import invoices
from modules.estimates import estimates
from modules.invoice_lines_update import invoice_lines_update
from modules.contacts import contacts
from modules.payments import payments
from modules.tickets_days import ticket_days

logger = start_loki("__main_frequent__")

logger.info(
    "---------START---------------",
    extra={"tags": {"service": "main_frequent", "finished": "yes"}},
)

contacts()
customers()
estimates(False, 30)
invoice_lines_update()
invoices()
payments()
ticket_days(30)

logger.info(
    "----------END----------------",
    extra={"tags": {"service": "main_frequent", "finished": "yes"}},
)

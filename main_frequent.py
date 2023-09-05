""" main script for the frequent modules """
from library.loki_library import start_loki
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

# contacts()
# invoice_lines_update()
# ticket_days(10)
# estimates(False, 10)
# invoices()
payments()

logger.info(
    "----------END----------------",
    extra={"tags": {"service": "main_frequent", "finished": "yes"}},
)

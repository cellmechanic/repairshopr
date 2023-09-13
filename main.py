""" main script for the frequent modules """
import argparse
from library.loki_library import start_loki
from modules.customers import customers
from modules.invoices import invoices
from modules.estimates import estimates
from modules.invoice_lines import invoice_lines
from modules.contacts import contacts
from modules.payments import payments
from modules.products import products
from modules.tickets_days import ticket_days


def main():
    """main script for the frequent modules"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full", action="store_true", help="Run full data set, takes a very long time"
    )
    parser.add_argument(
        "--frequent",
        action="store_true",
        help="Set lookback days low, run every 5 mins",
    )

    args = parser.parse_args()

    if args.frequent:
        logger = start_loki("__main_frequent__")

        logger.info(
            "---------START EVERY 5 MINS---------------",
            extra={"tags": {"service": "main_frequent", "finished": "yes"}},
        )

        contacts()  # Always does full run, small data set
        customers()  # Always does a full run, small data set
        estimates(False, 14)
        invoice_lines()
        invoices(False, 14)
        payments(False, 14)
        ticket_days(14)
        products()  # Always does a full run, small data set

        logger.info(
            "----------END EVERY 5 MINS----------------",
            extra={"tags": {"service": "main_frequent", "finished": "yes"}},
        )

    if args.full:
        logger = start_loki("__main_full__")

        logger.info(
            "---------START FULL RUN---------------",
            extra={"tags": {"service": "main_full", "finished": "full"}},
        )

        contacts()
        customers()
        estimates(True, 0)
        invoice_lines(True)
        invoices(True, 0)
        payments(True, 0)
        products()

        logger.info(
            "---------END FULL RUN---------------",
            extra={"tags": {"service": "main_full", "finished": "full"}},
        )


if __name__ == "__main__":
    main()

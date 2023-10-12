""" main script for the frequent modules """
import argparse
from library.loki_library import start_loki
from modules.backup import backup_database, upload_to_drive
from modules.users import users
from modules.customers import customers
from modules.invoices import invoices
from modules.estimates import estimates
from modules.invoice_lines import invoice_lines
from modules.contacts import contacts
from modules.output import output
from modules.payments import payments
from modules.products import products
from modules.tickets import tickets
from modules.volume import volume


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

        # volume()
        # contacts(logger, False)
        # customers(logger, False)
        estimates(logger, False, 7) #
        invoice_lines(logger, False)
        invoices(logger, False, 7) # has since_updated_at
        payments(logger, False, 7)
        tickets(logger, False, 14) # has since_updated_at
        products(logger, False)
        users(logger)
        output(logger)

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

        backup_database(logger)
        upload_to_drive(logger)
        # contacts(logger, True)
        # customers(logger, True)
        # estimates(logger, True)
        # invoice_lines(logger, True)
        # invoices(logger, True)
        # payments(logger, True)
        # tickets(logger, True)
        # products(logger, True)

        logger.info(
            "---------END FULL RUN---------------",
            extra={"tags": {"service": "main_full", "finished": "full"}},
        )


if __name__ == "__main__":
    main()

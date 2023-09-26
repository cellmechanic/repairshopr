"""estimate backup module"""
import time

from library import env_library
from library.api_requests import get_estimates, get_date_for_header
from library.db_create import create_estimates_table_if_not_exists
from library.db_delete import move_deleted_estimates_to_deleted_table
from library.db_general import compare_id_sums, connect_to_db, rate_limit
from library.db_insert import insert_estimates


def estimates(logger, full_run=False, lookback_days=365):
    """main script for the estimates module"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_estimates_table_if_not_exists(cursor)

    # Meta vars
    current_page = 1
    total_pages = 0
    api_page = 1
    all_data = []

    if not full_run:
        # Get 1st Page, then check to make sure not null
        data = get_estimates(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting estimate data",
                extra={"tags": {"service": "estimates"}},
            )

        lookback_date_formatted = get_date_for_header(lookback_days)
        found_page = 0

        for page in range(1, total_pages + 1):
            data = get_estimates(logger, page)
            if data is not None and "estimates" in data:
                found_older = any(
                    item["created_at"] < lookback_date_formatted
                    for item in data["estimates"]
                )
                if found_older:
                    logger.info(
                        "Found older than %s days",
                        lookback_days,
                        extra={"tags": {"service": "estimates"}},
                    )
                    break

                all_data.extend(data["estimates"])
                logger.info(
                    "Added in page # %s", page, extra={"tags": {"service": "estimates"}}
                )
                found_page = page
            else:
                logger.error(
                    "Error getting estimate data",
                    extra={"tags": {"service": "estimates"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Adding in : %s / %s",
            found_page,
            total_pages,
            extra={"tags": {"service": "estimates"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "estimates"}},
        )

        insert_estimates(logger, cursor, all_data)

    if full_run:
        data = get_estimates(logger, current_page)
        if data is not None:
            total_pages = data["meta"]["total_pages"]
        else:
            logger.error(
                "Error getting estimate data",
                extra={"tags": {"service": "estimates"}},
            )

        for page in range(1, total_pages + 1):
            data = get_estimates(logger, page)
            if data is not None:
                all_data.extend(data["estimates"])
                logger.info(
                    "Added in page # %s", page, extra={"tags": {"service": "estimates"}}
                )
                api_page = page
            else:
                logger.error(
                    "Error getting estimate data",
                    extra={"tags": {"service": "estimates"}},
                )
                break
            time.sleep(rate_limit())

        logger.info(
            "Received all data, %s page(s)",
            total_pages,
            extra={"tags": {"service": "estimates"}},
        )
        logger.info(
            "Number of entries to consider for DB: %s",
            len(all_data),
            extra={"tags": {"service": "estimates"}},
        )

        insert_estimates(logger, cursor, all_data)
        if api_page == total_pages:
            all_sourced = True
        else:
            all_sourced = False
        if all_sourced:
            deleted = compare_id_sums(logger, cursor, all_data, "estimates")
            if not deleted:
                move_deleted_estimates_to_deleted_table(
                    logger, cursor, connection, all_data
                )
            # Validate data / totals
            query = "SELECT COUNT(*) FROM estimates"
            cursor.execute(query)          
            result = cursor.fetchone()
            if result is not None:
                db_rows = result[0]
            else:
                db_rows = 0
            # Check if the total entries match the expected count
            if db_rows == len(all_data):
                logger.info(
                    "All Good -- Estimate API Rows: %s, DB Rows: %s",
                    len(all_data),
                    db_rows,
                    extra={"tags": {"service": "estimates", "finished": "full"}},
                )
            else:
                logger.error(
                    "Estimate API Rows: %s, DB Rows: %s",
                    len(all_data),
                    db_rows,
                    extra={"tags": {"service": "estimates"}},
                )
        elif not all_sourced:
            logger.error(
                "Can't check for deletes, problem with estimates API data",
                extra={"tags": {"service": "estimates"}},
            )

    connection.commit()
    connection.close()

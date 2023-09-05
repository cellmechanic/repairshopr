"""make timestamp files"""
import time
from library.fix_date_time_library import log_ts
from library.loki_library import start_loki


def update_last_ran(timestamp_file):
    """update the last time the script ran file, pass in the file"""
    logger = start_loki("__update_last_ran__")
    try:
        with open(timestamp_file, "w", encoding="utf-8") as file:
            file.write(str(int(time.time())))
    except IOError as error:
        logger.error(
            "%s Error updating timestamp: %s",
            log_ts(),
            error,
            extra={"tags": {"service": "timestamp"}},
        )


def check_last_ran(timestamp_file):
    """check the passed timestamp file to see the last time this ran"""
    logger = start_loki("__check_last_ran__")
    try:
        with open(timestamp_file, "r", encoding="utf-8") as file:
            last_run_timestamp_str = file.read().strip()

            if last_run_timestamp_str:
                last_run_timestamp = int(last_run_timestamp_str)
                last_run_time_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(last_run_timestamp)
                )
                logger.info(
                    "Last ran @ %s",
                    last_run_time_str,
                    extra={"tags": {"service": "timestamp"}},
                )
                return int(last_run_timestamp_str)
            else:
                logger.warning(
                    "No timestamp found, using 0 as time, assuming rebuild",
                    extra={"tags": {"service": "timestamp"}},
                )   
                return int(1)
    except FileNotFoundError:
        logger.warning(
            "No timestamp found, using 0 as time, assuming rebuild",
            extra={"tags": {"service": "timestamp"}},
        )
        return int(0)

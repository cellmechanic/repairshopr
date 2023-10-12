"""Backup the db to drive every night"""
import os
import subprocess
from datetime import datetime
from library import env_library

# Database credentials


# Generate a human-readable timestamp and append it to the backup file name
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
BACKUP_PATH = f"backup_{timestamp}.sql"


# Backup MariaDB database using mysqldump
def backup_database(logger):
    """Backup the database using mysqldump"""
    try:
        config = env_library.config
        db_user = config["user"]
        db_password = config["password"]
        db_name = config["database"]
        db_host = config["host"]
        if not db_password or not db_user or not db_name or not db_host:
            logger.error(
                "Error logging in to the database",
                extra={"tags": {"service": "backup", "finished": "full"}},
            )
            exit()
        subprocess.run(
            [
                "mysqldump",
                "-h",
                db_host,
                "-u",
                db_user,
                "-p" + db_password,
                db_name,
                "-r",
                BACKUP_PATH,
            ],
            check=True,
        )
        print("Database backed up successfully.")
        logger.info(
            "Database backed up successfully",
            extra={"tags": {"service": "backup", "finished": "full"}},
        )
    except subprocess.CalledProcessError:
        logger.error(
            "Error backing up the database",
            extra={"tags": {"service": "backup", "finished": "full"}},
        )
        exit()


def upload_to_drive(logger):
    """Upload the backup to Google Drive using rclone"""

    rclone_remote_name = "maria"
    folder_id = "proxbackup/prod"
    dest_path = f"{rclone_remote_name}:{folder_id}"

    command = [
        "rclone",
        "copy",
        BACKUP_PATH,
        dest_path,
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(
                "Backup up DB to Google Drive",
                extra={"tags": {"service": "backup", "finished": "full"}},
            )
        else:
            logger.error(
                f"Error with rclone: {result.stderr}",
                extra={"tags": {"service": "backup", "finished": "full"}},
            )

        # Delete the local backup file
        os.remove(BACKUP_PATH)
        logger.info(
            "Local backup file deleted.",
            extra={"tags": {"service": "backup", "finished": "full"}},
        )

    except OSError as e:
        logger.error(
            f"Error during rclone operation: {e}",
            extra={"tags": {"service": "backup", "finished": "full"}},
        )

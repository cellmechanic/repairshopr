"""Backup the db to drive every night"""
import os
import subprocess
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
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
    """Upload the backup to Google Drive"""
    gauth = GoogleAuth()
    
    # Load existing credentials if they exist
    gauth.LoadCredentialsFile("credentials.json")
    
    if gauth.credentials is None:
        # If no credentials exist, prompt the user
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # If credentials are expired, refresh them
        gauth.Refresh()
    else:
        # Just authorize if everything is in order
        gauth.Authorize()
        
    # Save the current credentials for the next run
    gauth.SaveCredentialsFile("credentials.json")
    
    drive = GoogleDrive(gauth)

    folder_id = "1x8cN3uGqdKtoc8yV5wenW9r22aFQX4TT"  # Replace with your actual folder ID

    file = drive.CreateFile(
        {
            "title": os.path.basename(BACKUP_PATH),
            "parents": [{"id": folder_id}],  # This line specifies the folder
        }
    )

    file.SetContentFile(BACKUP_PATH)
    file.Upload()
    logger.info(
        "Backup uploaded to Google Drive in the specified folder.",
        extra={"tags": {"service": "backup", "finished": "full"}},
    )
        # Delete the local backup file
    try:
        os.remove(BACKUP_PATH)
        logger.info(
            "Local backup file deleted.",
            extra={"tags": {"service": "backup", "finished": "full"}},
        )
    except OSError as e:
        logger.error(
            f"Error deleting the local backup file: {e}",
            extra={"tags": {"service": "backup", "finished": "full"}},
        )

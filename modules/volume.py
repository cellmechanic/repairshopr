""" volume module """
from library import env_library
from library.db_general import connect_to_db
from library.volume_library import create_volume_table_if_not_exists


def volume():
    """make sure volume is created"""
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_volume_table_if_not_exists(cursor)

    connection.commit()
    connection.close()

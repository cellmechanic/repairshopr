"""get users for db"""

from library import env_library
from library.api_requests import get_users
from library.db_create import create_users_table_if_not_exists
from library.db_general import connect_to_db
from library.db_insert import insert_users


def users(logger):
    """get users for db, super simple, no pages, no permissions"""

    # Database configurations
    config = env_library.config
    cursor, connection = connect_to_db(config)
    create_users_table_if_not_exists(cursor)

    data = get_users(logger)

    insert_users(logger, cursor, data)

    connection.commit()
    connection.close()

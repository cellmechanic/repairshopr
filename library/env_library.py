"""library to import api keys and env variables"""
import os
from dotenv import load_dotenv

# Load environment variables from .env, don't publish that
load_dotenv()
api_key_contact = os.getenv('API_CONTACT_KEY')
api_url_contact = os.getenv('API_CONTACT')
api_key_invoice = os.getenv('API_INVOICE_KEY')
api_url_invoice = os.getenv('API_INVOICE')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')

# Database configuration
config = {
    'user': user,
    'password': password,
    'host': host,
    'database': database,
}
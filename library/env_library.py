"""library to import api keys and env variables"""
import os
from dotenv import load_dotenv

# Load environment variables from .env, don't publish that
load_dotenv()

api_key_contact = os.getenv("API_CONTACT_KEY")
api_url_contact = os.getenv("API_CONTACT")
api_key_invoice = os.getenv("API_INVOICE_KEY")
api_url_invoice = os.getenv("API_INVOICE")
api_url_invoice_lines = os.getenv("API_INVOICE_LINES")
api_key_tickets = os.getenv("API_TICKETS_KEY")
api_url_tickets = os.getenv("API_TICKETS")
api_key_customers = os.getenv("API_CUSTOMERS_KEY")
api_url_customers = os.getenv("API_CUSTOMERS")
api_key_estimates = os.getenv("API_ESTIMATES_KEY")
api_url_estimates = os.getenv("API_ESTIMATES")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

# Database configuration
config = {
    "user": user,
    "password": password,
    "host": host,
    "database": database,
}

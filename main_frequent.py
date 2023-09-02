""" main script for the frequent modules """
from modules.invoice_lines_update import invoice_lines_update
from modules.contacts import contacts
from modules.tickets_days import ticket_days

contacts()
invoice_lines_update()
ticket_days(15)

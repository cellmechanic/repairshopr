"""api requests"""

import requests
from datetime import datetime, timedelta
from library import env_library
from library.fix_date_time_library import log_ts


def get_contacts(page):
    """api request for contacts"""
    url = f"{env_library.api_url_contact}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_contact}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"{log_ts()} Error fetching contacts on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"{log_ts()} Failed to get data for page {page}: {str(error)}")
        return None


def get_estimates(page):
    """api request for estimates"""
    url = f"{env_library.api_url_estimates}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_estimates}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"{log_ts()} Error fetching estimates on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"{log_ts()} Failed to get data for page {page}: {str(error)}")
        return None


def get_customers(page):
    """api request for customers"""
    url = f"{env_library.api_url_customers}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_customers}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"{log_ts()} Error fetching contacts on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"{log_ts()} Failed to get data for page {page}: {str(error)}")
        return None


def get_invoice_lines(page):
    """api request for invoice line items"""
    url = f"{env_library.api_url_invoice}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"{log_ts()} Error fetching invoice line items on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"{log_ts()} Failed to get data for page {page}: {str(error)}")
        return None


def get_tickets(page=None, look_back_period=None):
    """api request for ticket data"""

    # base url / header key
    url = f"{env_library.api_url_tickets}"
    headers = {"Authorization": f"Bearer {env_library.api_key_tickets}"}

    # params
    params = {}
    if page is not None:
        params["page"] = page
    if look_back_period is not None:
        params["since_updated_at"] = get_date_for_header(look_back_period)

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code != 200:
            print(f"{log_ts()} Error fetching invoice line items on page {page}: {response.text}")
            return None
        return response.json()

    except requests.RequestException as error:
        print(f"{log_ts()} Failed to get data for page {page}: {str(error)}")
        return None


def get_date_for_header(look_back):
    date_before = datetime.now() - timedelta(days=look_back)
    formatted_date = date_before.strftime('%Y-%m-%d')
    print(f"{log_ts()} Looking back to {formatted_date}")
    return formatted_date

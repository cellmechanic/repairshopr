"""api requests"""
from datetime import datetime, timedelta
import requests
from library import env_library
from library.fix_date_time_library import log_ts
from library.loki_library import start_loki


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
    logger = start_loki("__get_estimates__")
    url = f"{env_library.api_url_estimates}"
    headers = {"Authorization": f"Bearer {env_library.api_key_estimates}"}
    param = {"page": page}

    try:
        response = requests.get(url, headers=headers, params=param, timeout=10)
        if response.status_code != 200:
            logger.error(
                "Error fetching estimates on page %s: %s",
                page,
                response.text,
                extra={"tags": {"service": "estimates"}},
            )
            return None
        return response.json()
    except requests.RequestException as error:
        logger.error(
            "Error fetching estimates on page %s: %s",
            page,
            str(error),
            extra={"tags": {"service": "estimates"}},
        )
        return None


def get_payments(page):
    """api request for payments"""
    logger = start_loki("__get_payments__")
    url = f"{env_library.api_url_payments}"
    headers = {"Authorization": f"Bearer {env_library.api_key_payments}"}
    param = {"page": page}

    try:
        response = requests.get(url, headers=headers, params=param, timeout=10)
        if response.status_code != 200:
            logger.error(
                "Error fetching payments on page %s: %s",
                page,
                response.text,
                extra={"tags": {"service": "payments"}},
            )
            return None
        return response.json()
    except requests.RequestException as error:
        logger.error(
            "Error fetching payments on page %s: %s",
            page,
            str(error),
            extra={"tags": {"service": "payments"}},
        )
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
    url = f"{env_library.api_url_invoice_lines}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}

    logger = start_loki("__invoice_lines__")

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.error(
                "Error fetching invoice line items on page %s: %s",
                page,
                response.text,
                extra={"tags": {"service": "invoice_lines"}},
            )
            return None
        return response.json()
    except requests.RequestException as error:
        logger.error(
            "Error fetching invoice line items on page %s: %s",
            page,
            str(error),
            extra={"tags": {"service": "invoice_lines"}},
        )
        return None


def get_tickets(page=None, look_back_period=None):
    """api request for ticket data"""

    logger = start_loki("__get_tickets__")
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
            logger.error(
                "Error fetching ticket data on page %s: %s",
                page,
                response.text,
                extra={"tags": {"service": "tickets"}},
            )
            return None
        return response.json()

    except requests.RequestException as error:
        logger.error(
            "Error fetching ticket data on page %s: %s",
            page,
            str(error),
            extra={"tags": {"service": "tickets"}},
        )
        return None


def get_invoices(page=None, look_back_date=None):
    """API request for invoice data."""

    # base url / header key
    url = f"{env_library.api_url_invoice}"
    headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}

    # params
    params = {}
    if page is not None:
        params["page"] = page
    if look_back_date is not None:
        params["since_updated_at"] = look_back_date

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code != 200:
            print(f"{log_ts()} Error fetching invoices on page {page}: {response.text}")
            return None
        return response.json()

    except requests.RequestException as error:
        print(f"{log_ts()} Failed to get invoice data for page {page}: {str(error)}")
        return None


def get_date_for_header(look_back):
    """Get date for header"""
    date_before = datetime.now() - timedelta(days=look_back)
    formatted_date = date_before.strftime("%Y-%m-%d")
    return formatted_date

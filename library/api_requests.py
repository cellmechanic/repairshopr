"""api requests"""
from datetime import datetime, timedelta
import time
import requests
from library import env_library


def get_contacts(logger, page, max_retries=3, retry_delay=30):
    """api request for contacts"""
    url = f"{env_library.api_url_contact}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_contact}"}

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching contacts on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "contacts", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching contacts on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "contacts", "error": "non 200"}},
                )
                return None

            return response.json()

        except requests.RequestException as error:
            logger.error(
                "Error fetching contacts on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "contacts", "error": "retry"}},
            )

    return None


def get_estimates(logger, page, max_retries=3, retry_delay=30):
    """api request for estimates"""
    url = f"{env_library.api_url_estimates}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_estimates}"}

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching estimates on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "estimates", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching estimates on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "estimates", "error": "non 200"}},
                )
                return None

            return response.json()
        except requests.RequestException as error:
            logger.error(
                "Error fetching estimates on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "estimates"}},
            )

    return None


def get_payments(logger, page, max_retries=3, retry_delay=30):
    """api request for payments"""

    url = f"{env_library.api_url_payments}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_payments}"}

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching payments on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "payments", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching payments on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "payments", "error": "non 200"}},
                )
                return None

            return response.json()
        except requests.RequestException as error:
            logger.error(
                "Error fetching payments on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "payments"}},
            )

    return None


def get_customers(logger, page, max_retries=3, retry_delay=30):
    """api request for customers"""
    url = f"{env_library.api_url_customers}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_customers}"}

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching customers on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "customers", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching customers on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "customers", "error": "non 200"}},
                )
                return None

            return response.json()

        except requests.RequestException as error:
            logger.error(
                "Error fetching customers on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "customers"}},
            )

    return None


def get_invoice_lines(logger, page, max_retries=3, retry_delay=30):
    """api request for invoice line items"""
    url = f"{env_library.api_url_invoice_lines}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching invoice line items on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "invoice_lines", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching invoice line items on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "invoice_lines", "error": "non 200"}},
                )
                return None

            return response.json()

        except requests.RequestException as error:
            logger.error(
                "Error fetching invoice line items on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "invoice_lines", "error": "retry"}},
            )

    return None


def get_tickets(
    logger, page=None, look_back_period=None, max_retries=3, retry_delay=30
):
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

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching ticket data on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "tickets", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching ticket data on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "tickets", "error": "non 200"}},
                )
                return None

            return response.json()

        except requests.RequestException as error:
            logger.error(
                "Error fetching ticket data on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "tickets"}},
            )

    return None


def get_invoices(logger, page=None, look_back_date=None, max_retries=3, retry_delay=30):
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

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching invoices on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "invoices", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching invoices on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "invoices", "error": "non 200"}},
                )
                return None

            return response.json()

        except requests.RequestException as error:
            logger.error(
                "Error fetching invoices on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "invoices", "error": "retry"}},
            )

    return None


def get_products(logger, page, max_retries=3, retry_delay=30):
    """api request for products"""
    url = f"{env_library.api_url_products}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_products}"}

    for retry_count in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded for fetching products on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "products", "error": "rate limit"}},
                )
                time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                logger.error(
                    "Error fetching products on page %s: %s",
                    page,
                    response.text,
                    extra={"tags": {"service": "products", "error": "non 200"}},
                )
                return None

            return response.json()

        except requests.RequestException as error:
            logger.error(
                "Error fetching products on page %s (Retry %s/%s): %s",
                page,
                retry_count + 1,
                max_retries,
                str(error),
                extra={"tags": {"service": "products", "error": "retry"}},
            )

    return None


def get_date_for_header(look_back):
    """Get date for header"""
    date_before = datetime.now() - timedelta(days=look_back)
    formatted_date = date_before.strftime("%Y-%m-%d")
    return formatted_date

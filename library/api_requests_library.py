"""api requests"""
import requests
from library import env_library


def get_contacts(page):
    """api request"""
    url = f"{env_library.api_url_contact}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_contact}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Error fetching contacts on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"Failed to get data for page {page}: {str(error)}")
        return None


def get_invoice_lines(page):
    """api request"""
    url = f"{env_library.api_url_invoice}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_invoice}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Error fetching invoice line items on page {page}: {response.text}")
            return None
        return response.json()
    except requests.RequestException as error:
        print(f"Failed to get data for page {page}: {str(error)}")
        return None


def get_tickets(page):
    """api request"""
    url = f"{env_library.api_url_tickets}?page={page}"
    headers = {"Authorization": f"Bearer {env_library.api_key_tickets}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Error fetching invoice line items on page {page}: {response.text}")
            return None
        return response.json()

    except requests.RequestException as error:
        print(f"Failed to get data for page {page}: {str(error)}")
        return None

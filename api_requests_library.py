"""library for diff API requests"""
import requests
import env_library  # Make sure that you import the necessary variables from env_library


def get_contacts(page):
    """api request"""
    url = f'{env_library.api_url_contact}?page={page}'
    headers = {'Authorization': f'Bearer {env_library.api_key_contact}'}
    # Assuming the headers are defined in env_library
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code != 200:
        print(f'Error fetching contacts on page {page}: {response.text}')
        return None

    return response.json()

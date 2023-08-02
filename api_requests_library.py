"""library for diff API requests"""
import time
import requests
import env_library  # Make sure that you import the necessary variables from env_library


def get_contacts(page):
    """api request"""
    url = f'{env_library.api_url_contact}?page={page}'
    headers = {'Authorization': f'Bearer {env_library.api_key_contact}'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f'Error fetching contacts on page {page}: {response.text}')
            return None
        return response.json()
    except requests.RequestException as error:
        print(f'Failed to get data for page {page}: {str(error)}')
        return None


def update_last_ran(timestamp_file):
    ''' update the last time the script ran file, pass in the file'''
    try:
        with open(timestamp_file, 'w', encoding='utf-8') as file:
            file.write(str(int(time.time())))
        print("Timestamp updated successfully to: ", int(time.time()))
    except IOError as error:
        print(f"Error updating timestamp: {error}")


def check_last_ran(timestamp_file):
    ''' check the passed timestamp file to see the last time this ran'''
    try:
        with open(timestamp_file, 'r', encoding='utf-8') as file:
            last_run_timestamp_str = file.read().strip()
            if last_run_timestamp_str:
                last_run_timestamp = int(last_run_timestamp_str)
                print(f"Last ran @ {last_run_timestamp_str} in unix")
                last_run_time_str = time.strftime('%Y-%m-%d %H:%M:%S',
                                                  time.localtime(last_run_timestamp))
                print(f"Last ran @ {last_run_time_str}")
                return int(last_run_timestamp_str)
            else:
                print("no timestamp found, using now as time")
                return int(time.time())
    except FileNotFoundError:
        print("no timestamp file found, using now as time")
        return int(time.time())


def get_timestamp_code():
    '''return the correct timestamp code to stay uniform'''
    return '%Y-%m-%dT%H:%M:%S.%f%z'

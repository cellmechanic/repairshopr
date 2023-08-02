import mysql.connector
import requests
import json
import os
from dotenv import load_dotenv


load_dotenv();

api_key = os.getenv('API_KEY')
api_url = os.getenv('API_CONTACT')

headers = {'Authorization': f'Bearer {api_key}'}

response = requests.get(api_url, headers=headers)
if response.status_code != 200:
    print(f'Error fetching contacts: {response.text}')
    exit()

data = response.json()
sample_contact = data['contacts'][1] if data['contacts'] else {}
total_pages = data['meta']['total_pages']

# Print success message
print(f"Successfully fetched contacts. Total pages: {total_pages}")

# Optionally, print a sample contact
print("Sample contact:")
print(json.dumps(sample_contact, indent=4))
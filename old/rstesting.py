import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv();

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

# Connect to the database
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Create the rs_test_comments table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS rs_test_comments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    comment TEXT NOT NULL
)
''')

# Insert a comment
cursor.execute("INSERT INTO rs_test_comments (name) VALUES ('This is a test comment')")
connection.commit()  # Commit the transaction

# Read all comments
cursor.execute("SELECT * FROM rs_test_comments")
rows = cursor.fetchall()
for row in rows:
    print(f'ID: {row[0]} - Comment: {row[1]}')

# Close the connection
cursor.close()
connection.close()

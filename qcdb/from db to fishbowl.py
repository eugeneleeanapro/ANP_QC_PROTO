from flask import Flask, jsonify, request
import mysql.connector
import requests
from datetime import datetime

app = Flask(__name__)

# MySQL connection setup
def get_db_connection():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='mysql',
        database='qcdb'
    )
    return connection

# Fetch new QC data from the database
def fetch_new_qc_data():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Fetch all new QC data that hasn't been sent to Fishbowl yet
    query = "SELECT id, product_id, test_date, test_result FROM qc_table WHERE coa_generated = 0"
    cursor.execute(query)
    new_data = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return new_data

# Send QC data to Fishbowl
def send_to_fishbowl(data):
    # Fishbowl API endpoint
    fishbowl_url = 'http://localhost/phpmyadmin/index.php?route=/database/structure&db=qcdb'  # Example Fishbowl API endpoint

    # Send each data entry to Fishbowl
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer <your_fishbowl_api_token>'  # Add your Fishbowl API token here
    }

    for entry in data:
        # Prepare data to send (adjust fields as per Fishbowl's API requirements)
        payload = {
            "ProductID": entry['product_id'],
            "TestDate": entry['test_date'].strftime('%Y-%m-%d'),
            "TestResult": entry['test_result']
        }
        response = requests.post(fishbowl_url, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"Successfully sent data for product ID {entry['product_id']}")
            # Mark as processed in the database
            mark_as_processed(entry['id'])
        else:
            print(f"Failed to send data for product ID {entry['product_id']}")

# Mark QC data as processed
def mark_as_processed(qc_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    query = "UPDATE qc_table SET coa_generated = 1 WHERE id = %s"
    cursor.execute(query, (qc_id,))
    connection.commit()
    cursor.close()
    connection.close()

# Endpoint to trigger the import to Fishbowl
@app.route('/api/import_to_fishbowl', methods=['POST'])
def import_to_fishbowl():
    try:
        # Fetch new QC data
        new_data = fetch_new_qc_data()
        if not new_data:
            return jsonify({"message": "No new data to import"}), 200
        
        # Send data to Fishbowl
        send_to_fishbowl(new_data)
        return jsonify({"message": "Data import completed"}), 200
    except Exception as e:
        return jsonify({"message": f"An error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(port=5000)

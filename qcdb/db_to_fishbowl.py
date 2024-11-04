import requests
import mysql.connector
import json

class DBToFishbowl:
    def __init__(self):
        self.db_connection = self.connect_to_database()
        self.session_id = self.fishbowl_login()
        
    # Connect to MySQL database
    def connect_to_database(self):
        try:
            connection = mysql.connector.connect(
                host='localhost',
                user='root',
                password='mysql',
                database='qcdb'
            )
            return connection
        except mysql.connector.Error as err:
            print(f"Error connecting to the database: {err}")
            return None

    # Fishbowl API login
    def fishbowl_login(self):
        url = "http://anpenertech.myfishbowl.com:3819/api/login"
        payload = {
            "username": "admin",
            "password": "admin",
            "name": "QC Database Integration",       # Integrated App Name in Fishbowl
            "appKey": "YourGeneratedAppKey"          # Replace with the appKey from Fishbowl
        }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        
        if response.ok:
            session_id = response.json().get("sessionId")
            print("Logged into Fishbowl API successfully.")
            return session_id
        else:
            print("Fishbowl login failed:", response.text)
            return None

    # Fetch QC data from MySQL database
    def fetch_qc_data(self):
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM icp")  # You can modify this for additional tables as needed
        qc_data = cursor.fetchall()
        cursor.close()
        return qc_data

    # Update COA in Fishbowl with QC data
    def update_coa_in_fishbowl(self):
        if not self.session_id:
            print("No valid Fishbowl session. Exiting update.")
            return
        
        url = "http://anpenertech.myfishbowl.com:3819/api/execute"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Session {self.session_id}'
        }
        qc_data = self.fetch_qc_data()
        
        payload = {
            "requestType": "UpdateCOA",
            "qcData": qc_data  # Ensure this format matches Fishbowl’s COA update requirements
        }
        
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        
        if response.ok:
            print("QC data successfully updated in Fishbowl COA.")
        else:
            print("Failed to update COA:", response.text)

    # Close the database connection
    def close(self):
        if self.db_connection:
            self.db_connection.close()
            print("Database connection closed.")

# Run the class to update Fishbowl
if __name__ == "__main__":
    db_to_fishbowl = DBToFishbowl()
    if db_to_fishbowl.session_id:
        db_to_fishbowl.update_coa_in_fishbowl()
    db_to_fishbowl.close()

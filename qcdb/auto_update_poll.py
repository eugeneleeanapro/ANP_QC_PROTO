import csv
import mysql.connector
import os
import time

# Database connection function
def connect_to_database():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='mysql',
        database='qcdb'
    )

# Function to process the CSV file and update the database
def update_database_from_csv(csv_file_path):
    print("Processing the CSV file for updates...")  # Log when file is processed
    connection = connect_to_database()
    cursor = connection.cursor()

    # Function to check if a lot exists in the 'lots' table, insert if not
    def check_or_insert_lot(lot_number):
        cursor.execute("SELECT lot_number FROM lots WHERE lot_number = %s", (lot_number,))
        result = cursor.fetchone()
        if not result:
            print(f"Inserting lot_number: {lot_number} into lots table.")
            cursor.execute("INSERT INTO lots (lot_number, production_date) VALUES (%s, CURDATE())", (lot_number,))

    # Open and process the CSV file
    with open(csv_file_path, 'r') as file:
        contents = csv.reader(file)
        headers = next(contents, None)  # Skip header row

        if headers:
            print(f"CSV Headers Detected: {headers}")

        # SQL query for the icp table
        insert_icp_records = '''
            INSERT INTO icp (lot_number, status, Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Sn = VALUES(Sn), Si = VALUES(Si), Ca = VALUES(Ca), 
                                   Cr = VALUES(Cr), Cu = VALUES(Cu), Zr = VALUES(Zr), 
                                   Fe = VALUES(Fe), Na = VALUES(Na), Ni = VALUES(Ni), 
                                   Zn = VALUES(Zn), Co = VALUES(Co)
        '''

        # Insert data into the respective tables based on CSV rows
        for row in contents:
            # Skip incomplete rows with missing required fields
            if len(row) < 18:
                print(f"Skipping incomplete or invalid row: {row}")
                continue

            # Map CSV columns to expected fields; adjust indices as needed
            lot_number = row[0]
            values = row[7:18]  # Elements expected for the ICP table (Sn, Si, Ca, etc.)
            status = 'Pass'

            # Ensure lot_number is in 'lots' table
            check_or_insert_lot(lot_number)

            # Insert into the ICP table
            try:
                cursor.execute(insert_icp_records, (lot_number, status, *values))
            except mysql.connector.Error as err:
                print(f"Error inserting ICP data for {lot_number}: {err}")

    # Commit changes
    connection.commit()
    connection.close()
    print("Database update completed.")

# Function to periodically check the CSV file for changes
def poll_for_changes(csv_file_path, polling_interval=3600):  # Poll every hour
    last_modified_time = None

    while True:
        try:
            # Check if the file has been modified
            current_modified_time = os.path.getmtime(csv_file_path)

            if last_modified_time is None or current_modified_time > last_modified_time:
                print(f"Detected change in {csv_file_path}. Updating the database.")
                update_database_from_csv(csv_file_path)
                last_modified_time = current_modified_time

            # Wait for the next polling cycle
            time.sleep(polling_interval)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(polling_interval)

# Specify the CSV file path
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Start polling the CSV file for changes every hour
poll_for_changes(csv_file_path, polling_interval=3600)  # Poll every hour (3600 seconds)

import csv
import mysql.connector
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Specify the CSV file path
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Database connection function
def connect_to_database():
    return mysql.connector.connect(
        host='localhost',
        user='root',  # Default MySQL user for AMPPS
        password='mysql',  # MySQL password
        database='qcdb'  # Your database name
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

        # If the lot does not exist, insert it
        if not result:
            print(f"Inserting lot_number: {lot_number} into lots table.")
            cursor.execute("INSERT INTO lots (lot_number, production_date) VALUES (%s, CURDATE())", (lot_number,))

    # Open and process the CSV file
    with open(csv_file_path, 'r') as file:
        contents = csv.reader(file)
        next(contents)  # Skip the header row

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
            # Strip any empty trailing values
            row = [r if r != '' else None for r in row]

            # Check if the row has enough valid columns (Expected: 18 non-null)
            if len([x for x in row if x is not None]) != 18:
                print(f"Skipping incomplete or invalid row: {row}")
                continue

            # Ensure the lot_number exists in the lots table
            lot_number = row[0]
            check_or_insert_lot(lot_number)

            # Insert into the ICP table
            cursor.execute(insert_icp_records, (lot_number, 'Pass', row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17]))

    # Commit the changes
    connection.commit()

    # Close the connection
    connection.close()

    print(f"Database updated from CSV: {csv_file_path}")

# Class to handle file system events (renamed to `AutoUpdate`)
class AutoUpdate(FileSystemEventHandler):
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    # This function will be triggered when the CSV file is modified
    def on_modified(self, event):
        if event.src_path == self.csv_file_path:
            print(f"Detected change in {self.csv_file_path}. Updating the database.")
            update_database_from_csv(self.csv_file_path)

# Main function to start monitoring the CSV file
def monitor_csv(csv_file_path):
    print(f"Monitoring file: {csv_file_path}")
    event_handler = AutoUpdate(csv_file_path)
    observer = Observer()
    observer.schedule(event_handler, path=csv_file_path, recursive=False)
    observer.start()
    print(f"Started monitoring {csv_file_path} for changes...")

    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# Start monitoring the CSV file
monitor_csv(csv_file_path)

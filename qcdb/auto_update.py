import csv
import mysql.connector
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Specify the CSV file path
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Database connection function
def connect_to_database():
    try:
        return mysql.connector.connect(
            host='localhost',
            user='root',  # Default MySQL user for AMPPS
            password='mysql',  # MySQL password
            database='qcdb'  # Your database name
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to the database: {err}")
        return None

# Function to process the CSV file and update the database
def update_database_from_csv(csv_file_path):
    print("Processing the CSV file for updates...")
    connection = connect_to_database()
    if connection is None:
        print("Database connection failed. Update aborted.")
        return
    cursor = connection.cursor()

    # Function to check if a lot exists in the 'lots' table, insert if not
    def check_or_insert_lot(lot_number, product):
        cursor.execute("SELECT lot_number FROM lots WHERE lot_number = %s", (lot_number,))
        result = cursor.fetchone()
        if not result:
            print(f"Inserting lot_number: {lot_number} into lots table.")
            cursor.execute("INSERT INTO lots (lot_number, product, production_date) VALUES (%s, %s, CURDATE())", (lot_number, product))

    # Open and process the CSV file
    try:
        with open(csv_file_path, 'r') as file:
            contents = csv.reader(file)
            headers = next(contents)  # Skip the header row

            # SQL query for each table based on CSV structure
            insert_icp_records = '''
                INSERT INTO icp (lot_number, status, Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Sn = VALUES(Sn), Si = VALUES(Si), Ca = VALUES(Ca), 
                                       Cr = VALUES(Cr), Cu = VALUES(Cu), Zr = VALUES(Zr), 
                                       Fe = VALUES(Fe), Na = VALUES(Na), Ni = VALUES(Ni), 
                                       Zn = VALUES(Zn), Co = VALUES(Co)
            '''
            insert_solid_content_records = '''
                INSERT INTO solid_content (lot_number, status, solid_content)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE solid_content = VALUES(solid_content)
            '''
            insert_cnt_content_records = '''
                INSERT INTO cnt_content (lot_number, status, cnt_content)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE cnt_content = VALUES(cnt_content)
            '''
            insert_particle_size_records = '''
                INSERT INTO particle_size (lot_number, status, particle_size)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE particle_size = VALUES(particle_size)
            '''
            insert_moisture_records = '''
                INSERT INTO moisture (lot_number, status, moisture)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE moisture = VALUES(moisture)
            '''
            insert_electrical_resistance_records = '''
                INSERT INTO electrical_resistance (lot_number, status, electrical_resistance)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE electrical_resistance = VALUES(electrical_resistance)
            '''
            insert_magnetic_impurity_records = '''
                INSERT INTO magnetic_impurity (lot_number, status, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE magnetic_impurity_sum = VALUES(magnetic_impurity_sum),
                                        mag_Cr = VALUES(mag_Cr), mag_Fe = VALUES(mag_Fe),
                                        mag_Ni = VALUES(mag_Ni), mag_Zn = VALUES(mag_Zn)
            '''

            # Insert data based on CSV rows
            for row in contents:
                if len(row) < 24:  # Adjusted expected number of columns
                    print(f"Skipping incomplete or invalid row: {row}")
                    continue

                lot_number = row[0]
                product = row[1]
                solid_content_value = row[2]
                cnt_content_value = row[3]
                particle_size_value = row[4]
                moisture_value = row[5]
                electrical_resistance_value = row[6]
                magnetic_impurity_sum = row[7]
                mag_Cr = row[8]
                mag_Fe = row[9]
                mag_Ni = row[10]
                mag_Zn = row[11]
                icp_values = row[12:24]
                status = 'Pass'

                # Ensure lot_number exists in the lots table
                check_or_insert_lot(lot_number, product)

                # Insert data into the respective tables
                try:
                    cursor.execute(insert_icp_records, (lot_number, status, *icp_values))
                    cursor.execute(insert_solid_content_records, (lot_number, status, solid_content_value))
                    cursor.execute(insert_cnt_content_records, (lot_number, status, cnt_content_value))
                    cursor.execute(insert_particle_size_records, (lot_number, status, particle_size_value))
                    cursor.execute(insert_moisture_records, (lot_number, status, moisture_value))
                    cursor.execute(insert_electrical_resistance_records, (lot_number, status, electrical_resistance_value))
                    cursor.execute(insert_magnetic_impurity_records, (lot_number, status, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn))
                    print(f"Inserted data for lot_number {lot_number} into respective tables.")
                except mysql.connector.Error as err:
                    print(f"Error inserting data for {lot_number}: {err}")

        # Commit changes after processing all rows
        connection.commit()
        print(f"Database updated from CSV: {csv_file_path}")

    except Exception as e:
        print(f"Error processing CSV file: {e}")

    finally:
        connection.close()

# Class to handle file system events
class AutoUpdate(FileSystemEventHandler):
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    # Triggered when the CSV file is modified
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
        print("Stopping file monitoring...")
        observer.stop()
    observer.join()

# Start monitoring the CSV file
monitor_csv(csv_file_path)

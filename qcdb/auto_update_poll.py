import csv
import mysql.connector
import os
import time
from datetime import datetime, timedelta

# Initialize global variable to track the last processed row
last_processed_row = 0

# Database connection function
def connect_to_database():
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

# Function to process the CSV file and update the database
def update_database_from_csv(csv_file_path):
    global last_processed_row
    print("Checking for new data to update...")

    connection = connect_to_database()
    if not connection:
        print("Failed to connect to the database. Skipping update.")
        return

    cursor = connection.cursor()

    # Function to check if a lot exists in the 'lots' table, insert if not
    def check_or_insert_lot(lot_number):
        cursor.execute("SELECT lot_number FROM lots WHERE lot_number = %s", (lot_number,))
        result = cursor.fetchone()
        if not result:
            print(f"Inserting lot_number: {lot_number} into lots table.")
            cursor.execute("INSERT INTO lots (lot_number, production_date) VALUES (%s, CURDATE())", (lot_number,))

    try:
        with open(csv_file_path, 'r') as file:
            contents = list(csv.reader(file))  # Read all rows into a list
            headers = contents[0]  # First row is the header
            print(f"CSV Headers Detected: {headers}")

            # Check if there are new rows since the last processed row
            new_rows = contents[last_processed_row + 1:]  # Skip header and previously processed rows
            if not new_rows:
                print("No new rows to process.")
                return  # Exit if there are no new rows

            print(f"Found {len(new_rows)} new rows to process.")

            # SQL query for the every tables
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
            insert_particle_size_records = '''
                INSERT INTO particle_size (lot_number, status, particle_size)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE particle_size = VALUES(particle_size)
            '''
            insert_viscosity_records = '''
                INSERT INTO viscosity (lot_number, status, viscosity)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE viscosity = VALUES(viscosity)
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
                INSERT INTO magnetic_impurity (lot_number, status, magnetic_impurity)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE magnetic_impurity = VALUES(magnetic_impurity)
            '''

            # Insert each new row into the database
            for index, row in enumerate(new_rows, start=last_processed_row + 1):
                if len(row) < 18:
                    print(f"Skipping incomplete or invalid row: {row}")
                    continue

                lot_number = row[0]
                solid_content_value = row[1]
                particle_size_value = row[2]
                viscosity_value = row[3]
                moisture_value = row[4]
                electrical_resistance_value = row[5]
                magnetic_impurity_value = row[6]
                icp_values = row[7:18]
                status = 'Pass'

                # Ensure lot_number is in 'lots' table
                check_or_insert_lot(lot_number)

                # Insert into the ICP table
                try:
                    # Insert each data type into its respective table
                    cursor.execute(insert_icp_records, (lot_number, status, *icp_values))
                    cursor.execute(insert_solid_content_records, (lot_number, status, solid_content_value))
                    cursor.execute(insert_particle_size_records, (lot_number, status, particle_size_value))
                    cursor.execute(insert_viscosity_records, (lot_number, status, viscosity_value))
                    cursor.execute(insert_moisture_records, (lot_number, status, moisture_value))
                    cursor.execute(insert_electrical_resistance_records, (lot_number, status, electrical_resistance_value))
                    cursor.execute(insert_magnetic_impurity_records, (lot_number, status, magnetic_impurity_value))

                    print(f"Inserted data for lot_number {lot_number} into the ICP table.")
                except mysql.connector.Error as err:
                    print(f"Error inserting ICP data for {lot_number}: {err}")

                # Update last processed row after each successful insert
                last_processed_row = index

    except Exception as e:
        print(f"Error processing CSV file: {e}")
    finally:
        # Commit changes and close the connection
        connection.commit()
        connection.close()
        print("Database update completed.")

# Function to check the CSV file at the top of every hour
def poll_for_changes_every_hour(csv_file_path):
    global last_processed_row

    while True:
        try:
            # Get current time and calculate the next exact hour
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_duration = (next_hour - now).total_seconds()

            # Sleep until the top of the next hour
            print(f"Sleeping until next hour update at: {next_hour.strftime('%H:%M:%S')}")
            time.sleep(sleep_duration)

            # Check if new data exists and only update if there are new rows
            previous_row_count = last_processed_row
            update_database_from_csv(csv_file_path)

            # Only update the database if there are new rows
            if last_processed_row > previous_row_count:
                print(f"Database was updated with new rows up to row {last_processed_row}.")
            else:
                print("No new data found. Database update skipped.")

        except Exception as e:
            print(f"Error in hourly polling loop: {e}")

# Specify the CSV file path
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Start polling the CSV file every hour exactly at the top of the hour
poll_for_changes_every_hour(csv_file_path)

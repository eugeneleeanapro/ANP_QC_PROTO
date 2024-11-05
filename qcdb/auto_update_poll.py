import csv
import mysql.connector
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

# Product specification definitions
specifications = {
    "6.5J": {
        "Solid Content (%)": (6.4, 6.7),
        "CNT Content (%)": (4.9, 5.2),
        "Viscosity (cP)": 3000,
        "Particle Size (μm)": 3.0,
        "Electrode Resistance (Ω-cm)": 30,
        "Impurities": {
            "Ca": 1, "Cr": 1, "Cu": 1, "Fe": 2.3, "Na": 10,
            "Ni": 1, "Zn": 1, "Zr": 1
        },
        "Magnetic Impurity (ppb)": 30
    },
    "5.4J": {
        "Solid Content (%)": (5.3, 5.6),
        "CNT Content (%)": (4.4, 4.7),
        "Viscosity (cP)": 10000,
        "Particle Size (μm)": 3.0,
        "Moisture (ppm)": 1000,
        "Electrode Resistance (Ω-cm)": 45,
        "Impurities": {
            "Ca": 20, "Cr": 1, "Cu": 1, "Fe": 2.0, "Na": 10,
            "Ni": 1, "Zn": 1, "Zr": 1
        }
    },
    "6.0J": {
        "Solid Content (%)": (5.9, 6.2),
        "CNT Content (%)": (4.9, 5.2),
        "Viscosity (cP)": 10000,
        "Particle Size (μm)": 3.0,
        "Moisture (ppm)": 1000,
        "Electrode Resistance (Ω-cm)": 45,
        "Impurities": {
            "Ca": 20, "Cr": 1, "Cu": 1, "Fe": 2.3, "Na": 10,
            "Ni": 1, "Zn": 1, "Zr": 1
        },
        "Magnetic Impurity (ppb)": 30
    }
}

# Function to convert values to float if possible
def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
    
# Check specifications based on product type
def check_specifications(product_name, test_data):
    specs = specifications.get(product_name)
    if not specs:
        print(f"No specifications found for product {product_name}.")
        return "FAIL"

    for param, value in test_data.items():
        if value is None:
            continue  # Skip check if the value is None (null)
        if param in ["Solid Content (%)", "CNT Content (%)"]:
            if not (specs[param][0] <= value <= specs[param][1]):
                return "FAIL"
        elif param == "Viscosity (cP)" and value > specs["Viscosity (cP)"]:
            return "FAIL"
        elif param == "Particle Size (μm)" and value >= specs["Particle Size (μm)"]:
            return "FAIL"
        elif param == "Moisture (ppm)" and value > specs.get("Moisture (ppm)", float('inf')):
            return "FAIL"
        elif param == "Electrode Resistance (Ω-cm)" and value > specs["Electrode Resistance (Ω-cm)"]:
            return "FAIL"
        elif param in specs["Impurities"] and value > specs["Impurities"][param]:
            return "FAIL"
        elif param == "Magnetic Impurity (ppb)" and value > specs.get("Magnetic Impurity (ppb)", float('inf')):
            return "FAIL"
    return "PASS"

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
    def check_or_insert_lot(lot_number, product):
        cursor.execute("SELECT lot_number FROM lots WHERE lot_number = %s", (lot_number,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("INSERT INTO lots (lot_number, product, production_date) VALUES (%s, %s, CURDATE())", (lot_number, product))
            print(f"Inserted new lot_number {lot_number} into lots table.")

    # Helper function to handle empty strings as NULL values
    def to_decimal(value):
        return float(value) if value else None

    try:
        with open(csv_file_path, 'r') as file:
            contents = list(csv.reader(file))
            headers = contents[0]
            print(f"CSV Headers Detected: {headers}")

            # Get rows after the last processed one
            new_rows = contents[last_processed_row + 1:]
            if not new_rows:
                print("No new rows to process.")
                return

            print(f"Found {len(new_rows)} new rows to process.")

            # SQL queries for each table
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
                INSERT INTO magnetic_impurity (lot_number, status, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE magnetic_impurity_sum = VALUES(magnetic_impurity_sum),
                                        mag_Cr = VALUES(mag_Cr), mag_Fe = VALUES(mag_Fe),
                                        mag_Ni = VALUES(mag_Ni), mag_Zn = VALUES(mag_Zn)
            '''

            # Insert each new row into the database
            for index, row in enumerate(new_rows, start=last_processed_row + 1):
                if len(row) < 24:
                    print(f"Skipping incomplete row: {row}")
                    continue

                lot_number = row[0]
                product = row[1]
                solid_content_value = to_decimal(row[2])
                cnt_content_value = to_decimal(row[3])
                particle_size_value = to_decimal(row[4])
                viscosity_value = to_decimal(row[5])
                moisture_value = to_decimal(row[6])
                electrical_resistance_value = to_decimal(row[7])
                magnetic_impurity_sum = to_decimal(row[8])
                mag_Cr = to_decimal(row[9])
                mag_Fe = to_decimal(row[10])
                mag_Ni = to_decimal(row[11])
                mag_Zn = to_decimal(row[12])
                icp_values = [to_decimal(v) for v in row[13:24]]
                
                # Form test data for specification check
                test_data = {
                    "Solid Content (%)": solid_content_value,
                    "CNT Content (%)": cnt_content_value,
                    "Viscosity (cP)": viscosity_value,
                    "Particle Size (μm)": particle_size_value,
                    "Moisture (ppm)": moisture_value,
                    "Electrode Resistance (Ω-cm)": electrical_resistance_value,
                    "Ca": icp_values[2], "Cr": icp_values[3], "Cu": icp_values[4],
                    "Fe": icp_values[6], "Na": icp_values[7], "Ni": icp_values[8],
                    "Zn": icp_values[9], "Zr": icp_values[10],
                    "Magnetic Impurity (ppb)": magnetic_impurity_sum
                }

                # Determine pass/fail status
                status = check_specifications(product, test_data)
                print(f"Lot {lot_number} for product {product}: {status}")

                # Ensure lot_number exists in 'lots' table
                check_or_insert_lot(lot_number, product)

                # Insert into tables
                try:
                    cursor.execute(insert_icp_records, (lot_number, status, *icp_values))
                    cursor.execute(insert_solid_content_records, (lot_number, status, solid_content_value))
                    cursor.execute(insert_cnt_content_records, (lot_number, status, cnt_content_value))
                    cursor.execute(insert_particle_size_records, (lot_number, status, particle_size_value))
                    cursor.execute(insert_viscosity_records, (lot_number, status, viscosity_value))
                    cursor.execute(insert_moisture_records, (lot_number, status, moisture_value))
                    cursor.execute(insert_electrical_resistance_records, (lot_number, status, electrical_resistance_value))
                    cursor.execute(insert_magnetic_impurity_records, (lot_number, status, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn))

                    print(f"Inserted data for lot_number {lot_number} into respective tables.")
                except mysql.connector.Error as err:
                    print(f"Error inserting data for lot_number {lot_number}: {err}")

                last_processed_row = index  # Update last processed row

    except Exception as e:
        print(f"Error processing CSV file: {e}")
    finally:
        connection.commit()
        connection.close()
        print("Database update completed.")

# Poll for changes every hour
def poll_for_changes_every_hour(csv_file_path):
    while True:
        try:
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_duration = (next_hour - now).total_seconds()

            print(f"Sleeping until next hour update at: {next_hour.strftime('%H:%M:%S')}")
            time.sleep(sleep_duration)

            print(f"Checking for updates at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            update_database_from_csv(csv_file_path)
            
        except Exception as e:
            print(f"Error in hourly polling loop: {e}")

# Specify the CSV file path
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Start polling the CSV file every hour exactly at the top of the hour
poll_for_changes_every_hour(csv_file_path)

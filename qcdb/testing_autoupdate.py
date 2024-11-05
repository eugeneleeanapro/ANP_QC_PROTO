import csv
import mysql.connector
import time
from datetime import datetime, timedelta

# Initialize global variable to track the last processed row
last_processed_row = 0

# Specifications for products
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

# Function to evaluate product specifications for PASS/FAIL
def check_specifications(product_name, test_data):
    specs = specifications.get(product_name)
    if not specs:
        print(f"No specifications found for product {product_name}.")
        return "FAIL"

    for param, value in test_data.items():
        if value is None:
            continue  # Allow None (null) values in test data
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

# Insert data into database with status
def insert_data_with_status(cursor, lot_number, product_name, test_data, status):
    insert_query = '''
        INSERT INTO lots (lot_number, product, status, solid_content, cnt_content, viscosity, particle_size,
                          moisture, electrode_resistance, impurity_ca, impurity_cr, impurity_cu, impurity_fe,
                          impurity_na, impurity_ni, impurity_zn, impurity_zr, magnetic_impurity_sum)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE status = VALUES(status), solid_content = VALUES(solid_content),
                                cnt_content = VALUES(cnt_content), viscosity = VALUES(viscosity),
                                particle_size = VALUES(particle_size), moisture = VALUES(moisture),
                                electrode_resistance = VALUES(electrode_resistance),
                                impurity_ca = VALUES(impurity_ca), impurity_cr = VALUES(impurity_cr),
                                impurity_cu = VALUES(impurity_cu), impurity_fe = VALUES(impurity_fe),
                                impurity_na = VALUES(impurity_na), impurity_ni = VALUES(impurity_ni),
                                impurity_zn = VALUES(impurity_zn), impurity_zr = VALUES(impurity_zr),
                                magnetic_impurity_sum = VALUES(magnetic_impurity_sum)
    '''
    values = (
        lot_number, product_name, status, test_data.get("Solid Content (%)"), test_data.get("CNT Content (%)"),
        test_data.get("Viscosity (cP)"), test_data.get("Particle Size (μm)"), test_data.get("Moisture (ppm)"),
        test_data.get("Electrode Resistance (Ω-cm)"), test_data.get("Ca"), test_data.get("Cr"), test_data.get("Cu"),
        test_data.get("Fe"), test_data.get("Na"), test_data.get("Ni"), test_data.get("Zn"), test_data.get("Zr"),
        test_data.get("Magnetic Impurity (ppb)")
    )
    cursor.execute(insert_query, values)

# Function to process the CSV file and update the database
def update_database_from_csv(csv_file_path):
    global last_processed_row
    print("Checking for new data to update...")

    connection = connect_to_database()
    if not connection:
        print("Failed to connect to the database. Skipping update.")
        return

    cursor = connection.cursor()

    try:
        with open(csv_file_path, 'r') as file:
            contents = list(csv.reader(file))  # Read all rows into a list
            headers = contents[0]  # First row is the header
            print(f"CSV Headers Detected: {headers}")

            new_rows = contents[last_processed_row + 1:]
            if not new_rows:
                print("No new rows to process.")
                return

            print(f"Found {len(new_rows)} new rows to process.")

            for index, row in enumerate(new_rows, start=last_processed_row + 1):
                if len(row) < 24:
                    print(f"Skipping incomplete row: {row}")
                    continue

                lot_number = row[0]
                product = row[1]
                test_data = {
                    "Solid Content (%)": float(row[2]) if row[2] else None,
                    "CNT Content (%)": float(row[3]) if row[3] else None,
                    "Viscosity (cP)": float(row[4]) if row[4] else None,
                    "Particle Size (μm)": float(row[5]) if row[5] else None,
                    "Moisture (ppm)": float(row[6]) if row[6] else None,
                    "Electrode Resistance (Ω-cm)": float(row[7]) if row[7] else None,
                    "Ca": float(row[8]) if row[8] else None,
                    "Cr": float(row[9]) if row[9] else None,
                    "Cu": float(row[10]) if row[10] else None,
                    "Fe": float(row[11]) if row[11] else None,
                    "Na": float(row[12]) if row[12] else None,
                    "Ni": float(row[13]) if row[13] else None,
                    "Zn": float(row[14]) if row[14] else None,
                    "Zr": float(row[15]) if row[15] else None,
                    "Magnetic Impurity (ppb)": float(row[16]) if row[16] else None
                }

                if not lot_number or not product:
                    print("Insert lot number and/or product name.")
                    continue

                status = check_specifications(product, test_data)
                print(f"Lot {lot_number} for product {product}: {status}")
                insert_data_with_status(cursor, lot_number, product, test_data, status)
                last_processed_row = index

    except Exception as e:
        print(f"Error processing CSV file: {e}")

    finally:
        connection.commit()
        connection.close()
        print("Database update completed.")

# Poll for changes every 3 minutes
def poll_for_changes_every_3_minutes(csv_file_path):
    global last_processed_row
    while True:
        try:
            print("Waiting 3 minutes for the next update...")
            time.sleep(10)
            previous_row_count = last_processed_row
            update_database_from_csv(csv_file_path)
            if last_processed_row > previous_row_count:
                print(f"Database updated with new rows up to row {last_processed_row}.")
            else:
                print("No new data found. Update skipped.")
        except Exception as e:
            print(f"Error in polling loop: {e}")

# Specify the CSV file path
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Start polling the CSV file every 3 minutes
poll_for_changes_every_3_minutes(csv_file_path)

import csv
import mysql.connector
import time
from datetime import datetime, timedelta

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

# Product specifications dictionary
specifications = {
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
    },
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
    }
}

# Convert values to float safely
def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# Check individual parameter specifications
def check_individual_specifications(product_name, param, value):
    specs = specifications.get(product_name)
    if not specs or value is None:
        return "FAIL"

    if param in ["Solid Content (%)", "CNT Content (%)"]:
        min_val, max_val = specs[param]
        return "PASS" if min_val <= value <= max_val else "FAIL"
    elif param == "Viscosity (cP)":
        return "PASS" if value <= specs["Viscosity (cP)"] else "FAIL"
    elif param == "Particle Size (μm)":
        return "PASS" if value < specs["Particle Size (μm)"] else "FAIL"
    elif param == "Moisture (ppm)":
        return "PASS" if value <= specs.get("Moisture (ppm)", float('inf')) else "FAIL"
    elif param == "Electrode Resistance (Ω-cm)":
        return "PASS" if value <= specs["Electrode Resistance (Ω-cm)"] else "FAIL"
    elif param in specs["Impurities"]:
        return "PASS" if value <= specs["Impurities"][param] else "FAIL"
    elif param == "Magnetic Impurity (ppb)":
        return "PASS" if value <= specs.get("Magnetic Impurity (ppb)", float('inf')) else "FAIL"
    return "FAIL"

# Function to check or insert lot and mark as processed if already processed
def check_or_insert_lot(cursor, lot_number, product):
    cursor.execute("SELECT processed FROM lots WHERE lot_number = %s", (lot_number,))
    result = cursor.fetchone()
    
    if result:
        if result[0]:  # If `processed` is True, skip this lot
            print(f"Skipping already processed lot number: {lot_number}")
            return False
        cursor.execute("UPDATE lots SET production_date = CURDATE() WHERE lot_number = %s", (lot_number,))
    else:
        cursor.execute("INSERT INTO lots (lot_number, product, production_date, processed) VALUES (%s, %s, CURDATE(), FALSE)", (lot_number, product))
    return True

# Function to update the database from CSV data
def update_database_from_csv(csv_file_path):
    print("Checking for new data to update...")

    connection = connect_to_database()
    if not connection:
        print("Failed to connect to the database. Skipping update.")
        return

    cursor = connection.cursor()

    try:
        with open(csv_file_path, 'r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            headers = next(reader)  # skip header

            for row in reader:
                if len(row) < 24:
                    print(f"Skipping incomplete row: {row}")
                    continue

                lot_number = row[0]
                product = row[1]
                solid_content_value = safe_float(row[2])
                cnt_content_value = safe_float(row[3])
                particle_size_value = safe_float(row[4])
                viscosity_value = safe_float(row[5])
                moisture_value = safe_float(row[6])
                electrical_resistance_value = safe_float(row[7])
                magnetic_impurity_sum = safe_float(row[8])
                mag_Cr = safe_float(row[9])
                mag_Fe = safe_float(row[10])
                mag_Ni = safe_float(row[11])
                mag_Zn = safe_float(row[12])
                icp_values = [safe_float(val) for val in row[13:24]]

                test_data = {
                    "Solid Content (%)": solid_content_value,
                    "CNT Content (%)": cnt_content_value,
                    "Viscosity (cP)": viscosity_value,
                    "Particle Size (μm)": particle_size_value,
                    "Moisture (ppm)": moisture_value,
                    "Electrode Resistance (Ω-cm)": electrical_resistance_value,
                    "Ca": mag_Cr, "Cr": mag_Cr, "Cu": mag_Fe, "Fe": mag_Fe,
                    "Na": mag_Ni, "Ni": mag_Ni, "Zn": mag_Zn, "Zr": mag_Zn,
                    "Magnetic Impurity (ppb)": magnetic_impurity_sum
                }

                # Skip row if already processed
                if not check_or_insert_lot(cursor, lot_number, product):
                    continue

                statuses = {param: check_individual_specifications(product, param, value) for param, value in test_data.items()}

                try:
                    # Insert or update each parameter
                    cursor.execute("INSERT INTO solid_content (lot_number, status, solid_content) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE solid_content = VALUES(solid_content), status = %s", (lot_number, statuses["Solid Content (%)"], solid_content_value, statuses["Solid Content (%)"]))
                    cursor.execute("INSERT INTO cnt_content (lot_number, status, cnt_content) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE cnt_content = VALUES(cnt_content), status = %s", (lot_number, statuses["CNT Content (%)"], cnt_content_value, statuses["CNT Content (%)"]))
                    cursor.execute("INSERT INTO particle_size (lot_number, status, particle_size) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE particle_size = VALUES(particle_size), status = %s", (lot_number, statuses["Particle Size (μm)"], particle_size_value, statuses["Particle Size (μm)"]))
                    cursor.execute("INSERT INTO viscosity (lot_number, status, viscosity) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE viscosity = VALUES(viscosity), status = %s", (lot_number, statuses["Viscosity (cP)"], viscosity_value, statuses["Viscosity (cP)"]))
                    cursor.execute("INSERT INTO moisture (lot_number, status, moisture) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE moisture = VALUES(moisture), status = %s", (lot_number, statuses["Moisture (ppm)"], moisture_value, statuses["Moisture (ppm)"]))
                    cursor.execute("INSERT INTO electrical_resistance (lot_number, status, electrical_resistance) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE electrical_resistance = VALUES(electrical_resistance), status = %s", (lot_number, statuses["Electrode Resistance (Ω-cm)"], electrical_resistance_value, statuses["Electrode Resistance (Ω-cm)"]))
                    cursor.execute("INSERT INTO magnetic_impurity (lot_number, status, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE magnetic_impurity_sum = VALUES(magnetic_impurity_sum), status = %s", (lot_number, statuses["Magnetic Impurity (ppb)"], magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn, statuses["Magnetic Impurity (ppb)"]))
                    cursor.execute("INSERT INTO icp (lot_number, status, Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE Sn = VALUES(Sn), Si = VALUES(Si), Ca = VALUES(Ca), Cr = VALUES(Cr), Cu = VALUES(Cu), Zr = VALUES(Zr), Fe = VALUES(Fe), Na = VALUES(Na), Ni = VALUES(Ni), Zn = VALUES(Zn), Co = VALUES(Co)", (lot_number, statuses["CNT Content (%)"], *icp_values))

                    # Mark the lot as processed
                    cursor.execute("UPDATE lots SET processed = TRUE WHERE lot_number = %s", (lot_number,))
                    connection.commit()

                except mysql.connector.Error as err:
                    print(f"Error inserting data for lot_number {lot_number}: {err}")
                    connection.rollback()

    except Exception as e:
        print(f"Error processing CSV file: {e}")
    finally:
        connection.close()
        print("Database update completed.")

# Polling loop to run every hour
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
csv_file_path = 'C:/Users/GilbertYoon/OneDrive - ANP ENERTECH INC/Desktop/ERP/QC_mastersheet_csv.csv'

# Start polling the CSV file every hour exactly at the top of the hour
poll_for_changes_every_hour(csv_file_path)
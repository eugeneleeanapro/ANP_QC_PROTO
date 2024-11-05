import csv
import mysql.connector
import time
from datetime import datetime, timedelta

# Initialize global variable to track the last processed row
last_processed_row = 0

# Specifications for pass/fail check
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

# Check specifications based on product type
def check_specifications(product, test_data):
    specs = specifications.get(product)
    if not specs:
        print(f"No specifications found for product {product}.")
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

# Insert data into database with status
def insert_data_with_status(cursor, lot_number, product, test_data, status):
    insert_query = '''
        INSERT INTO lots (lot_number, product, status, solid_content, cnt_content, viscosity, particle_size,
                          moisture, electrical_resistance, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn,
                          Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE status = VALUES(status), solid_content = VALUES(solid_content),
                                cnt_content = VALUES(cnt_content), viscosity = VALUES(viscosity),
                                particle_size = VALUES(particle_size), moisture = VALUES(moisture),
                                electrical_resistance = VALUES(electrical_resistance),
                                magnetic_impurity_sum = VALUES(magnetic_impurity_sum), mag_Cr = VALUES(mag_Cr),
                                mag_Fe = VALUES(mag_Fe), mag_Ni = VALUES(mag_Ni), mag_Zn = VALUES(mag_Zn),
                                Sn = VALUES(Sn), Si = VALUES(Si), Ca = VALUES(Ca), Cr = VALUES(Cr),
                                Cu = VALUES(Cu), Zr = VALUES(Zr), Fe = VALUES(Fe), Na = VALUES(Na),
                                Ni = VALUES(Ni), Zn = VALUES(Zn), Co = VALUES(Co)
    '''
    values = (
        lot_number, product, status, test_data.get("Solid Content (%)"), test_data.get("CNT Content (%)"),
        test_data.get("Viscosity (cP)"), test_data.get("Particle Size (μm)"), test_data.get("Moisture (ppm)"),
        test_data.get("Electrode Resistance (Ω-cm)"), test_data.get("Magnetic Impurity Sum"),
        test_data.get("Cr"), test_data.get("Fe"), test_data.get("Ni"), test_data.get("Zn"),
        test_data.get("Sn"), test_data.get("Si"), test_data.get("Ca"), test_data.get("Cr"),
        test_data.get("Cu"), test_data.get("Zr"), test_data.get("Fe"), test_data.get("Na"),
        test_data.get("Ni"), test_data.get("Zn"), test_data.get("Co")
    )
    cursor.execute(insert_query, values)

# Process the CSV file and update the database
def update_database_from_csv(csv_file_path):
    global last_processed_row
    connection = connect_to_database()
    if not connection:
        print("Failed to connect to the database. Skipping update.")
        return
    cursor = connection.cursor()

    with open(csv_file_path, 'r', encoding='utf-8-sig') as file:
        contents = list(csv.reader(file))
        headers = contents[0]
        print(f"CSV Headers Detected: {headers}")

        new_rows = contents[last_processed_row + 1:]
        if not new_rows:
            print("No new rows to process.")
            return

        for index, row in enumerate(new_rows, start=last_processed_row + 1):
            lot_number = row[0]
            product = row[1]

            if not lot_number or not product:
                print("Insert lot number and/or product name.")
                continue

            test_data = {
                "Solid Content (%)": float(row[2]) if row[2] else None,
                "CNT Content (%)": float(row[3]) if row[3] else None,
                "Viscosity (cP)": float(row[4]) if row[4] else None,
                "Particle Size (μm)": float(row[5]) if row[5] else None,
                "Moisture (ppm)": float(row[6]) if row[6] else None,
                "Electrode Resistance (Ω-cm)": float(row[7]) if row[7] else None,
                "Magnetic Impurity Sum": float(row[8]) if row[8] else None,
                "Cr": float(row[9]) if row[9] else None,
                "Fe": float(row[10]) if row[10] else None,
                "Ni": float(row[11]) if row[11] else None,
                "Zn": float(row[12]) if row[12] else None,
                "Sn": float(row[13]) if row[13] else None,
                "Si": float(row[14]) if row[14] else None,
                "Ca": float(row[15]) if row[15] else None,
                "Cr": float(row[16]) if row[16] else None,
                "Cu": float(row[17]) if row[17] else None,
                "Zr": float(row[18]) if row[18] else None,
                "Fe": float(row[19]) if row[19] else None,
                "Na": float(row[20]) if row[20] else None,
                "Ni": float(row[21]) if row[21] else None,
                "Zn": float(row[22]) if row[22] else None,
                "Co": float(row[23]) if row[23] else None
            }

            status = check_specifications(product, test_data)
            print(f"Lot {lot_number} for product {product}: {status}")
            insert_data_with_status(cursor, lot_number, product, test_data, status)
            last_processed_row = index

    connection.commit()
    cursor.close()
    connection.close()

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

# Start polling the CSV file every hour
poll_for_changes_every_hour(csv_file_path)

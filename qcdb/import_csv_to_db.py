import csv
import mysql.connector

# Connect to the MySQL database (qcdb)
connection = mysql.connector.connect(
    host='localhost',
    user='root',  # Default MySQL user for AMPPS
    password='mysql',  # MySQL password
    database='qcdb'  # Your database name
)

# Creating a cursor object to execute SQL queries on the database
cursor = connection.cursor()

# Table definitions

# Create the lots table to store lot_number and production_date
create_lots_table = '''CREATE TABLE IF NOT EXISTS lots(
                      lot_number VARCHAR(50) PRIMARY KEY,
                      product VARCHAR(100),
                      production_date DATE
                    );'''

# Create the icp table with a foreign key constraint referencing the lots table
create_icp_table = '''CREATE TABLE IF NOT EXISTS icp(
                      id INT AUTO_INCREMENT PRIMARY KEY,
                      lot_number VARCHAR(50),
                      status VARCHAR(10),
                      Sn DECIMAL(12, 9),
                      Si DECIMAL(12, 9),
                      Ca DECIMAL(12, 9),
                      Cr DECIMAL(12, 9),
                      Cu DECIMAL(12, 9),
                      Zr DECIMAL(12, 9),
                      Fe DECIMAL(12, 9),
                      Na DECIMAL(12, 9),
                      Ni DECIMAL(12, 9),
                      Zn DECIMAL(12, 9),
                      Co DECIMAL(12, 9),
                      CONSTRAINT fk_lot_number FOREIGN KEY (lot_number) REFERENCES lots(lot_number)
                    );'''

# Create additional tables for solid_content, particle_size, etc.
create_solid_content_table = '''CREATE TABLE IF NOT EXISTS solid_content(
                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                  lot_number VARCHAR(50),
                                  status VARCHAR(10),
                                  solid_content DECIMAL(10, 2)
                                );'''

create_cnt_content_table = '''CREATE TABLE IF NOT EXISTS CNT_content(
                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                  lot_number VARCHAR(50),
                                  status VARCHAR(10),
                                  CNT_content DECIMAL(10, 2)
                                );'''

create_particle_size_table = '''CREATE TABLE IF NOT EXISTS particle_size(
                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                  lot_number VARCHAR(50),
                                  status VARCHAR(10),
                                  particle_size DECIMAL(10, 2)
                                );'''

create_viscosity_table = '''CREATE TABLE IF NOT EXISTS viscosity(
                             id INT AUTO_INCREMENT PRIMARY KEY,
                             lot_number VARCHAR(50),
                             status VARCHAR(10),
                             viscosity DECIMAL(10, 2)
                           );'''

create_moisture_table = '''CREATE TABLE IF NOT EXISTS moisture(
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            lot_number VARCHAR(50),
                            status VARCHAR(10),
                            moisture DECIMAL(10, 2)
                          );'''

create_electrical_resistance_table = '''CREATE TABLE IF NOT EXISTS electrical_resistance(
                                         id INT AUTO_INCREMENT PRIMARY KEY,
                                         lot_number VARCHAR(50),
                                         status VARCHAR(10),
                                         electrical_resistance DECIMAL(10, 2)
                                       );'''

create_magnetic_impurity_table = '''CREATE TABLE IF NOT EXISTS magnetic_impurity(
                                      id INT AUTO_INCREMENT PRIMARY KEY,
                                      lot_number VARCHAR(50),
                                      status VARCHAR(10),
                                      magnetic_impurity_sum DECIMAL(10, 2),
                                      mag_Cr DECIMAL(10, 2),
                                      mag_Fe DECIMAL(10, 2),
                                      mag_Ni DECIMAL(10, 2),
                                      mag_Zn DECIMAL(10, 2)
                                    );'''

# Execute all table creation queries
cursor.execute(create_lots_table)
cursor.execute(create_icp_table)
cursor.execute(create_solid_content_table)
cursor.execute(create_cnt_content_table)
cursor.execute(create_particle_size_table)
cursor.execute(create_viscosity_table)
cursor.execute(create_moisture_table)
cursor.execute(create_electrical_resistance_table)
cursor.execute(create_magnetic_impurity_table)

# Function to check if a lot exists in the 'lots' table, insert if not
def check_or_insert_lot(lot_number, product):
    cursor.execute("SELECT lot_number FROM lots WHERE lot_number = %s", (lot_number,))
    result = cursor.fetchone()
    
    # If the lot does not exist, insert it
    if not result:
        print(f"Inserting lot_number: {lot_number} with product: {product} into lots table.")
        cursor.execute("INSERT INTO lots (lot_number, product, production_date) VALUES (%s, %s, CURDATE())", (lot_number, product))

# Open the CSV file and read only the last row
with open('C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv', 'r') as file:
    contents = list(csv.reader(file))  # Convert CSV reader to list
    if len(contents) < 2:
        print("CSV file has no data rows.")
    else:
        last_row = contents[-1]  # Get the last data row

        # Strip any empty trailing values in the last row
        last_row = [r if r != '' else None for r in last_row]

        # Ensure the row has enough valid columns (Expected: 24 non-null)
        if len([x for x in last_row if x is not None]) < 24:
            print(f"Skipping incomplete or invalid row: {last_row}")
        else:
            lot_number = last_row[0]
            product = last_row[1]  # Product is now column 1
            check_or_insert_lot(lot_number, product)  # Insert lot_number and product if they don't exist

            # SQL queries for data insertion
            insert_icp_records = '''
                INSERT INTO icp (lot_number, status, Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            insert_solid_content_records = '''
                INSERT INTO solid_content (lot_number, status, solid_content)
                VALUES (%s, %s, %s)
            '''
            insert_cnt_content_records = '''
                INSERT INTO CNT_content (lot_number, status, CNT_content)
                VALUES (%s, %s, %s)
            '''
            insert_particle_size_records = '''
                INSERT INTO particle_size (lot_number, status, particle_size)
                VALUES (%s, %s, %s)
            '''
            insert_viscosity_records = '''
                INSERT INTO viscosity (lot_number, status, viscosity)
                VALUES (%s, %s, %s)
            '''
            insert_moisture_records = '''
                INSERT INTO moisture (lot_number, status, moisture)
                VALUES (%s, %s, %s)
            '''
            insert_electrical_resistance_records = '''
                INSERT INTO electrical_resistance (lot_number, status, electrical_resistance)
                VALUES (%s, %s, %s)
            '''
            insert_magnetic_impurity_records = '''
                INSERT INTO magnetic_impurity (lot_number, status, magnetic_impurity_sum, mag_Cr, mag_Fe, mag_Ni, mag_Zn)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''

            # Insert into tables
            cursor.execute(insert_icp_records, (lot_number, 'Pass', last_row[13], last_row[14], last_row[15], last_row[16], last_row[17], last_row[18], last_row[19], last_row[20], last_row[21], last_row[22], last_row[23]))
            cursor.execute(insert_solid_content_records, (lot_number, 'Pass', last_row[2]))
            cursor.execute(insert_cnt_content_records, (lot_number, 'Pass', last_row[3]))
            cursor.execute(insert_particle_size_records, (lot_number, 'Pass', last_row[4]))
            cursor.execute(insert_viscosity_records, (lot_number, 'Pass', last_row[5]))
            cursor.execute(insert_moisture_records, (lot_number, 'Pass', last_row[6]))
            cursor.execute(insert_electrical_resistance_records, (lot_number, 'Pass', last_row[7]))
            cursor.execute(insert_magnetic_impurity_records, (lot_number, 'Pass', last_row[8], last_row[9], last_row[10], last_row[11], last_row[12]))

# Commit and verify insertion
connection.commit()
print("Last row of CSV successfully inserted into the database.")

# Close the database connection
connection.close()
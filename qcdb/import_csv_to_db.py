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
                                      magnetic_impurity DECIMAL(10, 2)
                                    );'''

# Execute all table creation queries
cursor.execute(create_lots_table)
cursor.execute(create_icp_table)
cursor.execute(create_solid_content_table)
cursor.execute(create_particle_size_table)
cursor.execute(create_viscosity_table)
cursor.execute(create_moisture_table)
cursor.execute(create_electrical_resistance_table)
cursor.execute(create_magnetic_impurity_table)

# Function to check if a lot exists in the 'lots' table, insert if not
def check_or_insert_lot(lot_number):
    cursor.execute("SELECT lot_number FROM lots WHERE lot_number = %s", (lot_number,))
    result = cursor.fetchone()
    
    # If the lot does not exist, insert it
    if not result:
        print(f"Inserting lot_number: {lot_number} into lots table.")
        cursor.execute("INSERT INTO lots (lot_number, production_date) VALUES (%s, CURDATE())", (lot_number,))

# Open your CSV file (use the correct path)
with open('C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv', 'r') as file:
    contents = csv.reader(file)
    next(contents)  # Skip the header row

    # SQL query for the icp table
    insert_icp_records = '''
        INSERT INTO icp (lot_number, status, Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    # SQL queries for other tables
    insert_solid_content_records = '''
        INSERT INTO solid_content (lot_number, status, solid_content)
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
        INSERT INTO magnetic_impurity (lot_number, status, magnetic_impurity)
        VALUES (%s, %s, %s)
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
        check_or_insert_lot(lot_number)  # Insert lot_number if it doesn't exist

        # Print row for debugging purposes
        print(f"Inserting into icp table: Lot Number: {lot_number}, Elements: {row[7:18]}")

        # Insert into the ICP table
        cursor.execute(insert_icp_records, (lot_number, 'Pass', row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17]))

        # Insert into the solid_content table
        cursor.execute(insert_solid_content_records, (lot_number, 'Pass', row[1]))

        # Insert into the particle_size table
        cursor.execute(insert_particle_size_records, (lot_number, 'Pass', row[2]))

        # Insert into the viscosity table
        cursor.execute(insert_viscosity_records, (lot_number, 'Pass', row[3]))

        # Insert into the moisture table
        cursor.execute(insert_moisture_records, (lot_number, 'Pass', row[4]))

        # Insert into the electrical_resistance table
        cursor.execute(insert_electrical_resistance_records, (lot_number, 'Pass', row[5]))

        # Insert into the magnetic_impurity table
        cursor.execute(insert_magnetic_impurity_records, (lot_number, 'Pass', row[6]))


# Commit the transaction to the database
connection.commit()

# Verify that the data has been successfully inserted
select_all = "SELECT * FROM icp"
cursor.execute(select_all)
rows = cursor.fetchall()

# Output the result to the console
for r in rows:
    print(r)

# Close the database connection
connection.close()

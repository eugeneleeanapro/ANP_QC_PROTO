import paramiko
import os
import pandas as pd  # Importing pandas for CSV processing
import mysql.connector  # Importing MySQL connector for database operations

# SFTP Configuration
SFTP_HOST = 'localhost'
SFTP_PORT = 22
SFTP_USERNAME = 'root'
SFTP_PASSWORD = 'mysql'
REMOTE_FILE_PATH = ""  # Use raw string for Windows path
LOCAL_FILE_PATH = ""  # Ensure correct local path

# Database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mysql',
    'database': 'ANP_QC_PROTO'
}

# Standard values for testing methods
IMPURITIES_STANDARD = {
    'Iron': 0.05,
    'Copper': 0.01,
    'Nickel': 0.02,
    'Chromium': 0.03
}

MAGNETIC_MATERIAL_STANDARD = {
    'Iron': 0.1,
    'Nickel': 0.02,
    'Cobalt': 0.03,
    'Chromium': 0.05
}

# Connect to the database
def connect_to_db():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Download the file via SFTP using paramiko
def download_file_via_sftp():
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USERNAME, password=SFTP_PASSWORD)
        print(f"Connected to {SFTP_HOST}")
        
        sftp = ssh_client.open_sftp()
        sftp.get(REMOTE_FILE_PATH, LOCAL_FILE_PATH)
        print(f"File downloaded from {REMOTE_FILE_PATH} to {LOCAL_FILE_PATH}")
        
        sftp.close()
        ssh_client.close()
        return LOCAL_FILE_PATH
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

# Process the row and insert data based on testing_method
def process_row(row, connection):
    testing_method = row['testing_method']
    element = row['element']
    lot_number = row['Lot']
    
    # Example of fetching the test result from the first column (adjust column as needed)
    test_result = row['US1A240508-1-1']  
    
    # Impurities standard handling
    if testing_method == 'impurities_standard':
        if element in IMPURITIES_STANDARD:
            if float(test_result) > IMPURITIES_STANDARD[element]:
                print(f"Test result {test_result} exceeds impurities standard for {element}. Lot {lot_number} will not be saved.")
                return  # Skip row
            
            insert_impurity_data(connection, lot_number, element, test_result)

    # Magnetic material standard handling
    elif testing_method == 'magnetic_material_standard':
        if element in MAGNETIC_MATERIAL_STANDARD:
            if float(test_result) > MAGNETIC_MATERIAL_STANDARD[element]:
                print(f"Test result {test_result} exceeds magnetic material standard for {element}. Lot {lot_number} will not be saved.")
                return  # Skip row
            
            insert_magnetic_material_data(connection, lot_number, element, test_result)
    
    else:
        # For other testing methods, just log or handle other test data
        print(f"Processing other test data for {lot_number} - {testing_method}")

# Insert impurities data into the database
def insert_impurity_data(connection, lot_number, element, test_result):
    cursor = connection.cursor()
    query = """
        INSERT INTO impurities (lot_number, element, test_result)
        VALUES (%s, %s, %s)
    """
    cursor.execute(query, (lot_number, element, test_result))
    connection.commit()
    cursor.close()

# Insert magnetic material data into the database
def insert_magnetic_material_data(connection, lot_number, element, test_result):
    cursor = connection.cursor()
    query = """
        INSERT INTO magnetic_material (lot_number, element, test_result)
        VALUES (%s, %s, %s)
    """
    cursor.execute(query, (lot_number, element, test_result))
    connection.commit()
    cursor.close()

# Main function to handle pulling data from server and processing it
def main():
    # Step 1: Download the file via SFTP
    downloaded_file = download_file_via_sftp()

    # Step 2: If file downloaded, process it
    if downloaded_file:
        # Load the CSV file into a DataFrame
        data = pd.read_csv(downloaded_file)
        
        # Connect to the database
        connection = connect_to_db()
        if connection is None:
            print("Failed to connect to the database.")
            return
        
        # Iterate through the rows and process each one
        for _, row in data.iterrows():
            process_row(row, connection)
        
        # Close the database connection
        connection.close()

if __name__ == "__main__":
    main()

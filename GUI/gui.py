import tkinter as tk
from tkinter import messagebox
import subprocess
import mysql.connector
import os

# Define the absolute paths for each script
COA_FILLING_PATH = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Documents/GitHub/ANP_QC_PROTO/COA/coa_filling.py"
AUTO_UPDATE_POLL_PATH = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Documents/GitHub/ANP_QC_PROTO/qcdb/auto_update_poll.py"
IMPORT_CSV_PATH = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Documents/GitHub/ANP_QC_PROTO/qcdb/import_csv_to_db.py"
LAST_PROCESSED_ROW_FILE = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Documents/GitHub/ANP_QC_PROTO/last_processed_row.txt"

# Database connection function for the reset button
def connect_to_database():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='mysql',
        database='qcdb'
    )

# Function to run auto_update_poll.py
def run_auto_update():
    global auto_update_process
    if auto_update_process and auto_update_process.poll() is None:
        messagebox.showinfo("Info", "Auto-update already running.")
    else:
        auto_update_process = subprocess.Popen(["python", AUTO_UPDATE_POLL_PATH])
        messagebox.showinfo("Info", "Auto-update started.")

# Function to stop auto_update_poll.py, run import_csv_to_db.py, and then restart auto_update_poll.py
def run_import_csv():
    global auto_update_process
    if auto_update_process and auto_update_process.poll() is None:
        auto_update_process.terminate()
        auto_update_process.wait()
    subprocess.run(["python", IMPORT_CSV_PATH])
    messagebox.showinfo("Info", "CSV data imported successfully.")
    run_auto_update()

# Function to run coa_filling.py with the provided lot number
def run_coa_filling():
    lot_number = lot_number_entry.get().strip()
    if not lot_number:
        messagebox.showwarning("Warning", "Please enter a lot number.")
        return
    subprocess.run(["python", COA_FILLING_PATH, lot_number])
    messagebox.showinfo("Info", f"COA filled for lot number: {lot_number}")

# Function to reset the database and last_processed_row.txt
def reset_database():
    try:
        connection = connect_to_database()
        cursor = connection.cursor()
        reset_commands = """
        SET FOREIGN_KEY_CHECKS = 0;
        DELETE FROM electrical_resistance;
        DELETE FROM icp;
        DELETE FROM solid_content;
        DELETE FROM cnt_content;
        DELETE FROM particle_size;
        DELETE FROM viscosity;
        DELETE FROM moisture;
        DELETE FROM magnetic_impurity;
        DELETE FROM lots;
        ALTER TABLE electrical_resistance AUTO_INCREMENT = 1;
        ALTER TABLE icp AUTO_INCREMENT = 1;
        ALTER TABLE solid_content AUTO_INCREMENT = 1;
        ALTER TABLE cnt_content AUTO_INCREMENT = 1;
        ALTER TABLE particle_size AUTO_INCREMENT = 1;
        ALTER TABLE viscosity AUTO_INCREMENT = 1;
        ALTER TABLE moisture AUTO_INCREMENT = 1;
        ALTER TABLE magnetic_impurity AUTO_INCREMENT = 1;
        ALTER TABLE lots AUTO_INCREMENT = 1;
        SET FOREIGN_KEY_CHECKS = 1;
        """
        for command in reset_commands.split(';'):
            if command.strip():
                cursor.execute(command)
        connection.commit()

        # Reset last_processed_row.txt to '0'
        with open(LAST_PROCESSED_ROW_FILE, "w") as file:
            file.write("0")

        messagebox.showinfo("Info", "Database and last_processed_row.txt reset successfully.")
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Database reset failed: {err}")
    finally:
        connection.close()

# GUI setup
root = tk.Tk()
root.title("QC Database GUI")
root.geometry("400x300")

# Initialize auto-update process variable
auto_update_process = None

# Buttons and their commands
auto_update_button = tk.Button(root, text="Run Auto Update Hourly", command=run_auto_update)
auto_update_button.pack(pady=10)

import_csv_button = tk.Button(root, text="Import CSV Now", command=run_import_csv)
import_csv_button.pack(pady=10)

coa_button = tk.Button(root, text="Generate COA", command=run_coa_filling)
coa_button.pack(pady=10)

# Entry for lot number
lot_number_label = tk.Label(root, text="Enter Lot Number for COA:")
lot_number_label.pack()
lot_number_entry = tk.Entry(root)
lot_number_entry.pack(pady=5)

# Database reset button
reset_button = tk.Button(root, text="Reset Database", command=reset_database)
reset_button.pack(pady=20)

root.mainloop()

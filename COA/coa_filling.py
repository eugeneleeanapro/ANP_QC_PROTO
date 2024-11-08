import mysql.connector
from openpyxl import load_workbook
import os

# Database connection details
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "mysql",
    "database": "qcdb"
}

# Paths to the template and output COA file
TEMPLATE_PATH = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/COA.xlsx"
OUTPUT_PATH = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/COA - Copy.xlsx"

# Fetch QC data from database
def fetch_qc_data(lot_number):
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=True)
    
    query = """
        SELECT
            solid_content.status AS solid_content_status,
            cnt_content.status AS cnt_content_status,
            viscosity.status AS viscosity_status,
            particle_size.status AS particle_size_status,
            moisture.status AS moisture_status,
            electrical_resistance.status AS electrical_resistance_status,
            magnetic_impurity.status AS magnetic_impurity_status,
            solid_content.solid_content AS solid_content_value,
            cnt_content.cnt_content AS cnt_content_value,
            viscosity.viscosity AS viscosity_value,
            particle_size.particle_size AS particle_size_value,
            moisture.moisture AS moisture_value,
            electrical_resistance.electrical_resistance AS electrical_resistance_value,
            magnetic_impurity.magnetic_impurity_sum AS mag_sum,
            icp.Ca, icp.Cr, icp.Cu, icp.Fe, icp.Na, icp.Zn, icp.Zr, icp.Co
        FROM
            lots
        LEFT JOIN solid_content ON lots.lot_number = solid_content.lot_number
        LEFT JOIN cnt_content ON lots.lot_number = cnt_content.lot_number
        LEFT JOIN viscosity ON lots.lot_number = viscosity.lot_number
        LEFT JOIN particle_size ON lots.lot_number = particle_size.lot_number
        LEFT JOIN moisture ON lots.lot_number = moisture.lot_number
        LEFT JOIN electrical_resistance ON lots.lot_number = electrical_resistance.lot_number
        LEFT JOIN magnetic_impurity ON lots.lot_number = magnetic_impurity.lot_number
        LEFT JOIN icp ON lots.lot_number = icp.lot_number
        WHERE lots.lot_number = %s
    """
    cursor.execute(query, (lot_number,))
    result = cursor.fetchone()
    
    cursor.close()
    connection.close()
    
    if not result:
        raise ValueError(f"No data found for lot number {lot_number}")
    
    return result

# Update COA for product 6.0J in Excel
def fill_coa_for_6_0j(lot_number):
    qc_data = fetch_qc_data(lot_number)
    
    # Load the Excel workbook and select the relevant sheet
    workbook = load_workbook(TEMPLATE_PATH)
    sheet = workbook["6.0J"]

    # Fill in COA data based on provided cell mappings
    sheet["D12"] = lot_number
    sheet["D14"] = qc_data.get("inspection_date", "N/A")
    sheet["D16"] = "Pass" if all(value == "PASS" for value in qc_data.values() if isinstance(value, str)) else "Fail"
    sheet["E20"], sheet["F20"] = qc_data["solid_content_value"], qc_data["solid_content_status"]
    sheet["E21"], sheet["F21"] = qc_data["cnt_content_value"], qc_data["cnt_content_status"]
    sheet["E22"], sheet["F22"] = qc_data["viscosity_value"], qc_data["viscosity_status"]
    sheet["E23"], sheet["F23"] = qc_data["particle_size_value"], qc_data["particle_size_status"]
    sheet["E24"], sheet["F24"] = qc_data["moisture_value"], qc_data["moisture_status"]
    sheet["E25"], sheet["F25"] = qc_data["electrical_resistance_value"], qc_data["electrical_resistance_status"]

    # ICP Elements with Pass/Fail statuses
    icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Zn", "Zr", "Co"]
    icp_cells = ["F38", "F39", "F40", "F41", "F42", "F43", "F44", "F45"]
    
    for element, cell in zip(icp_elements, icp_cells):
        sheet[cell] = qc_data[element]
        sheet[cell.replace("F", "G")] = "PASS" if qc_data[element] <= 5 else "FAIL"  # Example threshold of 5 for PASS

    # Magnetic Impurity
    sheet["F51"] = qc_data["mag_sum"]

    # Save filled out COA to a new file
    try:
        workbook.save(OUTPUT_PATH)
        print(f"COA generated and saved at {OUTPUT_PATH}")
    except PermissionError:
        print("Permission denied: Close the output file if it's open and try again.")

# Run the code
if __name__ == "__main__":
    lot_number = input("Enter the lot number: ")
    fill_coa_for_6_0j(lot_number)

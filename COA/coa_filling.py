import sys
import mysql.connector
from openpyxl import load_workbook
from datetime import datetime

# Paths for COA template and output
template_path = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/COA.xlsx"
output_path = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/COA - Copy.xlsx"

# Function to connect to the database
def connect_to_database():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='mysql',
        database='qcdb'
    )

# Main function to fill COA
def fill_coa(lot_number):
    # Connect to the database and retrieve QC data for the lot number
    connection = connect_to_database()
    cursor = connection.cursor(dictionary=True)
    
    query = """
    SELECT lots.lot_number, lots.product, lots.status AS lot_status,
        solid_content.solid_content, solid_content.status AS solid_content_status,
        cnt_content.cnt_content, cnt_content.status AS cnt_content_status,
        particle_size.particle_size, particle_size.status AS particle_size_status,
        viscosity.viscosity, viscosity.status AS viscosity_status,
        moisture.moisture, moisture.status AS moisture_status,
        electrical_resistance.electrical_resistance, electrical_resistance.status AS electrical_resistance_status,
        magnetic_impurity.magnetic_impurity_sum AS mag_sum, magnetic_impurity.status AS magnetic_impurity_status,
        icp.Ca, icp.Cr, icp.Cu, icp.Fe, icp.Na, icp.Ni, icp.Zn, icp.Zr, icp.Co, icp.status AS icp_status
    FROM lots
    LEFT JOIN solid_content ON lots.lot_number = solid_content.lot_number
    LEFT JOIN cnt_content ON lots.lot_number = cnt_content.lot_number
    LEFT JOIN particle_size ON lots.lot_number = particle_size.lot_number
    LEFT JOIN viscosity ON lots.lot_number = viscosity.lot_number
    LEFT JOIN moisture ON lots.lot_number = moisture.lot_number
    LEFT JOIN electrical_resistance ON lots.lot_number = electrical_resistance.lot_number
    LEFT JOIN magnetic_impurity ON lots.lot_number = magnetic_impurity.lot_number
    LEFT JOIN icp ON lots.lot_number = icp.lot_number
    WHERE lots.lot_number = %s
    """
    
    cursor.execute(query, (lot_number,))
    qc_data = cursor.fetchone()
    connection.close()

    if not qc_data:
        print(f"No data found for lot number: {lot_number}")
        return

    # Load the appropriate sheet from the COA template based on product
    workbook = load_workbook(template_path)
    sheet = workbook[qc_data["product"]]  # Select sheet by product name ("6.0J", "6.5J", or "5.4J")

    # Fill in general information
    sheet["D12"] = lot_number
    sheet["D15"] = datetime.now().strftime("%Y-%m-%d")  # Inspection date
    sheet["D16"] = qc_data["lot_status"] or "PASS"  # Status from lots table
    
    # Additional lot number placement based on product type
    if qc_data["product"] == "6.0J" or qc_data["product"] == "5.4J":
        sheet["E36"] = lot_number  # Lot number for 6.0J and 5.4J
    elif qc_data["product"] == "6.5J":
        sheet["E38"] = lot_number  # Lot number for 6.5J

    # Fill in specific test results and statuses
    sheet["E20"] = qc_data.get("solid_content", "N/A")
    sheet["F20"] = qc_data.get("solid_content_status", "PASS")
    
    sheet["E21"] = qc_data.get("cnt_content", "N/A")
    sheet["F21"] = qc_data.get("cnt_content_status", "PASS")
    
    sheet["E22"] = qc_data.get("viscosity", "N/A")
    sheet["F22"] = qc_data.get("viscosity_status", "PASS")
    
    sheet["E23"] = qc_data.get("particle_size", "N/A")
    sheet["F23"] = qc_data.get("particle_size_status", "PASS")
    
    if qc_data["product"] == "5.4J":
        sheet["E24"] = qc_data.get("moisture", "N/A")
        sheet["F24"] = qc_data.get("moisture_status", "PASS")
        sheet["E25"] = qc_data.get("electrical_resistance", "N/A")
        sheet["F25"] = qc_data.get("electrical_resistance_status", "PASS")

        icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Ni", "Zn", "Zr", "Co"]
        for i, element in enumerate(icp_elements, start=38):
            value = qc_data.get(element, "N/A")
            sheet[f"F{i}"] = value
            sheet[f"G{i}"] = qc_data.get("icp_status", "PASS")

    elif qc_data["product"] == "6.5J":
        sheet["E24"] = qc_data.get("electrical_resistance", "N/A")
        sheet["F24"] = qc_data.get("electrical_resistance_status", "PASS")

        # Fill ICP elements from F40 to F47
        icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Ni", "Zn", "Zr"]
        for i, element in enumerate(icp_elements, start=40):
            value = qc_data.get(element, "N/A")
            sheet[f"F{i}"] = value
            sheet[f"G{i}"] = qc_data.get("icp_status", "PASS")
        
        # Fill only the magnetic impurity sum in F48
        sheet["F48"] = qc_data.get("mag_sum", "N/A")
        sheet["G48"] = qc_data.get("magnetic_impurity_status", "PASS")

    elif qc_data["product"] == "6.0J":
        sheet["E24"] = qc_data.get("moisture", "N/A")
        sheet["F24"] = qc_data.get("moisture_status", "PASS")
        sheet["E25"] = qc_data.get("electrical_resistance", "N/A")
        sheet["F25"] = qc_data.get("electrical_resistance_status", "PASS")

        icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Ni", "Zn", "Zr"]
        for i, element in enumerate(icp_elements, start=38):
            value = qc_data.get(element, "N/A")
            sheet[f"F{i}"] = value
            sheet[f"G{i}"] = qc_data.get("icp_status", "PASS")
        
        # Magnetic impurity details for 6.0J
        magnetic_elements = ["mag_Cr", "mag_Fe", "mag_Ni", "mag_Zn"]
        for i, element in enumerate(magnetic_elements, start=47):
            sheet[f"F{i}"] = qc_data.get(element, "N/A")

        sheet["F51"] = qc_data.get("mag_sum", "N/A")
        sheet["G51"] = qc_data.get("magnetic_impurity_status", "PASS")

    # Save the updated COA file
    try:
        workbook.save(output_path)
        print(f"COA saved for lot number {lot_number} to {output_path}")
    except PermissionError as e:
        print("Error: Close the COA file and try again.")

# Main script logic to handle command-line arguments
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Lot number not provided.")
        sys.exit(1)
    
    lot_number = sys.argv[1]
    fill_coa(lot_number)

import mysql.connector
from openpyxl import load_workbook
from datetime import datetime

# Paths to COA template and output
template_path = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/COA.xlsx"
output_path = "C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/COA - Copy.xlsx"

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

# Fetch data from QC tables with handling for NULL values
def fetch_qc_data(lot_number):
    connection = connect_to_database()
    if not connection:
        print("Failed to connect to the database.")
        return None

    cursor = connection.cursor(dictionary=True)
    
    # Define the query to fetch data
    query = """
    SELECT 
        lots.lot_number, lots.product, lots.status AS lot_status,
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
    
    return qc_data

# Fill COA template based on product type
def fill_coa(lot_number):
    # Load the COA template
    workbook = load_workbook(template_path)
    
    # Fetch QC data for the specified lot number
    qc_data = fetch_qc_data(lot_number)
    if not qc_data:
        print(f"No data found for lot number: {lot_number}")
        return
    
    # Determine product type and select appropriate sheet
    product = qc_data["product"]
    sheet = None
    if product == "6.0J":
        sheet = workbook["6.0J"]
        
        # Map data for 6.0J
        sheet["D12"] = lot_number
        sheet["D14"] = datetime.now().strftime("%Y-%m-%d")  # Inspection date
        sheet["D15"] = datetime.now().strftime("%Y-%m-%d")  # Date to write COA
        sheet["D16"] = qc_data["lot_status"]  # Status from lots table
        
        # Map testing results and statuses
        sheet["E20"] = qc_data["solid_content"]
        sheet["F20"] = qc_data["solid_content_status"] or "PASS"
        sheet["E21"] = qc_data["cnt_content"]
        sheet["F21"] = qc_data["cnt_content_status"] or "PASS"
        sheet["E22"] = qc_data["viscosity"]
        sheet["F22"] = qc_data["viscosity_status"] or "PASS"
        sheet["E23"] = qc_data["particle_size"]
        sheet["F23"] = qc_data["particle_size_status"] or "PASS"
        sheet["E24"] = qc_data["moisture"]
        sheet["F24"] = qc_data["moisture_status"] or "PASS"
        sheet["E25"] = qc_data["electrical_resistance"]
        sheet["F25"] = qc_data["electrical_resistance_status"] or "PASS"
        
        # Map ICP elements
        icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Ni", "Zn", "Zr", "Co"]
        icp_status = qc_data["icp_status"]
        for i, element in enumerate(icp_elements, start=38):
            value = qc_data[element] if qc_data[element] is not None else "N/A"
            sheet[f"F{i}"] = value
            sheet[f"G{i}"] = icp_status  # Apply icp_status directly for each ICP element

        # Magnetic impurity results and statuses
        magnetic_elements = ["mag_Cr", "mag_Fe", "mag_Ni", "mag_Zn"]
        for i, element in enumerate(magnetic_elements, start=47):
            value = qc_data[element] if qc_data[element] is not None else "N/A"
            sheet[f"F{i}"] = value

        sheet["F51"] = qc_data["mag_sum"]
        sheet["G51"] = qc_data["magnetic_impurity_status"] or "PASS"

    elif product == "6.5J":
        sheet = workbook["6.5J"]
        
        # Map data for 6.5J
        sheet["D12"] = lot_number
        sheet["D14"] = datetime.now().strftime("%Y-%m-%d")  # Inspection date
        sheet["D16"] = qc_data["lot_status"]
        
        # Map testing results and statuses
        sheet["E20"] = qc_data["solid_content"]
        sheet["F20"] = qc_data["solid_content_status"] or "PASS"
        sheet["E21"] = qc_data["cnt_content"]
        sheet["F21"] = qc_data["cnt_content_status"] or "PASS"
        sheet["E22"] = qc_data["viscosity"]
        sheet["F22"] = qc_data["viscosity_status"] or "PASS"
        sheet["E23"] = qc_data["particle_size"]
        sheet["F23"] = qc_data["particle_size_status"] or "PASS"
        sheet["E24"] = qc_data["electrical_resistance"]
        sheet["F24"] = qc_data["electrical_resistance_status"] or "PASS"

        # Map ICP elements
        icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Ni", "Zn", "Zr"]
        for i, element in enumerate(icp_elements, start=40):
            value = qc_data[element] if qc_data[element] is not None else "N/A"
            sheet[f"F{i}"] = value
            sheet[f"G{i}"] = qc_data["icp_status"]

        sheet["F48"] = qc_data["mag_sum"]
        sheet["G48"] = qc_data["magnetic_impurity_status"] or "PASS"

    elif product == "5.4J":
        sheet = workbook["5.4J"]
        
        # Map data for 5.4J
        sheet["D12"] = lot_number
        sheet["D15"] = datetime.now().strftime("%Y-%m-%d")  # Inspection date
        sheet["D16"] = qc_data["lot_status"]

        # Map testing results and statuses
        sheet["E20"] = qc_data["solid_content"]
        sheet["F20"] = qc_data["solid_content_status"] or "PASS"
        sheet["E21"] = qc_data["cnt_content"]
        sheet["F21"] = qc_data["cnt_content_status"] or "PASS"
        sheet["E22"] = qc_data["viscosity"]
        sheet["F22"] = qc_data["viscosity_status"] or "PASS"
        sheet["E23"] = qc_data["particle_size"]
        sheet["F23"] = qc_data["particle_size_status"] or "PASS"
        sheet["E24"] = qc_data["moisture"]
        sheet["F24"] = qc_data["moisture_status"] or "PASS"
        sheet["E25"] = qc_data["electrical_resistance"]
        sheet["F25"] = qc_data["electrical_resistance_status"] or "PASS"

        # Map ICP elements
        icp_elements = ["Ca", "Cr", "Cu", "Fe", "Na", "Ni", "Zn", "Zr", "Co"]
        for i, element in enumerate(icp_elements, start=38):
            value = qc_data[element] if qc_data[element] is not None else "N/A"
            sheet[f"F{i}"] = value
            sheet[f"G{i}"] = qc_data["icp_status"]

    # Save the updated COA to output
    workbook.save(output_path)
    print(f"COA saved to {output_path}")

# Main function to run the filling process
if __name__ == "__main__":
    lot_number = input("Enter the lot number: ")
    fill_coa(lot_number)

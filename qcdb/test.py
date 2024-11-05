import csv
import requests


# Path to your CSV file
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Function to read CSV and return as a list of lists
def csv_to_list_of_lists(csv_file_path):
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        data = [row for row in reader]
    return data

# Call the function and store the result
csv_data = csv_to_list_of_lists(csv_file_path)

# Print the result to verify
# for row in csv_data:
#     print(row)

def update_csv_to_fishbowl():
    # Call the function and store the result
    csv_data = csv_to_list_of_lists(csv_file_path)
    # URL for the API endpoint
    url = "http://localhost:80/api/import/:name"

    # Payload for the body
    payload = {
        csv_data
    }

    # Sending the POST request with the JSON payload
    response = requests.post(url, data=json.dumps(payload))

    # Handling the response
    # if response.ok:
    #     print("Request successful!")
    # #     print("Response:", response.json())
    # else:
    #     print("Request failed!")
    #     print("Status Code:", response.status_code)
    #     print("Response Text:", response.text)
    # {URL}}/api/import/:name

# Path to your CSV file
csv_file_path = 'C:/Users/EugeneLee/OneDrive - ANP ENERTECH INC/Desktop/QC_CSV.csv'

# Function to read CSV and return as a list of lists
def csv_to_list_of_lists(csv_file_path):
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        data = [row for row in reader]
    return data

# Call the function and store the result
csv_data = csv_to_list_of_lists(csv_file_path)

#app key, app name, header of api, coa test result path, app route, fishbowl url - cloud and localhost 필요합니당
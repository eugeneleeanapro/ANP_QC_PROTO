from flask import Flask, jsonify, request
import requests
import json
from db_to_fishbowl import DBToFishbowl

app = Flask(__name__)

# Instantiate the DBToFishbowl class
db_fishbowl = DBToFishbowl()

@app.route('/update-coa', methods=['POST'])
def update_coa():
    if db_fishbowl.session_id:
        db_fishbowl.update_coa_in_fishbowl()
        return jsonify({"status": "Success", "message": "COA updated in Fishbowl"}), 200
    else:
        return jsonify({"status": "Failed", "message": "Fishbowl login failed"}), 400

# Endpoint to fetch QC data
@app.route('/fetch-qc-data', methods=['GET'])
def fetch_qc_data():
    qc_data = db_fishbowl.fetch_qc_data()
    return jsonify(qc_data), 200

# Start the Flask API
if __name__ == '__main__':
    app.run(port=5000, debug=True)

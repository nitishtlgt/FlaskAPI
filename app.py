from flask import Flask, request, jsonify
from create_vector_db import CreateCollection  
from pymongo import MongoClient
import pandas as pd
import json

app = Flask(__name__)

collection_manager = CreateCollection('real_estate_test')

@app.route('/get_query', methods=['POST'])
def get_query():
    data = request.get_json()
    # data = {"query":"I want furnished house with 1 bed and 1 bath in sarjah area around 40,000AED"}

    if 'query' not in data:
        return jsonify({'error': 'Query parameter is missing'}), 400

    user_query = data['query']

    db_collection = collection_manager.create_collection()
    query_result = collection_manager.run_query(db_collection, user_query)
    print(query_result,"query_result")
    print(json.dumps(query_result))

    return json.dumps(query_result)


@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        csv_file_path = request.json.get('csv_file_path')

        if not csv_file_path:
            return jsonify({"error": "CSV file path not provided"}), 400

        client = MongoClient("mongodb://localhost:27017")
        db = client["RealEstate"]
        collection = db["CSVdata"]

        # csv_file_path = "real_estate_uae_cleaned_file.csv"
        df = pd.read_csv(csv_file_path)

        for index, row in df.iterrows():
            data = row.to_dict()
            collection.insert_one(data)

        client.close()

        return jsonify({"message": "Data inserted successfully"})
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(host = "0.0.0.0", debug=True, port=5000)
from kafka import KafkaProducer
from pymongo import MongoClient
from jsonschema import validate, ValidationError
import json
import datetime
import time
import pandas as pd

# === MongoDB setup ===
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["streaming_db"]
collection = db["sales_header"]

# === Kafka setup ===
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === JSON Schema (sales_header) ===
sales_header_schema = {
    "type": "object",
    "properties": {
        "doc_number": {"type": "string"},
        "trans_type_code": {"type": "string"},
        "customer_code": {"type": "string"},
        "rep_code": {"type": "string"},
        "trans_date": {"type": "string", "format": "date-time"},
        "fin_period": {"type": "string"}
    },
    "required": ["doc_number", "trans_type_code", "customer_code", "rep_code", "trans_date", "fin_period"]
}

# === Function: Export MongoDB data to CSV for Power BI ===
def export_to_csv():
    try:
        sales_header_data = list(collection.find())
        if not sales_header_data:
            print("⚠️ No data found in MongoDB collection.")
            return
        for doc in sales_header_data:
            doc.pop("_id", None)
        df = pd.DataFrame(sales_header_data)
        df.to_csv("C:\\Users\\tshir\\OneDrive - NORTH-WEST UNIVERSITY\\Desktop\\streaming_sales_header.csv", index=False)
        print("✅ Exported MongoDB collection to CSV for Power BI.")
    except Exception as e:
        print(f"❌ Error exporting MongoDB to CSV: {e}")

# === Streaming Loop ===
print("🚀 Starting producer: sending data every 5s, exporting to CSV every 15s...")

last_export_time = time.time()

while True:
    data = {
        "doc_number": f"INV{int(time.time())}",
        "trans_type_code": "SALE",
        "customer_code": "CUST100",
        "rep_code": "REP01",
        "trans_date": datetime.datetime.utcnow().isoformat() + "Z",
        "fin_period": "2025-10"
    }

    try:
        validate(instance=data, schema=sales_header_schema)
        print("✅ Valid data, sending to Kafka:", data)
        producer.send('sales_header', data)
        collection.insert_one(data)  # store in MongoDB
    except ValidationError as e:
        print("❌ Schema validation failed:", e.message)

    # Export every 15 seconds
    if time.time() - last_export_time >= 15:
        export_to_csv()
        last_export_time = time.time()

    time.sleep(5)  # wait before next record

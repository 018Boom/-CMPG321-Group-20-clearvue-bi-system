# -CMPG321-Group-20-clearvue-bi-system
Exporting data to Power BI
# Streaming Data Bridge to Power BI

This script acts as a bridge between Kafka, MongoDB, and Power BI.
It continuously reads simulated streaming data, stores it in MongoDB, 
and exports the data to a CSV file that Power BI can connect to.

## Setup
1. Run Zookeeper and Kafka locally.
2. Ensure MongoDB is running on `localhost:27017`.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   

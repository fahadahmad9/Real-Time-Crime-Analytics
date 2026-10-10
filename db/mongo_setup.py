from pymongo import MongoClient, ASCENDING
import yaml
import sys

def setup_mongodb():
    with open('/app/config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    mongo_cfg = config['mongodb']
    client = MongoClient(mongo_cfg['host'], mongo_cfg['port'])
    db = client[mongo_cfg['database']]

    # --- Alert Logs Collection ---
    if 'alert_logs' not in db.list_collection_names():
        db.create_collection('alert_logs')
        print("Created collection: alert_logs")

    db['alert_logs'].create_index([('district', ASCENDING)])
    db['alert_logs'].create_index([('timestamp', ASCENDING)])
    print("Indexes created on alert_logs")

    # --- Raw Event Logs Collection ---
    if 'event_logs' not in db.list_collection_names():
        db.create_collection('event_logs')
        print("Created collection: event_logs")

    db['event_logs'].create_index([('district', ASCENDING)])
    db['event_logs'].create_index([('date', ASCENDING)])
    db['event_logs'].create_index([('primary_type', ASCENDING)])
    print("Indexes created on event_logs")

    print("MongoDB setup complete.")
    client.close()

if __name__ == '__main__':
    setup_mongodb()

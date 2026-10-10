import sys
sys.path.insert(0, '/app')

import json
import time
import yaml
import logging
import psycopg2
from pymongo import MongoClient
from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    with open('config/config.yaml', 'r') as f:
        return yaml.safe_load(f)

def main():
    config = load_config()

    # Kafka consumer
    consumer = KafkaConsumer(
        config['kafka']['topic'],
        bootstrap_servers=['localhost:9093'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='storm-simple'
    )

    # PostgreSQL
    pg = config['postgresql']
    pg_conn = psycopg2.connect(
        host='localhost', port=pg['port'],
        dbname=pg['database'], user=pg['user'],
        password=pg['password']
    )
    pg_cursor = pg_conn.cursor()

    # MongoDB
    mongo = config['mongodb']
    mongo_client = MongoClient('localhost', mongo['port'])
    mongo_db = mongo_client[mongo['database']]
    alert_col = mongo_db[mongo['alerts_collection']]

    # Sliding window config
    window_size = config['storm']['window_size']
    slide_interval = config['storm']['slide_interval']
    threshold = config['storm']['anomaly_threshold']

    district_events = defaultdict(deque)
    last_slide = time.time()
    total_received = 0

    logger.info(f"Storm simulator started. Window={window_size}s Threshold={threshold}")

    for msg in consumer:
        record = msg.value
        district = record.get('district', 'UNKNOWN').strip()
        now = time.time()

        # Add event to window
        district_events[district].append(now)

        # Remove old events outside window
        cutoff = now - window_size
        while district_events[district] and district_events[district][0] < cutoff:
            district_events[district].popleft()

        total_received += 1
        if total_received % 50 == 0:
            logger.info(f"Processed {total_received} events")

        # Check at slide interval
        if now - last_slide >= slide_interval:
            last_slide = now
            for dist, timestamps in district_events.items():
                count = len(timestamps)
                if count > threshold:
                    if count > threshold * 2:
                        severity = "CRITICAL"
                    elif count > threshold * 1.5:
                        severity = "HIGH"
                    else:
                        severity = "MEDIUM"

                    ts = datetime.now()
                    logger.warning(f"ALERT [{severity}] District {dist}: {count} events")

                    # Write to PostgreSQL
                    pg_cursor.execute("""
                        INSERT INTO alerts
                            (district, event_count, threshold, severity, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (dist, count, threshold, severity, ts))
                    pg_conn.commit()

                    # Write to MongoDB
                    alert_col.insert_one({
                        "district": dist,
                        "event_count": count,
                        "threshold": threshold,
                        "severity": severity,
                        "timestamp": ts.isoformat(),
                        "created_at": datetime.utcnow().isoformat()
                    })

if __name__ == '__main__':
    main()

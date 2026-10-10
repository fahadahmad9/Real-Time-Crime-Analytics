import sys
sys.path.insert(0, '/app')

import logging
import yaml
import psycopg2
from pymongo import MongoClient
from datetime import datetime
from streamparse.bolt import Bolt

logger = logging.getLogger(__name__)

class AlertBolt(Bolt):
    outputs = []

    def initialize(self, stormconf, context):
        with open('/app/config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)

        # PostgreSQL connection
        pg = config['postgresql']
        self.pg_conn = psycopg2.connect(
            host=pg['host'], port=pg['port'],
            dbname=pg['database'], user=pg['user'],
            password=pg['password']
        )
        self.pg_cursor = self.pg_conn.cursor()
        logger.info("AlertBolt: PostgreSQL connected.")

        # MongoDB connection
        mongo = config['mongodb']
        self.mongo_client = MongoClient(mongo['host'], mongo['port'])
        self.mongo_db = self.mongo_client[mongo['database']]
        self.alert_collection = self.mongo_db[mongo['alerts_collection']]
        logger.info("AlertBolt: MongoDB connected.")

    def process(self, tup):
        try:
            district    = tup.values[0]
            event_count = tup.values[1]
            threshold   = tup.values[2]
            severity    = tup.values[3]
            timestamp   = tup.values[4]

            ts = datetime.fromtimestamp(timestamp)

            # Write to PostgreSQL
            self.pg_cursor.execute("""
                INSERT INTO alerts
                    (district, event_count, threshold, severity, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (district, event_count, threshold, severity, ts))
            self.pg_conn.commit()

            # Write to MongoDB
            alert_doc = {
                "district":    district,
                "event_count": event_count,
                "threshold":   threshold,
                "severity":    severity,
                "timestamp":   ts.isoformat(),
                "created_at":  datetime.utcnow().isoformat()
            }
            self.alert_collection.insert_one(alert_doc)

            logger.info(
                f"Alert saved [{severity}] District {district} "
                f"count={event_count} at {ts}"
            )

        except Exception as e:
            logger.error(f"AlertBolt error: {e}")
            try:
                self.pg_conn.rollback()
            except:
                pass

    def cleanup(self):
        try:
            self.pg_cursor.close()
            self.pg_conn.close()
            self.mongo_client.close()
        except:
            pass

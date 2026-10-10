import sys
sys.path.insert(0, '/app')

import csv
import json
import time
import yaml
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    with open('config/config.yaml', 'r') as f:
        return yaml.safe_load(f)

def create_producer(broker):
    logger.info(f"Connecting to Kafka broker: {broker}")
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        acks='all'
    )
    logger.info("Kafka producer connected.")
    return producer

def parse_row(row):
    """Extract required fields from a CSV row dict."""
    return {
        "case_number":   row.get("Case Number", ""),
        "date":          row.get("Date", ""),
        "block":         row.get("Block", ""),
        "primary_type":  row.get("Primary Type", ""),
        "district":      row.get("District", ""),
        "arrest":        row.get("Arrest", ""),
        "latitude":      row.get("Latitude", ""),
        "longitude":     row.get("Longitude", ""),
        "iucr":          row.get("IUCR", ""),
        "description":   row.get("Description", ""),
        "community_area":row.get("Community Area", ""),
        "year":          row.get("Year", ""),
    }

def is_valid(record):
    """Discard rows missing critical fields."""
    required = ["case_number", "date", "primary_type", "district"]
    for field in required:
        if not record.get(field):
            return False
    return True

def stream_crimes(producer, topic, csv_path, rate):
    logger.info(f"Streaming from: {csv_path}")
    logger.info(f"Topic: {topic} | Rate: {rate} row/sec")

    sent = 0
    skipped = 0

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    record = parse_row(row)

                    if not is_valid(record):
                        skipped += 1
                        logger.warning(f"Skipping malformed row: {record}")
                        continue

                    producer.send(topic, value=record)
                    sent += 1

                    if sent % 100 == 0:
                        logger.info(f"Sent: {sent} | Skipped: {skipped}")

                    time.sleep(1.0 / rate)

                except Exception as e:
                    skipped += 1
                    logger.error(f"Error processing row: {e}")
                    continue

    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error reading CSV: {e}")
        sys.exit(1)
    finally:
        producer.flush()
        logger.info(f"Streaming complete. Sent: {sent} | Skipped: {skipped}")

if __name__ == '__main__':
    config = load_config()

    broker  = config['kafka']['broker']
    topic   = config['kafka']['topic']
    rate    = config['kafka']['producer_rate']
    csv_path = config['data']['crime']

    producer = create_producer(broker)

    try:
        stream_crimes(producer, topic, csv_path, rate)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.close()
        logger.info("Producer closed.")

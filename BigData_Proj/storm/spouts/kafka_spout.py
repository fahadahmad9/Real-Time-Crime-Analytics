import sys
sys.path.insert(0, '/app')

import json
import logging
from streamparse.spout import Spout
from kafka import KafkaConsumer
import yaml

logger = logging.getLogger(__name__)

class CrimeKafkaSpout(Spout):
    outputs = ['crime_record']

    def initialize(self, stormconf, context):
        with open('/app/config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)

        self.consumer = KafkaConsumer(
            config['kafka']['topic'],
            bootstrap_servers=[config['kafka']['broker']],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='storm-crime-group'
        )
        self.messages = iter(self.consumer)
        logger.info("CrimeKafkaSpout initialized.")

    def next_tuple(self):
        try:
            msg = next(self.messages)
            record = msg.value
            self.emit([json.dumps(record)], id=msg.offset)
        except StopIteration:
            import time
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"Spout error: {e}")

    def ack(self, tup_id):
        pass

    def fail(self, tup_id):
        logger.warning(f"Failed tuple id: {tup_id}")

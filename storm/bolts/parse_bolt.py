import sys
sys.path.insert(0, '/app')

import json
import logging
from streamparse.bolt import Bolt

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ["case_number", "date", "primary_type", "district"]

class ParseBolt(Bolt):
    outputs = ['parsed_record']

    def process(self, tup):
        try:
            raw = tup.values[0]
            record = json.loads(raw)

            for field in REQUIRED_FIELDS:
                if not record.get(field):
                    logger.warning(f"Malformed record missing '{field}': {record}")
                    return

            self.emit([json.dumps(record)])

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e} | raw: {tup.values[0]}")
        except Exception as e:
            logger.error(f"ParseBolt error: {e}")

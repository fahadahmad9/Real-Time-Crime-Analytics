import sys
sys.path.insert(0, '/app')

import json
import logging
from streamparse.bolt import Bolt

logger = logging.getLogger(__name__)

class DistrictBolt(Bolt):
    outputs = ['district', 'record_json']

    def process(self, tup):
        try:
            record = json.loads(tup.values[0])
            district = record.get("district", "UNKNOWN").strip()

            if not district:
                district = "UNKNOWN"

            self.emit([district, json.dumps(record)])

        except Exception as e:
            logger.error(f"DistrictBolt error: {e}")

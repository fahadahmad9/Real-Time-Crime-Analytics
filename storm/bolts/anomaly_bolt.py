import sys
sys.path.insert(0, '/app')

import logging
import yaml
from streamparse.bolt import Bolt

logger = logging.getLogger(__name__)

class AnomalyBolt(Bolt):
    outputs = ['district', 'event_count', 'threshold', 'severity', 'timestamp']

    def initialize(self, stormconf, context):
        with open('/app/config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        self.threshold = config['storm']['anomaly_threshold']
        logger.info(f"AnomalyBolt: threshold={self.threshold}")

    def process(self, tup):
        try:
            district     = tup.values[0]
            event_count  = tup.values[1]
            window_start = tup.values[2]
            window_end   = tup.values[3]

            if event_count > self.threshold:
                if event_count > self.threshold * 2:
                    severity = "CRITICAL"
                elif event_count > self.threshold * 1.5:
                    severity = "HIGH"
                else:
                    severity = "MEDIUM"

                logger.warning(
                    f"ANOMALY [{severity}] District {district}: "
                    f"{event_count} events (threshold={self.threshold})"
                )
                self.emit([district, event_count, self.threshold,
                           severity, window_end])

        except Exception as e:
            logger.error(f"AnomalyBolt error: {e}")

import sys
sys.path.insert(0, '/app')

import json
import time
import logging
import yaml
from collections import defaultdict, deque
from streamparse.bolt import Bolt

logger = logging.getLogger(__name__)

class WindowBolt(Bolt):
    outputs = ['district', 'event_count', 'window_start', 'window_end']

    def initialize(self, stormconf, context):
        with open('/app/config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)

        self.window_size   = config['storm']['window_size']
        self.slide_interval = config['storm']['slide_interval']
        self.events = defaultdict(deque)
        self.last_slide = time.time()
        logger.info(f"WindowBolt: window={self.window_size}s slide={self.slide_interval}s")

    def process(self, tup):
        try:
            district = tup.values[0]
            now = time.time()
            self.events[district].append(now)

            # Remove events outside the window
            cutoff = now - self.window_size
            while self.events[district] and self.events[district][0] < cutoff:
                self.events[district].popleft()

            # Emit counts at each slide interval
            if now - self.last_slide >= self.slide_interval:
                self.last_slide = now
                window_start = now - self.window_size
                for dist, timestamps in self.events.items():
                    count = len(timestamps)
                    self.emit([dist, count, window_start, now])
                    logger.debug(f"District {dist}: {count} events in window")

        except Exception as e:
            logger.error(f"WindowBolt error: {e}")

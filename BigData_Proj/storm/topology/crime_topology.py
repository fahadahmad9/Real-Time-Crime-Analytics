import sys
sys.path.insert(0, '/app')

from streamparse import Topology
from storm.spouts.kafka_spout import CrimeKafkaSpout
from storm.bolts.parse_bolt import ParseBolt
from storm.bolts.district_bolt import DistrictBolt
from storm.bolts.window_bolt import WindowBolt
from storm.bolts.anomaly_bolt import AnomalyBolt
from storm.bolts.alert_bolt import AlertBolt

class CrimeTopology(Topology):
    kafka_spout = CrimeKafkaSpout.spec(par=1)

    parse_bolt = ParseBolt.spec(
        inputs=[kafka_spout],
        par=2
    )

    district_bolt = DistrictBolt.spec(
        inputs=[parse_bolt],
        par=2
    )

    window_bolt = WindowBolt.spec(
        inputs={district_bolt: ['district', 'record_json']},
        par=2
    )

    anomaly_bolt = AnomalyBolt.spec(
        inputs=[window_bolt],
        par=1
    )

    alert_bolt = AlertBolt.spec(
        inputs=[anomaly_bolt],
        par=1
    )

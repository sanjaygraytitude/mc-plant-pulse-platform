"""Broker — stands in for MQTT broker + Kafka. Subscribes edge publishers
(stdin NDJSON), partitions messages by topic key (asset id), retains a
configurable log (Kafka-style replay), and fans out to consumer groups
(stream processor, historian) via Unix pipes."""
import json, sys, time, argparse, collections

class Broker:
    def __init__(self, retention=10000):
        self.log = collections.deque(maxlen=retention)   # Kafka-style retained log
        self.consumers = []                              # consumer groups
        self.counts = collections.Counter()

    def publish(self, msg):
        self.log.append(msg)
        self.counts[msg["topic"]] += 1
        for c in self.consumers:
            c(msg)

    def replay(self, topic_prefix=""):
        return [m for m in self.log if m["topic"].startswith(topic_prefix)]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retention", type=int, default=10000)
    args = ap.parse_args()
    b = Broker(args.retention)
    for line in sys.stdin:                      # edge publishers write here
        try:
            b.publish(json.loads(line))
        except json.JSONDecodeError:
            continue
    print(json.dumps({"published": sum(b.counts.values()),
                      "topics": dict(b.counts)}), file=sys.stderr)

if __name__ == "__main__":
    main()

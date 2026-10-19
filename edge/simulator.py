"""Edge gateway simulator — stands in for an Ignition Edge / Kepware gateway
polling PLC tags at a real plant. Publishes telemetry messages to the broker
as newline-delimited JSON on stdout (the platform's MQTT-style transport)."""
import json, time, random, argparse, sys, os, math

HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS = json.load(open(os.path.join(HERE, "..", "config", "assets.json")))["assets"]
T0 = time.time()

def read_tag(a, t, fault=None):
    """Simulate one PLC tag read: target + noise + slow drift, plus a fault ramp."""
    noise = (a["max"] - a["min"]) * 0.02
    v = a["target"] + random.gauss(0, noise) + 0.4 * math.sin(t / 7.0)
    if fault == a["id"]:
        elapsed = t - T0
        v += (a["crit"] - a["target"]) * min(1.4, elapsed / 12.0)  # ramp well past crit in ~12s
    return round(v, 2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plant", default="plantA")
    ap.add_argument("--rate", type=float, default=1.0, help="seconds between tag scans")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--fault", default=None, help="asset id to drive into CRIT")
    args = ap.parse_args()
    end = time.time() + args.duration
    while time.time() < end:
        t = time.time()
        for a in ASSETS:
            msg = {"topic": f"telemetry.{args.plant}.{a['id']}",
                   "payload": {"asset": a["id"], "ts": int(t * 1000),
                               "value": read_tag(a, t, args.fault),
                               "unit": a["unit"], "plant": args.plant}}
            print(json.dumps(msg), flush=True)
        time.sleep(args.rate)

if __name__ == "__main__":
    main()

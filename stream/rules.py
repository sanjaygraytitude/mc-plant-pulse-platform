"""Stream processor — stands in for Kafka Streams. Evaluates threshold rules
with hold-time (value must breach for N seconds) and emits events; CRIT events
open maintenance tickets (the AMC workflow)."""
import json, time, os

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open(os.path.join(HERE, "..", "config", "assets.json")))

class RuleEngine:
    def __init__(self, emit):
        self.emit = emit
        self.rules = CFG["rules"]
        self.assets = {a["id"]: a for a in CFG["assets"]}
        self.breach_start = {}   # (rule, asset) -> first-breach ts

    def process(self, msg):
        p = msg["payload"]
        a = self.assets.get(p["asset"])
        if not a:
            return
        events = []
        for r in self.rules:
            if r["asset"] not in ("*", p["asset"]):
                continue
            breached = p["value"] > (a["crit"] if r["severity"] == "CRIT" else a["warn"])
            key = (r["id"], p["asset"])
            if breached:
                start = self.breach_start.setdefault(key, p["ts"])
                held = (p["ts"] - start) / 1000.0
                if held >= r["hold_s"]:
                    events.append({"type": "breach", "rule": r["id"], "asset": p["asset"],
                                   "severity": r["severity"], "value": p["value"],
                                   "held_s": round(held, 1), "ts": p["ts"],
                                   "action": r["action"]})
                    self.breach_start.pop(key, None)   # fire once per episode
            else:
                self.breach_start.pop(key, None)
        for e in events:
            self.emit(e)

def make_ticket(e):
    """CRIT breaches become maintenance tickets — the AMC value prop."""
    return {"ticket": f"AMC-{e['ts']}", "asset": e["asset"], "severity": e["severity"],
            "detail": f"{e['rule']} breach held {e['held_s']}s at {e['value']}",
            "opened": e["ts"], "status": "open"}

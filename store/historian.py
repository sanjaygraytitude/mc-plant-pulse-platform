"""Historian — stands in for InfluxDB/TimescaleDB. Ring-buffer time-series
store per asset, with query API for the dashboard (latest, window, stats)."""
import collections, time

class Historian:
    def __init__(self, window=600):
        self.series = collections.defaultdict(lambda: collections.deque(maxlen=window))
        self.last = {}

    def write(self, msg):
        p = msg["payload"]
        self.series[p["asset"]].append((p["ts"], p["value"]))
        self.last[p["asset"]] = p

    def latest(self, asset):
        return self.last.get(asset)

    def window(self, asset, n=40):
        pts = list(self.series[asset])[-n:]
        return [{"ts": ts, "value": v} for ts, v in pts]

    def stats(self, asset):
        vals = [v for _, v in self.series[asset]]
        if not vals:
            return None
        return {"min": min(vals), "max": max(vals), "mean": round(sum(vals)/len(vals), 2), "n": len(vals)}

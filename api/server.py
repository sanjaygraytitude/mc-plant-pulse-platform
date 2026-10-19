"""API server — serves the dashboard: REST endpoints over the historian,
events, tickets, plus a WebSocket-style push channel. Stands in for the
platform's query layer (Grafana/API service)."""
import json, os, sys, time, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from store.historian import Historian
from stream.rules import RuleEngine, make_ticket
from broker.broker import Broker

class Platform:
    """Wires broker -> stream -> historian, holds events + tickets."""
    def __init__(self):
        self.historian = Historian()
        self.events = []
        self.tickets = []
        self.broker = Broker()
        self.engine = RuleEngine(self.on_event)
        self.broker.consumers.append(self.historian.write)
        self.broker.consumers.append(self.engine.process)

    def on_event(self, e):
        self.events.append(e)
        if e["action"] == "ticket":
            self.tickets.append(make_ticket(e))

    def ingest(self, line):
        try:
            self.broker.publish(json.loads(line))
        except Exception:
            pass

P = Platform()

class Handler(BaseHTTPRequestHandler):
    def _send(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/health":
            return self._send({"ok": True, "uptime_s": round(time.time()-START, 1)})
        if self.path == "/api/assets":
            return self._send([{"id": a["id"], "name": a["name"], "unit": a["unit"],
                                "min": a["min"], "max": a["max"], "target": a["target"],
                                "warn": a["warn"], "crit": a["crit"]}
                               for a in CFG["assets"]])
        if self.path.startswith("/api/latest/"):
            asset = self.path.split("/")[-1]
            return self._send(P.historian.latest(asset) or {})
        if self.path.startswith("/api/window/"):
            asset = self.path.split("/")[-1]
            return self._send(P.historian.window(asset))
        if self.path == "/api/events":
            return self._send(P.events[-50:])
        if self.path == "/api/tickets":
            return self._send(P.tickets)
        return self._send({"error": "not found"}, 404)

    def do_POST(self):
        if self.path == "/api/ingest":
            n = int(self.headers.get("Content-Length", 0))
            P.ingest(self.rfile.read(n).decode())
            return self._send({"ok": True})
        return self._send({"error": "not found"}, 404)

    def log_message(self, *a):
        pass

CFG = json.load(open(os.path.join(HERE, "..", "config", "assets.json")))
START = time.time()

def run(port=8080):
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()

if __name__ == "__main__":
    run(int(sys.argv[1]) if len(sys.argv) > 1 else 8080)

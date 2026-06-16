import sys
import threading
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

LB_PORT = 3000
HEALTH_CHECK_INTERVAL = 5
HEALTH_CHECK_TIMEOUT = 2


class BackendPool:
    def __init__(self, backends):
        self.backends = [b.rstrip("/") for b in backends]
        self.healthy = {b: True for b in self.backends}
        self.index = 0
        self.lock = threading.Lock()

    def next_backend(self):
        with self.lock:
            alive = [b for b in self.backends if self.healthy[b]]
            if not alive:
                return None
            backend = alive[self.index % len(alive)]
            self.index = (self.index + 1) % len(alive)
            return backend

    def mark(self, backend, is_healthy):
        with self.lock:
            was_healthy = self.healthy[backend]
            self.healthy[backend] = is_healthy
        if was_healthy and not is_healthy:
            print(f"[DOWN] {backend}")
        elif not was_healthy and is_healthy:
            print(f"[UP]   {backend}")

    def run_health_checks(self):
        import time
        while True:
            for backend in self.backends:
                try:
                    req = urllib.request.Request(backend + "/", method="GET")
                    urllib.request.urlopen(req, timeout=HEALTH_CHECK_TIMEOUT)
                    self.mark(backend, True)
                except Exception:
                    self.mark(backend, False)
            time.sleep(HEALTH_CHECK_INTERVAL)


class RedirectHandler(BaseHTTPRequestHandler):
    pool = None

    def log_message(self, format, *args):
        pass

    def _redirect(self):
        backend = self.pool.next_backend()
        if backend is None:
            self.send_response(503)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Service Unavailable: all backends are down\n")
            print(f"[503]  {self.command} {self.path} -> no healthy backends")
            return

        location = backend + self.path
        self.send_response(307)
        self.send_header("Location", location)
        self.end_headers()
        print(f"[307]  {self.command} {self.path} -> {location}")

    def do_GET(self):
        self._redirect()

    def do_POST(self):
        self._redirect()

    def do_PUT(self):
        self._redirect()

    def do_DELETE(self):
        self._redirect()


def main():
    if len(sys.argv) < 2:
        print("Usage: python load_balancer.py <backend_url> [backend_url ...]")
        print("Example: python load_balancer.py http://localhost:8080 http://localhost:8081")
        sys.exit(1)

    backends = sys.argv[1:]
    pool = BackendPool(backends)
    RedirectHandler.pool = pool

    t = threading.Thread(target=pool.run_health_checks, daemon=True)
    t.start()

    print(f"Load balancer listening on port {LB_PORT}")
    print(f"Backends: {', '.join(pool.backends)}")
    print("-" * 60)

    server = HTTPServer(("", LB_PORT), RedirectHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
        server.server_close()


if __name__ == "__main__":
    main()

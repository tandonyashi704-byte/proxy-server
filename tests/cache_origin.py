from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class CacheOriginHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"Hello from the origin server!\n"

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Cache-Control", "public, max-age=30")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

        print(f"[ORIGIN] Served {self.path}")

    def log_message(self, format_string, *args):
        super().log_message(format_string, *args)


server = ThreadingHTTPServer(
    ("127.0.0.1", 9001),
    CacheOriginHandler,
)

print("Cache test origin listening on 127.0.0.1:9001")
server.serve_forever()
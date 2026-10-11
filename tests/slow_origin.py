
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import time


class SlowHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print(f"Received request: {self.path}")

        time.sleep(2)

        body = b"Response from slow origin\n"

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    server = ThreadingHTTPServer(
        ("127.0.0.1", 9001),
        SlowHandler,
    )

    print("Slow origin listening on http://127.0.0.1:9001")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

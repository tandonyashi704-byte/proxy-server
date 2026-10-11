from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class TestOriginHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = (
            "Hello from the test origin!\n"
            f"Path requested: {self.path}\n"
        ).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "public, max-age=30")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string, *args):
        print("[ORIGIN]", format_string % args)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 9001), TestOriginHandler)
    print("Test origin listening on http://127.0.0.1:9001")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping test origin...")
    finally:
        server.server_close()

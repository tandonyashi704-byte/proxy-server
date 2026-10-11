from __future__ import annotations

import socket
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit
from email.message import Message



@dataclass
class CachedResponse:
    status: int
    reason: str
    headers: list[tuple[str, str]]
    body: bytes
    expires_at: float


CacheKey = tuple[str, int, str]

response_cache: OrderedDict[CacheKey, CachedResponse] = OrderedDict()
cache_lock = threading.Lock()

MAX_CACHE_ENTRIES = 100
MAX_CACHE_BYTES = 20 * 1024 * 1024
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
current_cache_bytes = 0

DENYLIST_FILE = Path(__file__).with_name("blocked_domains.txt")




def load_blocked_domains() -> set[str]:
    """Load the denylist on each request so edits take effect immediately."""
    blocked: set[str] = set()

    with DENYLIST_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            domain = line.strip().lower()
            if not domain or domain.startswith("#"):
                continue
            blocked.add(domain.rstrip("."))

    return blocked


def is_blocked_host(host: str, blocked_domains: set[str]) -> bool:
    host = host.lower().rstrip(".")
    return any(
        host == domain or host.endswith("." + domain)
        for domain in blocked_domains
    )




def _cache_control_directives(value: str) -> dict[str, str | None]:
    """Parse the simple Cache-Control directives used by this project."""
    directives: dict[str, str | None] = {}
    for part in value.split(","):
        name, separator, raw_value = part.partition("=")
        name = name.strip().lower()
        if name:
            directives[name] = raw_value.strip().strip('"') if separator else None
    return directives


def request_allows_cache(headers: Message) -> bool:
    if headers.get("Authorization") or headers.get("Cookie"):
        return False

    directives = {
        part.split("=", 1)[0].strip().lower()
        for part in headers.get("Cache-Control", "").split(",")
    }

    if directives.intersection({"no-store", "no-cache"}):
        return False

    if "no-cache" in headers.get("Pragma", "").lower():
        return False

    return True


def get_cache_ttl(
    request_headers: Message,
    status: int,
    response_headers: list[tuple[str, str]],
) -> int | None:
    """Return a cache lifetime for responses supported by this cache."""
    if status != 200 or not request_allows_cache(request_headers):
        return None

    response_header_names = {name.lower() for name, _ in response_headers}
    if "set-cookie" in response_header_names or "vary" in response_header_names:
        return None

    cache_control = ",".join(
        value for name, value in response_headers
        if name.lower() == "cache-control"
    )
    directives = _cache_control_directives(cache_control)

    if any(name in directives for name in ("no-store", "no-cache", "private")):
        return None

    max_age = directives.get("s-maxage") or directives.get("max-age")
    if max_age is None or not max_age.isdigit():
        return None

    ttl = int(max_age)
    return ttl if ttl > 0 else None


def get_cached_response(key: CacheKey) -> CachedResponse | None:
    global current_cache_bytes

    with cache_lock:
        entry = response_cache.get(key)
        if entry is None:
            return None

        if time.monotonic() >= entry.expires_at:
            response_cache.pop(key, None)
            current_cache_bytes -= len(entry.body)
            return None

        response_cache.move_to_end(key)
        return entry


def store_cached_response(key: CacheKey, entry: CachedResponse) -> bool:
    """Insert an entry, evicting least-recently-used entries as needed."""
    global current_cache_bytes

    body_size = len(entry.body)
    if body_size > MAX_RESPONSE_BYTES or body_size > MAX_CACHE_BYTES:
        return False

    with cache_lock:
        now = time.monotonic()
        expired_keys = [
            old_key for old_key, old_entry in response_cache.items()
            if now >= old_entry.expires_at
        ]
        for old_key in expired_keys:
            old_entry = response_cache.pop(old_key)
            current_cache_bytes -= len(old_entry.body)

        old_entry = response_cache.pop(key, None)
        if old_entry is not None:
            current_cache_bytes -= len(old_entry.body)

        while response_cache and (
            len(response_cache) >= MAX_CACHE_ENTRIES
            or current_cache_bytes + body_size > MAX_CACHE_BYTES
        ):
            _, evicted = response_cache.popitem(last=False)
            current_cache_bytes -= len(evicted.body)

        response_cache[key] = entry
        response_cache.move_to_end(key)
        current_cache_bytes += body_size
        return True


def clear_expired_cache_entries() -> int:
    global current_cache_bytes

    now = time.monotonic()
    removed = 0
    with cache_lock:
        expired_keys = [
            key for key, entry in response_cache.items()
            if now >= entry.expires_at
        ]
        for key in expired_keys:
            entry = response_cache.pop(key)
            current_cache_bytes -= len(entry.body)
            removed += 1
    return removed


def send_cached_response(
    handler: BaseHTTPRequestHandler,
    cached: CachedResponse,
) -> None:
    handler.send_response(cached.status, cached.reason)

    excluded = {
        "connection", "keep-alive", "proxy-connection",
        "transfer-encoding", "content-length", "server", "date",
    }
    for name, value in cached.headers:
        if name.lower() not in excluded:
            handler.send_header(name, value)

    handler.send_header("Content-Length", str(len(cached.body)))
    handler.send_header("Connection", "close")
    handler.send_header("X-Proxy-Cache", "HIT")
    handler.end_headers()
    handler.wfile.write(cached.body)
    handler.close_connection = True




class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        try:
            target = urlsplit(self.path)
            if (
                target.scheme.lower() != "http"
                or not target.hostname
                or target.username is not None
                or target.password is not None
            ):
                self.send_error(400, "Expected a valid HTTP URL")
                return

            host = target.hostname
            port = target.port or 80
            path = target.path or "/"
            if target.query:
                path += "?" + target.query
        except ValueError:
            self.send_error(400, "Invalid destination URL")
            return

        if self.headers.get("Transfer-Encoding"):
            self.send_error(501, "Request body handling not implemented")
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_error(400, "Invalid Content-Length")
            return

        if content_length != 0:
            self.send_error(501, "Request body handling not implemented")
            return

        try:
            blocked_domains = load_blocked_domains()
        except OSError as error:
            self.log_message("Unable to load denylist: %s", error)
            self.send_error(500, "Proxy access policy unavailable")
            return

        if is_blocked_host(host, blocked_domains):
            self.log_message("BLOCKED destination=%s", host)
            self.send_error(403, "Destination blocked by proxy policy")
            return

        cache_key: CacheKey = (host.lower().rstrip("."), port, path)
        cache_eligible_request = request_allows_cache(self.headers)

        if cache_eligible_request:
            cached = get_cached_response(cache_key)
            if cached is not None:
                print(f"[CACHE HIT] http://{host}:{port}{path}")
                send_cached_response(self, cached)
                return
            print(f"[CACHE MISS] http://{host}:{port}{path}")
        else:
            print(f"[CACHE BYPASS] http://{host}:{port}{path}")

        hop_by_hop = {
            "connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "proxy-connection", "te", "trailer",
            "transfer-encoding", "upgrade", "host", "content-length",
        }
        connection_tokens = {
            token.strip().lower()
            for token in self.headers.get("Connection", "").split(",")
            if token.strip()
        }
        hop_by_hop.update(connection_tokens)
        request_headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in hop_by_hop
        }

        upstream: HTTPConnection | None = None
        headers_sent = False

        try:
            print(f"[FORWARD] GET http://{host}:{port}{path}")
            upstream = HTTPConnection(host, port, timeout=10)
            upstream.request("GET", path, headers=request_headers)
            response = upstream.getresponse()

            status = response.status
            reason = response.reason
            response_headers = response.getheaders()

            response_hop_by_hop = {
                "connection", "keep-alive", "proxy-connection", "te",
                "trailer", "transfer-encoding", "upgrade",
            }
            for name, value in response_headers:
                if name.lower() == "connection":
                    response_hop_by_hop.update(
                        token.strip().lower()
                        for token in value.split(",")
                        if token.strip()
                    )

            ttl = get_cache_ttl(self.headers, status, response_headers)
            cache_buffer: bytearray | None = (
                bytearray() if ttl is not None else None
            )
            cache_too_large = False

            excluded_response_headers = response_hop_by_hop | {
                "content-length", "server", "date",
            }

            self.send_response(status, reason)
            for name, value in response_headers:
                if name.lower() not in excluded_response_headers:
                    self.send_header(name, value)

            self.send_header("Connection", "close")
            self.send_header(
                "X-Proxy-Cache", "MISS" if cache_eligible_request else "BYPASS"
            )
            self.end_headers()
            headers_sent = True
            self.close_connection = True

            total_bytes = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break

                self.wfile.write(chunk)
                total_bytes += len(chunk)

                if cache_buffer is not None:
                    if len(cache_buffer) + len(chunk) <= MAX_RESPONSE_BYTES:
                        cache_buffer.extend(chunk)
                    else:
                        cache_buffer = None
                        cache_too_large = True

            if cache_too_large:
                print(
                    f"[CACHE SKIP] Response exceeds {MAX_RESPONSE_BYTES} bytes"
                )
            elif cache_buffer is not None and ttl is not None:
                cache_headers = [
                    (name, value)
                    for name, value in response_headers
                    if name.lower() not in excluded_response_headers
                ]
                cached_response = CachedResponse(
                    status=status,
                    reason=reason,
                    headers=cache_headers,
                    body=bytes(cache_buffer),
                    expires_at=time.monotonic() + ttl,
                )
                if store_cached_response(cache_key, cached_response):
                    print(
                        f"[CACHE STORE] {host}{path}, TTL={ttl}s, "
                        f"body={len(cache_buffer)} bytes"
                    )
                else:
                    print("[CACHE SKIP] Cache capacity limit reached")
            else:
                print("[CACHE SKIP] Response is not cacheable")

            print(f"[RESPONSE] {status}, {total_bytes} bytes")

        except (OSError, TimeoutError, ConnectionError) as error:
            self.log_message("Upstream/client I/O error: %s", error)
            if not headers_sent and not self.wfile.closed:
                try:
                    self.send_error(502, "Unable to reach origin server")
                except OSError:
                    pass
        except Exception as error:
            self.log_message("Unexpected proxy error: %s: %s", type(error).__name__, error)
            if not headers_sent and not self.wfile.closed:
                try:
                    self.send_error(502, "Unable to reach origin server")
                except OSError:
                    pass
        finally:
            if upstream is not None:
                upstream.close()

    def log_message(self, format_string: str, *args: object) -> None:
        super().log_message(format_string, *args)




class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 100

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        max_workers: int = 50,
        client_timeout: float = 15,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self._slots = threading.BoundedSemaphore(max_workers)
        self.client_timeout = client_timeout
        super().__init__(server_address, handler_class)

    def get_request(self) -> tuple[socket.socket, tuple[str, int]]:
        request, client_address = super().get_request()
        request.settimeout(self.client_timeout)
        return request, client_address

    def process_request(
        self,
        request: socket.socket,
        client_address: tuple[str, int],
    ) -> None:
        if not self._slots.acquire(blocking=False):
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Content-Length: 0\r\n"
                    b"Connection: close\r\n"
                    b"Retry-After: 1\r\n\r\n"
                )
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return

        try:
            super().process_request(request, client_address)
        except Exception:
            self._slots.release()
            raise

    def process_request_thread(
        self,
        request: socket.socket,
        client_address: tuple[str, int],
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()




if __name__ == "__main__":
    server = BoundedThreadingHTTPServer(
        ("127.0.0.1", 8080),
        ProxyHandler,
        max_workers=50,
        client_timeout=15,
    )

    print("HTTP proxy listening on http://127.0.0.1:8080")
    print("Maximum active handlers: 50")
    print("Client socket timeout: 15 seconds")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down proxy...")
    finally:
        server.server_close()

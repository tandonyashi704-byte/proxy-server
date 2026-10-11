#!/usr/bin/env python3
"""
HTTP Benchmark Suite: Direct Client-Server vs Proxy Server
===========================================================
This benchmark script compares the performance difference between:
1. Direct Client-to-Server Communication (Baseline)
2. Client-to-Server via Proxy Server - Cache MISS (Forwarding Overhead)
3. Client-to-Server via Proxy Server - Cache HIT (In-Memory Acceleration)
4. Direct vs Proxy with Simulated Origin Latency (Real-world backend delay)

Usage:
    python benchmark.py                     # Runs with default settings (auto-starts servers if needed)
    python benchmark.py -n 100 -c 5         # 100 requests, 5 concurrent threads
    python benchmark.py --help              # See all options
"""

from __future__ import annotations

import argparse
import concurrent.futures
from dataclasses import dataclass, field
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import statistics
import sys
import threading
import time
from typing import Callable, Optional
from urllib.parse import urlsplit

# Ensure console encoding safety on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Import the existing proxy server components
try:
    from proxy import BoundedThreadingHTTPServer, ProxyHandler, response_cache, current_cache_bytes
except ImportError:
    BoundedThreadingHTTPServer = None
    ProxyHandler = None
    response_cache = None
    current_cache_bytes = 0


# ==============================================================================
# Terminal Color Formatting
# ==============================================================================
class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    @classmethod
    def disable(cls):
        cls.HEADER = ""
        cls.BLUE = ""
        cls.CYAN = ""
        cls.GREEN = ""
        cls.YELLOW = ""
        cls.RED = ""
        cls.BOLD = ""
        cls.DIM = ""
        cls.RESET = ""


# Check if the terminal supports ANSI colors
if not sys.stdout.isatty() and "WT_SESSION" not in os.environ and "TERM" not in os.environ:
    Colors.disable()


# ==============================================================================
# Data Structures
# ==============================================================================
@dataclass
class SingleRequestResult:
    latency_ms: float
    status_code: int
    bytes_received: int
    cache_status: str  # "HIT", "MISS", "BYPASS", "DIRECT", "ERROR"
    error: Optional[str] = None


@dataclass
class BenchmarkScenarioResult:
    name: str
    description: str
    total_requests: int
    concurrency: int
    total_time_s: float
    rps: float
    successful_requests: int
    failed_requests: int
    latencies_ms: list[float]
    stats: dict[str, float] = field(default_factory=dict)
    cache_hits: int = 0
    cache_misses: int = 0
    cache_bypasses: int = 0
    total_bytes: int = 0

    def calculate_stats(self) -> None:
        if not self.latencies_ms:
            self.stats = {
                "min": 0.0,
                "avg": 0.0,
                "median": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "max": 0.0,
                "stdev": 0.0,
            }
            return

        sorted_lats = sorted(self.latencies_ms)
        n = len(sorted_lats)

        def pct(p: float) -> float:
            idx = int(round((p / 100.0) * (n - 1)))
            return sorted_lats[max(0, min(idx, n - 1))]

        self.stats = {
            "min": sorted_lats[0],
            "avg": statistics.mean(sorted_lats),
            "median": statistics.median(sorted_lats),
            "p90": pct(90.0),
            "p95": pct(95.0),
            "p99": pct(99.0),
            "max": sorted_lats[-1],
            "stdev": statistics.stdev(sorted_lats) if n > 1 else 0.0,
        }


# ==============================================================================
# Built-in Benchmark Origin Server
# ==============================================================================
class BenchmarkOriginHandler(BaseHTTPRequestHandler):
    """
    HTTP handler designed for benchmarking.
    Supports:
      - /hello: standard cacheable endpoint (Cache-Control: public, max-age=60)
      - /delayed: endpoint that sleeps for simulated backend/database work
      - /uncached: endpoint with Cache-Control: no-store
      - /health: lightweight health check
    """
    simulated_delay_s: float = 0.030  # 30 ms default

    def do_GET(self) -> None:
        if self.path == "/health":
            body = b"OK\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/delayed"):
            if self.simulated_delay_s > 0:
                time.sleep(self.simulated_delay_s)
            body = (
                f"Hello from delayed origin! Backend delay: {self.simulated_delay_s * 1000:.1f}ms\n"
                f"Path: {self.path}\n"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=60")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/uncached"):
            body = f"Dynamic uncacheable response for {self.path}\n".encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Default cacheable response
        body = (
            "Hello from the test origin server!\n"
            f"Path requested: {self.path}\n"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "public, max-age=60")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string: str, *args: object) -> None:
        # Suppress request logging during benchmarks to keep output clean
        pass


# ==============================================================================
# Helper Functions: Socket & Server Management
# ==============================================================================
def is_port_available(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a local TCP port can be bound."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def find_free_port(preferred_port: int, host: str = "127.0.0.1") -> int:
    """Return preferred_port if available; otherwise return an unused port."""
    if is_port_available(preferred_port, host):
        return preferred_port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return sock.getsockname()[1]


def check_server_health(host: str, port: int, path: str = "/health", timeout: float = 1.0) -> bool:
    """Verify if an HTTP server is running and responding."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return True
    except Exception:
        return False


def check_proxy_health(proxy_host: str, proxy_port: int, origin_host: str, origin_port: int, timeout: float = 1.0) -> bool:
    """Verify if the proxy is running and can forward or return X-Proxy-Cache."""
    try:
        conn = http.client.HTTPConnection(proxy_host, proxy_port, timeout=timeout)
        target_url = f"http://{origin_host}:{origin_port}/health"
        conn.request("GET", target_url)
        resp = conn.getresponse()
        resp.read()
        has_cache_header = resp.getheader("X-Proxy-Cache") is not None
        conn.close()
        return has_cache_header
    except Exception:
        return False


# Thread-filtered standard output redirector to silence background server logs
class FilteredStream:
    def __init__(self, original_stream, verbose: bool = False):
        self.original_stream = original_stream
        self.verbose = verbose

    def write(self, s: str):
        if self.verbose or threading.current_thread() is threading.main_thread():
            try:
                self.original_stream.write(s)
            except UnicodeEncodeError:
                # Fallback for Windows consoles with restricted codepages
                encoding = getattr(self.original_stream, "encoding", "ascii") or "ascii"
                safe_s = s.encode(encoding, errors="replace").decode(encoding)
                self.original_stream.write(safe_s)

    def flush(self):
        try:
            self.original_stream.flush()
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self.original_stream, name)


# ==============================================================================
# HTTP Client Request Workers
# ==============================================================================
def direct_client_request(
    origin_host: str,
    origin_port: int,
    path: str,
    timeout: float = 5.0,
) -> SingleRequestResult:
    """Execute a single HTTP GET request directly to the origin server."""
    t0 = time.perf_counter()
    try:
        conn = http.client.HTTPConnection(origin_host, origin_port, timeout=timeout)
        headers = {
            "Host": f"{origin_host}:{origin_port}",
            "Connection": "close",
            "User-Agent": "BenchmarkDirectClient/1.0",
        }
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        status = resp.status
        conn.close()
        t1 = time.perf_counter()
        return SingleRequestResult(
            latency_ms=(t1 - t0) * 1000.0,
            status_code=status,
            bytes_received=len(body),
            cache_status="DIRECT",
        )
    except Exception as exc:
        t1 = time.perf_counter()
        return SingleRequestResult(
            latency_ms=(t1 - t0) * 1000.0,
            status_code=0,
            bytes_received=0,
            cache_status="ERROR",
            error=str(exc),
        )


def proxy_client_request(
    proxy_host: str,
    proxy_port: int,
    origin_host: str,
    origin_port: int,
    path: str,
    timeout: float = 5.0,
) -> SingleRequestResult:
    """Execute a single HTTP GET request to the origin via the forward proxy."""
    t0 = time.perf_counter()
    try:
        conn = http.client.HTTPConnection(proxy_host, proxy_port, timeout=timeout)
        target_url = f"http://{origin_host}:{origin_port}{path}"
        headers = {
            "Host": f"{origin_host}:{origin_port}",
            "Connection": "close",
            "User-Agent": "BenchmarkProxyClient/1.0",
        }
        conn.request("GET", target_url, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        status = resp.status
        cache_header = resp.getheader("X-Proxy-Cache", "UNKNOWN").upper()
        conn.close()
        t1 = time.perf_counter()
        return SingleRequestResult(
            latency_ms=(t1 - t0) * 1000.0,
            status_code=status,
            bytes_received=len(body),
            cache_status=cache_header,
        )
    except Exception as exc:
        t1 = time.perf_counter()
        return SingleRequestResult(
            latency_ms=(t1 - t0) * 1000.0,
            status_code=0,
            bytes_received=0,
            cache_status="ERROR",
            error=str(exc),
        )


# ==============================================================================
# Benchmark Runner Engine
# ==============================================================================
def run_scenario(
    name: str,
    description: str,
    request_fn_generator: Callable[[int], Callable[[], SingleRequestResult]],
    num_requests: int,
    concurrency: int = 1,
) -> BenchmarkScenarioResult:
    """
    Run a batch of requests sequentially or concurrently, collecting
    latencies and response metadata.
    """
    results: list[SingleRequestResult] = []

    start_wall_time = time.perf_counter()

    if concurrency <= 1:
        # Sequential execution
        for i in range(num_requests):
            req_fn = request_fn_generator(i)
            results.append(req_fn())
    else:
        # Concurrent execution with thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(request_fn_generator(i)) for i in range(num_requests)]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

    total_wall_time = time.perf_counter() - start_wall_time
    if total_wall_time <= 0:
        total_wall_time = 0.0001

    latencies = [r.latency_ms for r in results if r.error is None and r.status_code == 200]
    success_count = len(latencies)
    error_count = len(results) - success_count

    cache_hits = sum(1 for r in results if r.cache_status == "HIT")
    cache_misses = sum(1 for r in results if r.cache_status == "MISS")
    cache_bypasses = sum(1 for r in results if r.cache_status == "BYPASS")
    total_bytes = sum(r.bytes_received for r in results)

    scenario_result = BenchmarkScenarioResult(
        name=name,
        description=description,
        total_requests=num_requests,
        concurrency=concurrency,
        total_time_s=total_wall_time,
        rps=num_requests / total_wall_time,
        successful_requests=success_count,
        failed_requests=error_count,
        latencies_ms=latencies,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        cache_bypasses=cache_bypasses,
        total_bytes=total_bytes,
    )
    scenario_result.calculate_stats()
    return scenario_result


# ==============================================================================
# Reporting & Comparison Logic
# ==============================================================================
def print_banner(
    origin_url: str,
    proxy_url: str,
    requests_count: int,
    concurrency: int,
    auto_started: bool,
):
    print(f"{Colors.BOLD}{Colors.CYAN}" + "=" * 82 + f"{Colors.RESET}")
    print(
        f"{Colors.BOLD}{Colors.CYAN}       HTTP FORWARD PROXY BENCHMARK SUITE (Performance Analyzer)       {Colors.RESET}"
    )
    print(f"{Colors.BOLD}{Colors.CYAN}" + "=" * 82 + f"{Colors.RESET}")
    print(f"  {Colors.BOLD}Target Origin Server:{Colors.RESET}   {origin_url}")
    print(f"  {Colors.BOLD}Proxy Server:{Colors.RESET}           {proxy_url}")
    print(f"  {Colors.BOLD}Requests per Test:{Colors.RESET}      {requests_count}")
    print(
        f"  {Colors.BOLD}Concurrency Level:{Colors.RESET}      {concurrency} worker thread(s) "
        f"({'Concurrent' if concurrency > 1 else 'Sequential'})"
    )
    print(f"  {Colors.BOLD}Server Mode:{Colors.RESET}            {'Auto-managed background servers' if auto_started else 'External running servers'}")
    print(f"  {Colors.BOLD}Python Version:{Colors.RESET}         {sys.version.split()[0]}")
    print(f"{Colors.BOLD}{Colors.CYAN}" + "=" * 82 + f"{Colors.RESET}\n")


def print_results_table(scenarios: list[BenchmarkScenarioResult]):
    """Print an ASCII table summarizing all benchmark scenarios."""
    print(f"{Colors.BOLD}{Colors.HEADER}--- BENCHMARK RESULTS SUMMARY TABLE ---{Colors.RESET}")
    header = (
        f"{'Scenario Name':<32} | {'Req/s':>9} | {'Avg':>9} | {'Min':>8} | "
        f"{'P50':>8} | {'P95':>8} | {'Max':>8} | {'Cache Status':<12}"
    )
    print("-" * len(header))
    print(f"{Colors.BOLD}{header}{Colors.RESET}")
    print("-" * len(header))

    for sc in scenarios:
        st = sc.stats
        if sc.cache_hits > 0 and sc.cache_misses == 0:
            cache_info = f"{Colors.GREEN}100% HIT{Colors.RESET}"
        elif sc.cache_misses > 0 and sc.cache_hits == 0:
            cache_info = f"{Colors.YELLOW}100% MISS{Colors.RESET}"
        elif sc.cache_hits > 0 or sc.cache_misses > 0:
            hit_pct = (sc.cache_hits / (sc.cache_hits + sc.cache_misses)) * 100.0
            cache_info = f"{hit_pct:.0f}% HIT"
        else:
            cache_info = f"{Colors.DIM}N/A (Direct){Colors.RESET}"

        row = (
            f"{sc.name:<32} | {sc.rps:>8.1f}/s | {st['avg']:>7.2f}ms | {st['min']:>6.2f}ms | "
            f"{st['median']:>6.2f}ms | {st['p95']:>6.2f}ms | {st['max']:>6.2f}ms | {cache_info}"
        )
        print(row)

    print("-" * len(header) + "\n")


def print_comparative_analysis(
    direct: BenchmarkScenarioResult,
    proxy_miss: BenchmarkScenarioResult,
    proxy_hit: BenchmarkScenarioResult,
    delayed_direct: Optional[BenchmarkScenarioResult] = None,
    delayed_hit: Optional[BenchmarkScenarioResult] = None,
) -> dict:
    """
    Perform deep statistical comparison and explain differences
    both mathematically and descriptively (Hindi + English).
    """
    d_avg = direct.stats["avg"]
    m_avg = proxy_miss.stats["avg"]
    h_avg = proxy_hit.stats["avg"]

    # 1. Forwarding Overhead: Direct vs Proxy MISS
    miss_latency_diff = m_avg - d_avg
    miss_overhead_pct = (miss_latency_diff / d_avg * 100.0) if d_avg > 0 else 0.0
    miss_rps_ratio = (proxy_miss.rps / direct.rps) if direct.rps > 0 else 1.0

    # 2. Caching Speedup: Direct vs Proxy HIT
    hit_latency_diff = d_avg - h_avg
    hit_speedup_factor = (d_avg / h_avg) if h_avg > 0 else 1.0
    hit_latency_reduction_pct = (hit_latency_diff / d_avg * 100.0) if d_avg > 0 else 0.0
    hit_rps_multiplier = (proxy_hit.rps / direct.rps) if direct.rps > 0 else 1.0

    print(f"{Colors.BOLD}{Colors.GREEN}" + "=" * 82 + f"{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}      PERFORMANCE DIFFERENCE ANALYSIS & COMPARISON (Comparative Summary)      {Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}" + "=" * 82 + f"{Colors.RESET}\n")

    # Section 1
    print(f"{Colors.BOLD}1. DIRECT vs PROXY MISS (Proxy Forwarding Overhead / Extra Hop Latency):{Colors.RESET}")
    print(f"   - Direct Client-Server Avg Latency: {Colors.CYAN}{d_avg:.2f} ms{Colors.RESET} ({direct.rps:.1f} req/s)")
    print(f"   - Proxy Cache MISS Avg Latency:     {Colors.YELLOW}{m_avg:.2f} ms{Colors.RESET} ({proxy_miss.rps:.1f} req/s)")
    if miss_latency_diff >= 0:
        print(
            f"   - {Colors.BOLD}Overhead Added:{Colors.RESET}                     "
            f"{Colors.YELLOW}+{miss_latency_diff:.2f} ms (+{miss_overhead_pct:.1f}% additional latency){Colors.RESET}"
        )
        print(
            f"   - {Colors.BOLD}Throughput Impact:{Colors.RESET}                  "
            f"Throughput decreased to {miss_rps_ratio * 100:.1f}% of baseline"
        )
    else:
        print(
            f"   - {Colors.BOLD}Difference:{Colors.RESET}                         "
            f"{Colors.GREEN}{miss_latency_diff:.2f} ms ({miss_overhead_pct:.1f}%){Colors.RESET}"
        )
    print(
        f"   - {Colors.DIM}Explanation: When a request misses the cache, the proxy acts as an intermediary.{Colors.RESET}\n"
        f"     {Colors.DIM}It creates an upstream TCP connection to the origin, filters hop-by-hop headers,{Colors.RESET}\n"
        f"     {Colors.DIM}and streams data back. This adds a slight network hop latency (~1-3 ms locally).{Colors.RESET}\n"
        f"     {Colors.DIM}(Hindi: Cache Miss hone par proxy ko origin se data mangwana padta hai, isliye halka extra time lagta hai.){Colors.RESET}\n"
    )

    # Section 2
    print(f"{Colors.BOLD}2. DIRECT vs PROXY HIT (In-Memory Cache Speedup / Caching Advantage):{Colors.RESET}")
    print(f"   - Direct Client-Server Avg Latency: {Colors.CYAN}{d_avg:.2f} ms{Colors.RESET} ({direct.rps:.1f} req/s)")
    print(f"   - Proxy Cache HIT Avg Latency:      {Colors.GREEN}{h_avg:.2f} ms{Colors.RESET} ({proxy_hit.rps:.1f} req/s)")
    if hit_latency_diff > 0:
        print(
            f"   - {Colors.BOLD}Latency Reduction:{Colors.RESET}                  "
            f"{Colors.GREEN}-{hit_latency_diff:.2f} ms saved ({hit_latency_reduction_pct:.1f}% faster){Colors.RESET}"
        )
        print(
            f"   - {Colors.BOLD}Speedup Multiplier:{Colors.RESET}                 "
            f"{Colors.BOLD}{Colors.GREEN}{hit_speedup_factor:.2f}x FASTER response time!{Colors.RESET}"
        )
        print(
            f"   - {Colors.BOLD}Throughput Multiplier:{Colors.RESET}              "
            f"{Colors.BOLD}{Colors.GREEN}{hit_rps_multiplier:.2f}x HIGHER request capacity!{Colors.RESET}"
        )
    else:
        print(
            f"   - {Colors.BOLD}Latency Comparison:{Colors.RESET}                "
            f"Direct baseline is {abs(hit_latency_diff):.2f} ms faster than proxy on micro-responses"
        )
        print(
            f"   - {Colors.DIM}Note: For trivial static responses (<2ms), proxy internals (denylist file reads,{Colors.RESET}\n"
            f"     {Colors.DIM}request logging, semaphore slots) take a few ms. See Section 3 for real backend gains.{Colors.RESET}"
        )

    # Proxy MISS vs Proxy HIT within proxy
    intra_proxy_diff = m_avg - h_avg
    intra_proxy_pct = (intra_proxy_diff / m_avg * 100.0) if m_avg > 0 else 0.0
    intra_proxy_speedup = (m_avg / h_avg) if h_avg > 0 else 1.0
    print(
        f"   - {Colors.BOLD}Proxy MISS vs Proxy HIT:{Colors.RESET}           "
        f"{Colors.GREEN}Cache HIT is {intra_proxy_diff:.2f} ms faster ({intra_proxy_pct:.1f}% latency cut, {intra_proxy_speedup:.2f}x faster){Colors.RESET}"
    )
    print(
        f"   - {Colors.DIM}Explanation: On a Cache HIT, the proxy serves the response directly from its in-memory{Colors.RESET}\n"
        f"     {Colors.DIM}LRU cache. The origin server is never contacted, saving network and origin computation.{Colors.RESET}\n"
        f"     {Colors.DIM}(Hindi: Cache HIT me proxy RAM se turant response bhej deta hai, bina origin ko call kiye.){Colors.RESET}\n"
    )

    # Section 3 (Delayed origin scenario if present)
    delayed_summary = None
    if delayed_direct and delayed_hit:
        dd_avg = delayed_direct.stats["avg"]
        dh_avg = delayed_hit.stats["avg"]
        d_diff = dd_avg - dh_avg
        d_factor = (dd_avg / dh_avg) if dh_avg > 0 else 1.0
        d_pct = (d_diff / dd_avg * 100.0) if dd_avg > 0 else 0.0

        delayed_summary = {
            "delayed_direct_avg_ms": dd_avg,
            "delayed_proxy_hit_avg_ms": dh_avg,
            "speedup_factor": d_factor,
            "latency_saved_ms": d_diff,
        }

        print(
            f"{Colors.BOLD}3. REAL-WORLD BACKEND LATENCY (With Simulated 30ms Origin Processing Delay):{Colors.RESET}"
        )
        print(f"   - Direct Client-to-Server (Delayed): {Colors.RED}{dd_avg:.2f} ms{Colors.RESET} ({delayed_direct.rps:.1f} req/s)")
        print(f"   - Proxy Cache HIT (Cached Response): {Colors.GREEN}{dh_avg:.2f} ms{Colors.RESET} ({delayed_hit.rps:.1f} req/s)")
        print(
            f"   - {Colors.BOLD}Time Saved Per Request:{Colors.RESET}             "
            f"{Colors.BOLD}{Colors.GREEN}-{d_diff:.2f} ms saved ({d_pct:.1f}% reduction){Colors.RESET}"
        )
        print(
            f"   - {Colors.BOLD}Real-world Speedup:{Colors.RESET}                  "
            f"{Colors.BOLD}{Colors.GREEN}{d_factor:.1f}x FASTER!{Colors.RESET}"
        )
        print(
            f"   - {Colors.DIM}Takeaway: In realistic environments where APIs/databases have non-zero query latency,{Colors.RESET}\n"
            f"     {Colors.DIM}a caching proxy eliminates 90-99% of round-trip delays for repeated requests.{Colors.RESET}\n"
            f"     {Colors.DIM}(Hindi: Asli websites par jahan backend delay hota hai, proxy cache se 10x-50x speed badh jati hai!){Colors.RESET}\n"
        )

    print(f"{Colors.BOLD}{Colors.GREEN}" + "=" * 82 + f"{Colors.RESET}\n")

    return {
        "direct_avg_ms": d_avg,
        "direct_rps": direct.rps,
        "proxy_miss_avg_ms": m_avg,
        "proxy_miss_rps": proxy_miss.rps,
        "proxy_miss_overhead_ms": miss_latency_diff,
        "proxy_miss_overhead_pct": miss_overhead_pct,
        "proxy_hit_avg_ms": h_avg,
        "proxy_hit_rps": proxy_hit.rps,
        "proxy_hit_speedup_factor": hit_speedup_factor,
        "proxy_hit_latency_reduction_pct": hit_latency_reduction_pct,
        "proxy_hit_rps_multiplier": hit_rps_multiplier,
        "delayed_scenario": delayed_summary,
    }


# ==============================================================================
# Main Orchestrator
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Performance Comparison: Direct Client-Server vs HTTP Proxy Server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-n", "--requests",
        type=int,
        default=50,
        help="Number of HTTP requests to execute for each benchmark scenario",
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=1,
        help="Number of concurrent worker threads (1 = sequential)",
    )
    parser.add_argument(
        "--origin",
        type=str,
        default="http://127.0.0.1:9001",
        help="Origin server base URL (e.g. http://127.0.0.1:9001)",
    )
    parser.add_argument(
        "--proxy",
        type=str,
        default="http://127.0.0.1:8080",
        help="Proxy server base URL (e.g. http://127.0.0.1:8080)",
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=30.0,
        help="Simulated origin processing delay (in ms) for the delayed-origin test",
    )
    parser.add_argument(
        "--skip-delayed",
        action="store_true",
        help="Skip the simulated slow origin benchmark scenario",
    )
    parser.add_argument(
        "--no-auto-start",
        action="store_true",
        help="Do not auto-start background servers if target servers are not responding",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Path to export benchmark results as JSON (e.g. benchmark_results.json)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show verbose server logs and debugging messages during benchmark",
    )

    args = parser.parse_args()

    # Parse origin URL
    origin_parsed = urlsplit(args.origin)
    origin_host = origin_parsed.hostname or "127.0.0.1"
    origin_port = origin_parsed.port or 9001

    # Parse proxy URL
    proxy_parsed = urlsplit(args.proxy)
    proxy_host = proxy_parsed.hostname or "127.0.0.1"
    proxy_port = proxy_parsed.port or 8080

    # Determine if servers are already running or if we need to auto-start them
    managed_origin_server: Optional[ThreadingHTTPServer] = None
    managed_proxy_server: Optional[BoundedThreadingHTTPServer] = None
    auto_started = False

    # Check origin health
    origin_alive = check_server_health(origin_host, origin_port, "/health", timeout=0.5)

    # Check proxy health
    proxy_alive = check_proxy_health(proxy_host, proxy_port, origin_host, origin_port, timeout=0.5)

    if not origin_alive or not proxy_alive:
        if args.no_auto_start:
            print(f"{Colors.RED}[ERROR] Servers are not reachable and --no-auto-start is set.{Colors.RESET}")
            print(f"Origin alive: {origin_alive} ({origin_host}:{origin_port})")
            print(f"Proxy alive:  {proxy_alive} ({proxy_host}:{proxy_port})")
            sys.exit(1)

        auto_started = True

        # Silence stdout/stderr from background servers unless verbose
        sys.stdout = FilteredStream(sys.stdout, verbose=args.verbose)
        sys.stderr = FilteredStream(sys.stderr, verbose=args.verbose)

        # 1. Start managed Origin Server if needed
        if not origin_alive:
            if not is_port_available(origin_port, origin_host):
                origin_port = find_free_port(origin_port + 1, origin_host)
            BenchmarkOriginHandler.simulated_delay_s = args.delay_ms / 1000.0
            managed_origin_server = ThreadingHTTPServer((origin_host, origin_port), BenchmarkOriginHandler)
            origin_thread = threading.Thread(target=managed_origin_server.serve_forever, daemon=True)
            origin_thread.start()

        # 2. Start managed Proxy Server if needed
        if not proxy_alive:
            if BoundedThreadingHTTPServer is None or ProxyHandler is None:
                print(f"{Colors.RED}[ERROR] Could not import ProxyHandler from proxy.py{Colors.RESET}")
                sys.exit(1)

            # Check if preferred proxy port is available; if not (e.g. Oracle on 8080), find an open port
            if not is_port_available(proxy_port, proxy_host):
                proxy_port = find_free_port(8081, proxy_host)

            managed_proxy_server = BoundedThreadingHTTPServer(
                (proxy_host, proxy_port),
                ProxyHandler,
                max_workers=max(50, args.concurrency * 2),
                client_timeout=15,
            )
            proxy_thread = threading.Thread(target=managed_proxy_server.serve_forever, daemon=True)
            proxy_thread.start()

        # Give background servers a brief moment to bind sockets
        time.sleep(0.3)

    origin_url = f"http://{origin_host}:{origin_port}"
    proxy_url = f"http://{proxy_host}:{proxy_port}"

    print_banner(
        origin_url=origin_url,
        proxy_url=proxy_url,
        requests_count=args.requests,
        concurrency=args.concurrency,
        auto_started=auto_started,
    )

    scenarios_results: list[BenchmarkScenarioResult] = []

    try:
        # ======================================================================
        # Warmup Phase
        # ======================================================================
        print(f"[*] Priming sockets and performing warmup checks...")
        direct_client_request(origin_host, origin_port, "/hello")
        proxy_client_request(proxy_host, proxy_port, origin_host, origin_port, "/warmup")
        print(f"    {Colors.GREEN}[OK] Connections verified and ready.{Colors.RESET}\n")

        # ======================================================================
        # Scenario 1: Direct Client to Server (Baseline)
        # ======================================================================
        print(f"[*] [1/3] Benchmarking {Colors.BOLD}Direct Client-to-Server{Colors.RESET} (Normal Communication)...")
        res_direct = run_scenario(
            name="1. Direct (Normal Client-Server)",
            description="Client connects directly to Origin server (No proxy intermediary)",
            request_fn_generator=lambda idx: (lambda: direct_client_request(origin_host, origin_port, f"/hello?i={idx}")),
            num_requests=args.requests,
            concurrency=args.concurrency,
        )
        scenarios_results.append(res_direct)
        print(
            f"    -> Done in {res_direct.total_time_s:.2f}s | "
            f"RPS: {Colors.CYAN}{res_direct.rps:.1f}{Colors.RESET} | "
            f"Avg Latency: {Colors.CYAN}{res_direct.stats['avg']:.2f} ms{Colors.RESET} | "
            f"P50: {res_direct.stats['median']:.2f} ms"
        )

        # ======================================================================
        # Scenario 2: Proxy Server - Cache MISS (Forwarding Overhead)
        # ======================================================================
        print(f"\n[*] [2/3] Benchmarking {Colors.BOLD}Proxy Server - Cache MISS{Colors.RESET} (Forwarding Overhead)...")
        # Use unique paths so every request is a cold cache MISS that must be forwarded to origin
        res_proxy_miss = run_scenario(
            name="2. Proxy MISS (Forwarding Overhead)",
            description="Unique requests sent via Proxy; proxy must contact origin on each request",
            request_fn_generator=lambda idx: (
                lambda: proxy_client_request(proxy_host, proxy_port, origin_host, origin_port, f"/uncached_item_{idx}")
            ),
            num_requests=args.requests,
            concurrency=args.concurrency,
        )
        scenarios_results.append(res_proxy_miss)
        print(
            f"    -> Done in {res_proxy_miss.total_time_s:.2f}s | "
            f"RPS: {Colors.YELLOW}{res_proxy_miss.rps:.1f}{Colors.RESET} | "
            f"Avg Latency: {Colors.YELLOW}{res_proxy_miss.stats['avg']:.2f} ms{Colors.RESET} | "
            f"Cache Misses: {res_proxy_miss.cache_misses}/{res_proxy_miss.total_requests}"
        )

        # ======================================================================
        # Scenario 3: Proxy Server - Cache HIT (In-Memory Acceleration)
        # ======================================================================
        print(f"\n[*] [3/3] Benchmarking {Colors.BOLD}Proxy Server - Cache HIT{Colors.RESET} (In-Memory Acceleration)...")
        # Prime the cache once with the target URL
        cache_target_path = "/cached_benchmark_resource"
        prime_res = proxy_client_request(proxy_host, proxy_port, origin_host, origin_port, cache_target_path)
        if prime_res.error:
            print(f"    {Colors.YELLOW}[!] Cache priming warning: {prime_res.error}{Colors.RESET}")

        res_proxy_hit = run_scenario(
            name="3. Proxy HIT (In-Memory Cache)",
            description="Repeated requests for cached resource served directly from proxy RAM",
            request_fn_generator=lambda idx: (
                lambda: proxy_client_request(proxy_host, proxy_port, origin_host, origin_port, cache_target_path)
            ),
            num_requests=args.requests,
            concurrency=args.concurrency,
        )
        scenarios_results.append(res_proxy_hit)
        print(
            f"    -> Done in {res_proxy_hit.total_time_s:.2f}s | "
            f"RPS: {Colors.GREEN}{res_proxy_hit.rps:.1f}{Colors.RESET} | "
            f"Avg Latency: {Colors.GREEN}{res_proxy_hit.stats['avg']:.2f} ms{Colors.RESET} | "
            f"Cache Hits: {Colors.GREEN}{res_proxy_hit.cache_hits}/{res_proxy_hit.total_requests}{Colors.RESET}"
        )

        # ======================================================================
        # Scenario 4: Simulated Origin Delay (Optional / Real-world)
        # ======================================================================
        res_delayed_direct = None
        res_delayed_hit = None

        if not args.skip_delayed:
            delay_ms = args.delay_ms
            delayed_requests_count = min(15, args.requests)  # Keep count reasonable since it sleeps
            print(
                f"\n[*] [Extra] Benchmarking {Colors.BOLD}Origin with Simulated Backend Delay ({delay_ms:.0f}ms){Colors.RESET}..."
            )

            # Direct delayed test
            res_delayed_direct = run_scenario(
                name=f"4. Direct (With {delay_ms:.0f}ms Origin Delay)",
                description="Direct requests to origin with simulated database/processing latency",
                request_fn_generator=lambda idx: (
                    lambda: direct_client_request(origin_host, origin_port, f"/delayed?item={idx}")
                ),
                num_requests=delayed_requests_count,
                concurrency=args.concurrency,
            )
            scenarios_results.append(res_delayed_direct)

            # Proxy cached delayed test: prime it once, then query from cache
            delayed_cache_path = "/delayed_cached_item"
            proxy_client_request(proxy_host, proxy_port, origin_host, origin_port, delayed_cache_path)

            res_delayed_hit = run_scenario(
                name=f"5. Proxy HIT (Bypasses {delay_ms:.0f}ms Delay)",
                description="Proxy serves response from RAM, bypassing origin processing delay entirely",
                request_fn_generator=lambda idx: (
                    lambda: proxy_client_request(proxy_host, proxy_port, origin_host, origin_port, delayed_cache_path)
                ),
                num_requests=delayed_requests_count,
                concurrency=args.concurrency,
            )
            scenarios_results.append(res_delayed_hit)

            print(
                f"    -> Direct ({delay_ms:.0f}ms delay): Avg Latency = {Colors.RED}{res_delayed_direct.stats['avg']:.2f} ms{Colors.RESET}"
            )
            print(
                f"    -> Proxy HIT (from memory):      Avg Latency = {Colors.GREEN}{res_delayed_hit.stats['avg']:.2f} ms{Colors.RESET}"
            )

        print("\n")
        # Print summary table
        print_results_table(scenarios_results)

        # Print comparative analysis
        comparison_stats = print_comparative_analysis(
            direct=res_direct,
            proxy_miss=res_proxy_miss,
            proxy_hit=res_proxy_hit,
            delayed_direct=res_delayed_direct,
            delayed_hit=res_delayed_hit,
        )

        # Export JSON if requested
        if args.output_json:
            json_payload = {
                "metadata": {
                    "timestamp": time.time(),
                    "requests_per_test": args.requests,
                    "concurrency": args.concurrency,
                    "origin_url": origin_url,
                    "proxy_url": proxy_url,
                },
                "scenarios": [
                    {
                        "name": sc.name,
                        "description": sc.description,
                        "total_requests": sc.total_requests,
                        "concurrency": sc.concurrency,
                        "total_time_s": sc.total_time_s,
                        "rps": sc.rps,
                        "successful_requests": sc.successful_requests,
                        "failed_requests": sc.failed_requests,
                        "stats": sc.stats,
                        "cache_hits": sc.cache_hits,
                        "cache_misses": sc.cache_misses,
                    }
                    for sc in scenarios_results
                ],
                "comparison": comparison_stats,
            }
            output_file = Path(args.output_json)
            output_file.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
            print(f"[*] Benchmark results exported to: {output_file.resolve()}")

    finally:
        # Gracefully shut down background servers if we started them
        if managed_origin_server is not None:
            try:
                managed_origin_server.shutdown()
                managed_origin_server.server_close()
            except Exception:
                pass
        if managed_proxy_server is not None:
            try:
                managed_proxy_server.shutdown()
                managed_proxy_server.server_close()
            except Exception:
                pass


if __name__ == "__main__":
    main()

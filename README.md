# HTTP Forward Proxy (Python)

A small HTTP forward proxy built with Python's standard library. It forwards HTTP `GET` requests to origin servers and adds a conservative in-memory response cache, a domain denylist, and bounded request concurrency.

> **Scope:** This project supports plain HTTP `GET` requests only. It does **not** support HTTPS `CONNECT` tunnelling, other HTTP methods, request bodies, or production deployment. Run it locally for learning and testing.

## Features

- **HTTP forwarding:** accepts absolute-form HTTP URLs and forwards requests to the destination server.
- **Concurrent clients:** handles requests on separate threads, with a maximum of 50 active handlers and a request queue size of 100.
- **Domain denylist:** blocks a domain and its subdomains with `403 Forbidden`.
- **In-memory response cache:** caches eligible `200 OK` responses when the origin explicitly supplies a positive `max-age` or `s-maxage` directive.
- **Cache safeguards:** avoids caching responses with directives such as `private`, `no-store`, or `no-cache`, responses with `Set-Cookie` or `Vary`, and requests containing authorization/cookie headers or cache-bypass conditions.
- **LRU eviction and limits:** up to 100 entries, 20 MiB total cached response bodies, and 2 MiB per cached response.
- **Cache visibility:** response header `X-Proxy-Cache` reports `HIT`, `MISS`, or `BYPASS`.
- **Standard-library only:** no third-party Python packages are required.

## Requirements

- Python 3.10 or newer recommended
- `curl` for the command-line tests below (optional; any HTTP client that supports a proxy can be used)

Check your Python version:

```bash
python3 --version
```

## Project layout

Keep these files together in the same directory:

```text
proxy-server/
├── proxy.py
├── blocked_domains.txt
├── test_origin.py
└── README.md
```

`proxy.py` loads `blocked_domains.txt` from its own directory. The `test_origin.py` helper below starts a local origin server that returns cacheable responses, so you can verify cache hits without depending on an external website.

## 1. Set up the project

Clone or download the repository, then enter the project directory. Make sure `proxy.py` and `blocked_domains.txt` are in the same folder.

No package installation is needed. Check that the files compile:

```bash
python3 -m py_compile proxy.py test_origin.py
```

## 2. Local test origin

The included `test_origin.py` helper starts a small local origin server on `127.0.0.1:9001`. It returns a plain-text body and sends `Cache-Control: public, max-age=30`, allowing you to test cache misses, cache hits, and expiration without depending on an external website.

If you are setting this project up manually, make sure `test_origin.py` is saved next to `proxy.py` and `blocked_domains.txt`. Its code uses only Python's standard library.

## 3. Start both servers

Open two terminal windows in the project directory.

**Terminal 1 — start the origin server:**

```bash
python3 test_origin.py
```

**Terminal 2 — start the proxy:**

```bash
python3 proxy.py
```

The proxy listens on `127.0.0.1:8080`; the test origin listens on `127.0.0.1:9001`. Keep both terminals running while testing.

## 4. Test forwarding and caching

Open a third terminal. The `--noproxy ""` option tells `curl` to use the proxy even for localhost URLs, which some environments otherwise bypass.

### A. First request — expected cache miss

```bash
curl --noproxy "" -i -x http://127.0.0.1:8080 http://127.0.0.1:9001/hello
```

Look for:

```text
HTTP/1.1 200 OK
X-Proxy-Cache: MISS
```

The response body should include `Path requested: /hello`. The proxy terminal should show a cache miss and a cache store.

### B. Repeat the same request — expected cache hit

Run the exact same command again:

```bash
curl --noproxy "" -i -x http://127.0.0.1:8080 http://127.0.0.1:9001/hello
```

Look for:

```text
HTTP/1.1 200 OK
X-Proxy-Cache: HIT
```

The second response should come from the proxy cache while the entry is fresh. The origin terminal should not log another `/hello` request for the cache hit.

### C. Different path — expected cache miss

```bash
curl --noproxy "" -i -x http://127.0.0.1:8080 http://127.0.0.1:9001/another-path
```

This URL has a different cache key, so it should return `X-Proxy-Cache: MISS` the first time.

### D. Expiration — expected miss after 30 seconds

After requesting `/hello`, wait at least 31 seconds, then run the `/hello` command again. The expired entry should be fetched from the origin and return `X-Proxy-Cache: MISS`.

## 5. Test domain blocking

The included `blocked_domains.txt` contains example entries:

```text
example.com
blocked.test
```

Try a blocked domain:

```bash
curl --noproxy "" -i -x http://127.0.0.1:8080 http://example.com/
```

Expected result: `403 Forbidden`. The proxy checks the denylist before looking in the cache, so a cached response cannot bypass a newly applied block rule.

To test subdomain matching:

```bash
curl --noproxy "" -i -x http://127.0.0.1:8080 http://sub.example.com/
```

This should also return `403 Forbidden`. The proxy rejects the request before trying to contact that host.

To block another domain, add one domain per line in `blocked_domains.txt`. Blank lines and lines beginning with `#` are ignored. Changes are read on each request, so a proxy restart is not required.

**Important:** Do not add `127.0.0.1` or `localhost` to the denylist while running the local-origin tests, or those tests will be blocked too.

## 6. Test bypassing the cache

Requests containing `Authorization` or `Cookie` headers should not be served from the cache. For example:

```bash
curl --noproxy "" -i -x http://127.0.0.1:8080 \
  -H 'Cookie: session=demo' \
  http://127.0.0.1:9001/hello
```

Expected proxy response header: `X-Proxy-Cache: BYPASS`.

## 7. Run a basic concurrency check

With both servers running, issue ten requests in parallel:

```bash
seq 1 10 | xargs -P 10 -I {} curl --noproxy "" -sS \
  -o /dev/null -w 'request {}: HTTP %{http_code}\n' \
  -x http://127.0.0.1:8080 \
  'http://127.0.0.1:9001/concurrent'
```

All requests should complete with HTTP `200`. Since all requests use the same cache key, concurrent first requests may result in more than one origin fetch; this simple proxy does not coalesce simultaneous cache misses.

## 8. Troubleshooting

- **`curl` connects directly instead of using the proxy:** include `--noproxy ""` in the command.
- **`502 Bad Gateway`:** verify `test_origin.py` is running on port `9001` and that the URL uses `http://`, not `https://`.
- **No cache hit:** use the exact same URL twice, make sure the origin sends `Cache-Control: public, max-age=30`, and ensure you are within the 30-second freshness window. Requests with cookies, authorization, conditional headers, range headers, or cache-bypass directives do not use the cache.
- **`403 Forbidden`:** check whether the requested host appears in `blocked_domains.txt` or matches a blocked domain's subdomain.
- **Port already in use:** stop the process using port `8080` or `9001`, or update the corresponding bind port in the Python file and adjust the commands.

## Limitations and security notes

- HTTP only; HTTPS URLs and `CONNECT` are not supported.
- Only `GET` requests without a request body are supported.
- The proxy binds to `127.0.0.1`, making it available only from the local machine by default.
- The cache is in memory and is cleared when the proxy process stops.
- This is an educational project, not a hardened production proxy. Do not expose it publicly without adding authentication, stronger destination/SSRF controls, more complete HTTP protocol handling, and additional testing.

## How it works

1. The proxy parses the absolute HTTP URL supplied by the client.
2. It loads the denylist and rejects blocked destinations.
3. For eligible requests, it checks the cache using the destination host, port, and path.
4. On a cache miss, it forwards the `GET` request to the origin server.
5. It streams the response to the client and buffers eligible small responses for caching.
6. It evicts expired or least-recently-used entries when necessary to stay within cache limits.

## License

No license has been specified yet. Add a `LICENSE` file before distributing the project if you want to state reuse and contribution terms explicitly.

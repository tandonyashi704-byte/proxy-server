import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from proxy import (
    CachedResponse,
    clear_expired_cache_entries,
    current_cache_bytes,
    get_cached_response,
    response_cache,
    store_cached_response,
)


def entry(body: bytes, ttl: int = 60) -> CachedResponse:
    return CachedResponse(
        status=200,
        reason="OK",
        headers=[],
        body=body,
        expires_at=time.monotonic() + ttl,
    )


def test_lru_eviction():
    response_cache.clear()

    import proxy
    proxy.current_cache_bytes = 0
    proxy.MAX_CACHE_ENTRIES = 2

    store_cached_response(("a", 80, "/"), entry(b"A"))
    store_cached_response(("b", 80, "/"), entry(b"B"))

    assert get_cached_response(("a", 80, "/")) is not None

    store_cached_response(("c", 80, "/"), entry(b"C"))

    assert get_cached_response(("a", 80, "/")) is not None
    assert get_cached_response(("b", 80, "/")) is None
    assert get_cached_response(("c", 80, "/")) is not None

    print("LRU eviction passed!")


def test_expired_entry():
    import proxy
    proxy.response_cache.clear()
    proxy.current_cache_bytes = 0

    store_cached_response(("expired", 80, "/"), entry(b"old", ttl=-1))

    assert get_cached_response(("expired", 80, "/")) is None
    assert proxy.current_cache_bytes == 0

    print("Expired-entry cleanup passed!")


if __name__ == "__main__":
    test_lru_eviction()
    test_expired_entry()
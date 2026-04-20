#!/usr/bin/env python3
"""smoke test. verifies the proxy forwards to the backend correctly.
run this after `docker-compose up -d` to make sure everything is wired."""

import sys
import time
import httpx


PROXY_URL = "http://localhost:8080"  # goes through nginx
BACKEND_URL = "http://localhost:9000"


def wait_for(url, timeout=30):
    """poll until the service is healthy."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(f"{url}/health", timeout=2)
            if r.status_code == 200:
                return True
        except httpx.RequestError:
            pass
        time.sleep(0.5)
    return False


def main():
    print("waiting for backend...")
    if not wait_for(BACKEND_URL):
        print("backend not responding")
        sys.exit(1)
    print("  backend up")

    print("waiting for proxy (via nginx)...")
    if not wait_for(PROXY_URL):
        print("proxy not responding")
        sys.exit(1)
    print("  proxy up")

    # reset backend stats
    httpx.post(f"{BACKEND_URL}/stats/reset")

    # hit the proxy a few times
    print("\nmaking 5 requests through proxy...")
    for i in range(5):
        r = httpx.get(f"{PROXY_URL}/data/key_{i}")
        data = r.json()
        print(f"  key_{i}: {r.status_code} — latency {data.get('_proxy_latency_ms')}ms")

    # check backend saw them
    stats = httpx.get(f"{BACKEND_URL}/stats").json()
    print(f"\nbackend saw {stats['total_calls']} calls (expected 5)")

    if stats["total_calls"] != 5:
        print("MISMATCH")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()

"""
Locust scenarios for sustained load testing across priority tiers.

Run modes:

  # Headless, mixed-tier sustained load
  locust -f tests/locustfile.py --headless \
      -u 200 -r 50 -t 30s --host http://localhost:8080

  # Web UI
  locust -f tests/locustfile.py --host http://localhost:8080
  # then open http://localhost:8089

User mix (default):
  60% normal traffic (tier 2)
  30% batch traffic  (tier 3)
  10% critical       (tier 1)

This roughly mirrors a realistic production mix where most traffic
is normal user requests, with some background batch jobs and a small
slice of high-priority paths (e.g. payment, login).
"""
import random
from locust import HttpUser, task, between


KEYS = [f"key_{i}" for i in range(20)]


class CriticalUser(HttpUser):
    """Tier-1 traffic: low rate, must always succeed."""
    wait_time = between(0.5, 1.5)
    weight = 1  # 10% of users

    @task
    def get_data(self):
        key = random.choice(KEYS)
        with self.client.get(
            f"/data/{key}",
            headers={"X-Priority": "1"},
            catch_response=True,
            name="GET /data/{key} [tier=1]",
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"tier-1 got status {response.status_code}")


class NormalUser(HttpUser):
    """Tier-2 traffic: bulk of load. Some shedding under stress is OK."""
    wait_time = between(0.1, 0.5)
    weight = 6  # 60% of users

    @task
    def get_data(self):
        key = random.choice(KEYS)
        with self.client.get(
            f"/data/{key}",
            headers={"X-Priority": "2"},
            catch_response=True,
            name="GET /data/{key} [tier=2]",
        ) as response:
            # 503 is expected behavior under load — count as success for shedder
            if response.status_code in (200, 503):
                response.success()
            else:
                response.failure(f"unexpected status {response.status_code}")


class BatchUser(HttpUser):
    """Tier-3 traffic: high rate, first to be shed."""
    wait_time = between(0.05, 0.2)
    weight = 3  # 30% of users

    @task
    def get_data(self):
        key = random.choice(KEYS)
        with self.client.get(
            f"/data/{key}",
            headers={"X-Priority": "3"},
            catch_response=True,
            name="GET /data/{key} [tier=3]",
        ) as response:
            if response.status_code in (200, 503):
                response.success()
            else:
                response.failure(f"unexpected status {response.status_code}")

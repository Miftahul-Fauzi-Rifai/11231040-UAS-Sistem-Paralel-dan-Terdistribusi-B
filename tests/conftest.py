import time
import pytest
import httpx

BASE_URL = "http://localhost:8080"


@pytest.fixture(scope="session")
def client():
    """HTTP client yang menunggu aggregator siap sebelum test dijalankan."""
    with httpx.Client(base_url=BASE_URL, timeout=15.0) as c:
        for _ in range(30):
            try:
                r = c.get("/health")
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(2)
        yield c

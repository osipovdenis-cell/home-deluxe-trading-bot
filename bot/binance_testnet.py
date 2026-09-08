import hashlib
import hmac
import time
from urllib.parse import urlencode

import httpx


class BinanceTestnetClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.client = httpx.Client(base_url=base_url, timeout=10.0)

    def account(self) -> dict:
        params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
        query = urlencode(params)
        signature = hmac.new(self.api_secret, query.encode(), hashlib.sha256).hexdigest()
        response = self.client.get(
            f"/api/v3/account?{query}&signature={signature}",
            headers={"X-MBX-APIKEY": self.api_key},
        )
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self.client.close()

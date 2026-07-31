"""
HTTP Client Connector

Make HTTP requests to external services.
Emits events for all requests and responses.
"""

import logging
from typing import Dict, Any, Optional

import aiohttp

from .base import BaseConnector, ConnectorConfig

logger = logging.getLogger("omega.connectors.http_client")


class HTTPClientConnector(BaseConnector):
    """
    HTTP client capability connector.

    Actions:
    - get: HTTP GET
    - post: HTTP POST
    - put: HTTP PUT
    - delete: HTTP DELETE
    - patch: HTTP PATCH
    - head: HTTP HEAD
    """

    @property
    def capabilities(self) -> list:
        return ["http.request"]

    async def start(self):
        """Start the HTTP client connector."""
        timeout = self.config.config.get("timeout", 30)
        max_retries = self.config.config.get("max_retries", 3)
        user_agent = self.config.config.get("user_agent", "Omega-Core/0.1.0")

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"User-Agent": user_agent},
        )
        self._max_retries = max_retries

        logger.info(f"HTTP client connector started: timeout={timeout}s")

    async def stop(self):
        """Stop the HTTP client connector."""
        if getattr(self, "_session", None):
            await self._session.close()
        logger.info("HTTP client connector stopped")

    async def execute(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an HTTP request."""
        url = params.get("url")
        if not url:
            return {"error": "missing_url"}

        headers = params.get("headers", {})
        data = params.get("data")
        json_data = params.get("json")

        method = action.upper()
        if method not in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]:
            return {"error": f"Unsupported method: {method}"}

        return await self._request(method, url, headers=headers, data=data, json_data=json_data)

    async def _request(
        self,
        method: str,
        url: str,
        headers: dict = None,
        data: Any = None,
        json_data: Any = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request with retries."""
        last_error = None

        for attempt in range(self._max_retries):
            try:
                async with self._session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    data=data,
                    json=json_data,
                ) as response:
                    body = await response.text()

                    result = {
                        "status": response.status,
                        "url": str(response.url),
                        "method": method,
                        "headers": dict(response.headers),
                        "body": body,
                        "body_length": len(body),
                        "attempt": attempt + 1,
                    }

                    await self.emit("http.response", {
                        "url": url,
                        "method": method,
                        "status": response.status,
                        "attempt": attempt + 1,
                    })

                    return result

            except Exception as e:
                last_error = str(e)
                logger.warning(f"HTTP {method} {url} failed (attempt {attempt + 1}): {e}")

        return {
            "error": "request_failed",
            "url": url,
            "method": method,
            "last_error": last_error,
            "attempts": self._max_retries,
        }

    def health(self) -> bool:
        """Check if session is active."""
        return getattr(self, "_session", None) is not None and not self._session.closed

"""SP-API Brand Analytics client for SQP reports."""

import hashlib
import hmac
import time
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import requests

from ..config import SPAPIConfig
from .auth import SPAPIAuth
from .models import APIResponse, SearchQueryMetrics, SQPReport


# SP-API endpoints
SP_API_BASE_URL = "https://sellingpartnerapi-na.amazon.com"
SQP_ENDPOINT = "/analytics/brandAnalytics/v1/searchQueryPerformance"


class BrandAnalyticsClient:
    """Client for Amazon Brand Analytics API."""

    def __init__(self, config: SPAPIConfig):
        self.config = config
        self.auth = SPAPIAuth(config)
        self._session = requests.Session()

    def _sign_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Sign request using AWS Signature Version 4."""
        # Get current timestamp
        t = datetime.utcnow()
        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = t.strftime("%Y%m%d")

        # AWS region for SP-API NA endpoint
        region = "us-east-1"
        service = "execute-api"

        # Create canonical request
        canonical_uri = url.split(SP_API_BASE_URL)[-1].split("?")[0]
        canonical_querystring = urlencode(sorted(params.items())) if params else ""

        # Headers to sign
        host = "sellingpartnerapi-na.amazon.com"
        signed_headers = "host;x-amz-date"
        canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"

        payload_hash = hashlib.sha256(b"").hexdigest()

        canonical_request = (
            f"{method}\n"
            f"{canonical_uri}\n"
            f"{canonical_querystring}\n"
            f"{canonical_headers}\n"
            f"{signed_headers}\n"
            f"{payload_hash}"
        )

        # Create string to sign
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        string_to_sign = (
            f"{algorithm}\n"
            f"{amz_date}\n"
            f"{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        )

        # Create signing key
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode(), hashlib.sha256).digest()

        k_date = sign(f"AWS4{self.config.aws_secret_key}".encode(), date_stamp)
        k_region = sign(k_date, region)
        k_service = sign(k_region, service)
        k_signing = sign(k_service, "aws4_request")

        # Create signature
        signature = hmac.new(
            k_signing, string_to_sign.encode(), hashlib.sha256
        ).hexdigest()

        # Create authorization header
        authorization = (
            f"{algorithm} "
            f"Credential={self.config.aws_access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )

        headers["x-amz-date"] = amz_date
        headers["Authorization"] = authorization

        return headers

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, str] | None = None,
    ) -> APIResponse:
        """Make authenticated request to SP-API."""
        url = f"{SP_API_BASE_URL}{endpoint}"

        # Get LWA token
        headers = self.auth.get_auth_headers()

        # Sign request
        headers = self._sign_request(method, url, headers, params)

        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=30,
            )

            if response.status_code == 200:
                return APIResponse.from_success(response.json())
            else:
                error_data = response.json() if response.text else {}
                return APIResponse.from_error(
                    code=str(response.status_code),
                    message=error_data.get("message", response.text),
                )

        except requests.RequestException as e:
            return APIResponse.from_error(code="REQUEST_ERROR", message=str(e))

    def get_sqp_report(
        self,
        asin: str,
        start_date: date,
        end_date: date,
    ) -> APIResponse:
        """Fetch Search Query Performance report for an ASIN.

        Args:
            asin: Parent ASIN (will include all child variations)
            start_date: Report start date
            end_date: Report end date

        Returns:
            APIResponse containing SQPReport on success
        """
        params = {
            "marketplaceId": self.config.marketplace_id,
            "reportingPeriod": "WEEK",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "asin": asin,
        }

        response = self._make_request("GET", SQP_ENDPOINT, params)

        if not response.success:
            return response

        # Parse response into SQPReport
        report = self._parse_sqp_response(response.data, asin, start_date, end_date)
        return APIResponse.from_success(report)

    def _parse_sqp_response(
        self,
        data: dict[str, Any],
        asin: str,
        start_date: date,
        end_date: date,
    ) -> SQPReport:
        """Parse SP-API SQP response into SQPReport model."""
        report = SQPReport(
            asin=asin,
            start_date=start_date,
            end_date=end_date,
            marketplace_id=self.config.marketplace_id,
        )

        # Parse search query data
        # The actual response structure may vary - this handles common formats
        queries_data = data.get(
            "searchQueries", data.get("payload", {}).get("searchQueries", [])
        )

        for query_data in queries_data:
            metrics = SearchQueryMetrics(
                search_query=query_data.get("searchQuery", ""),
                search_volume=query_data.get("searchVolume", 0),
                search_score=query_data.get("searchScore", 0.0),
                asin=asin,
                reporting_date=start_date,
            )

            # Parse impressions
            impressions = query_data.get("impressions", {})
            metrics.impressions_total = impressions.get("totalCount", 0)
            metrics.impressions_asin = impressions.get("asinCount", 0)
            metrics.impressions_share = impressions.get("asinShare", 0.0)

            # Parse clicks
            clicks = query_data.get("clicks", {})
            metrics.clicks_total = clicks.get("totalCount", 0)
            metrics.clicks_asin = clicks.get("asinCount", 0)
            metrics.clicks_share = clicks.get("asinShare", 0.0)

            # Parse purchases
            purchases = query_data.get("purchases", {})
            metrics.purchases_total = purchases.get("totalCount", 0)
            metrics.purchases_asin = purchases.get("asinCount", 0)
            metrics.purchases_share = purchases.get("asinShare", 0.0)

            # Parse pricing if available
            pricing = query_data.get("pricing", {})
            metrics.asin_price = pricing.get("asinPrice")
            metrics.market_price = pricing.get("marketPrice")

            report.queries.append(metrics)

        return report

    def get_weekly_reports(
        self,
        asin: str,
        weeks: int = 12,
    ) -> list[APIResponse]:
        """Fetch multiple weeks of SQP reports.

        Args:
            asin: Parent ASIN
            weeks: Number of weeks to fetch (default 12)

        Returns:
            List of APIResponses, one per week
        """
        reports = []
        today = date.today()

        for week_offset in range(weeks):
            # Calculate week boundaries (Monday to Sunday)
            end_date = today - timedelta(days=today.weekday() + 1 + (week_offset * 7))
            start_date = end_date - timedelta(days=6)

            # Rate limiting - 1 request per second
            if week_offset > 0:
                time.sleep(1)

            report = self.get_sqp_report(asin, start_date, end_date)
            reports.append(report)

        return reports

    def test_connection(self) -> dict[str, Any]:
        """Test SP-API connection.

        Returns dict with success status and details.
        """
        # First test LWA auth
        auth_result = self.auth.test_connection()
        if not auth_result["success"]:
            return auth_result

        # Then try a minimal API call
        today = date.today()
        end_date = today - timedelta(days=today.weekday() + 1)
        start_date = end_date - timedelta(days=6)

        params = {
            "marketplaceId": self.config.marketplace_id,
            "reportingPeriod": "WEEK",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }

        response = self._make_request("GET", SQP_ENDPOINT, params)

        if response.success or response.error_code == "400":
            # 400 is expected without ASIN - means API is reachable
            return {
                "success": True,
                "message": "Successfully connected to SP-API Brand Analytics",
                "marketplace": self.config.marketplace_id,
            }

        return {
            "success": False,
            "message": f"API connection failed: {response.error_message}",
            "error_code": response.error_code,
        }

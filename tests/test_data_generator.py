"""Tests for DataGeneratorService._get() retry logic."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from services.data_generator import DataGeneratorService


@pytest.fixture
def svc():
    return DataGeneratorService(api_url="https://randomuser.me/api/")


class TestGetRetry:
    def test_succeeds_on_first_attempt(self, svc):
        """No retry when first request succeeds."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"results": []}

        with patch("services.data_generator.requests.get", return_value=mock_resp) as mock_get:
            result = svc._get("https://example.com")

        assert result == {"results": []}
        mock_get.assert_called_once()

    def test_retries_on_request_exception_then_succeeds(self, svc):
        """Retries on RequestException and succeeds on 2nd attempt."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"results": ["ok"]}

        with (
            patch(
                "services.data_generator.requests.get",
                side_effect=[requests.exceptions.ConnectionError("refused"), mock_resp],
            ) as mock_get,
            patch("services.data_generator.time.sleep") as mock_sleep,
        ):
            result = svc._get("https://example.com")

        assert result == {"results": ["ok"]}
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(DataGeneratorService._RETRY_DELAY)

    def test_raises_after_max_retries_exceeded(self, svc):
        """Raises RuntimeError after all retry attempts are exhausted."""
        with (
            patch(
                "services.data_generator.requests.get",
                side_effect=requests.exceptions.Timeout("timed out"),
            ),
            patch("services.data_generator.time.sleep"),
        ):
            with pytest.raises(
                RuntimeError, match="Failed to reach randomuser API after 3 attempts"
            ):
                svc._get("https://example.com")

    def test_non_200_raises_immediately_without_retry(self, svc):
        """Non-200 HTTP response raises RuntimeError without retrying."""
        mock_resp = MagicMock()
        mock_resp.status_code = 503

        with (
            patch("services.data_generator.requests.get", return_value=mock_resp) as mock_get,
            patch("services.data_generator.time.sleep") as mock_sleep,
        ):
            with pytest.raises(RuntimeError, match="HTTP 503"):
                svc._get("https://example.com")

        mock_get.assert_called_once()
        mock_sleep.assert_not_called()

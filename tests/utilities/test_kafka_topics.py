from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from utilities.kafka_topics import create_kafka_topic, delete_kafka_topic


class TestCreateKafkaTopic:
    """Tests for create_kafka_topic."""

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Verify create_kafka_topic succeeds when Kafka admin client succeeds."""
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin = MagicMock()
        mock_admin.create_topics.return_value = {"org.proj.coll": mock_future}

        with patch("utilities.kafka_topics.get_kafka_admin_client", return_value=mock_admin):
            await create_kafka_topic("org", "proj", "coll")

        mock_admin.create_topics.assert_called_once()

    @pytest.mark.asyncio
    async def test_failure_raises_http_500(self):
        """Verify create_kafka_topic raises HTTP 500 when Kafka admin client fails."""
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Kafka error")
        mock_admin = MagicMock()
        mock_admin.create_topics.return_value = {"org.proj.coll": mock_future}

        with (
            patch("utilities.kafka_topics.get_kafka_admin_client", return_value=mock_admin),
            pytest.raises(HTTPException) as exc_info,
        ):
            await create_kafka_topic("org", "proj", "coll")

        assert exc_info.value.status_code == 500
        assert "org.proj.coll" in exc_info.value.detail


class TestDeleteKafkaTopic:
    """Tests for delete_kafka_topic."""

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Verify delete_kafka_topic succeeds when Kafka admin client succeeds."""
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin = MagicMock()
        mock_admin.delete_topics.return_value = {"org.proj.coll": mock_future}

        with patch("utilities.kafka_topics.get_kafka_admin_client", return_value=mock_admin):
            await delete_kafka_topic("org", "proj", "coll")

        mock_admin.delete_topics.assert_called_once()

    @pytest.mark.asyncio
    async def test_inner_failure_raises_http_500(self):
        """Verify delete_kafka_topic raises HTTP 500 when topic deletion future fails."""
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Kafka error")
        mock_admin = MagicMock()
        mock_admin.delete_topics.return_value = {"org.proj.coll": mock_future}

        with (
            patch("utilities.kafka_topics.get_kafka_admin_client", return_value=mock_admin),
            pytest.raises(HTTPException) as exc_info,
        ):
            await delete_kafka_topic("org", "proj", "coll")

        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_outer_failure_raises_http_500(self):
        """Verify delete_kafka_topic raises HTTP 500 when admin client call fails."""
        mock_admin = MagicMock()
        mock_admin.delete_topics.side_effect = Exception("Connection error")

        with (
            patch("utilities.kafka_topics.get_kafka_admin_client", return_value=mock_admin),
            pytest.raises(HTTPException) as exc_info,
        ):
            await delete_kafka_topic("org", "proj", "coll")

        assert exc_info.value.status_code == 500

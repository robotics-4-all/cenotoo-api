from unittest.mock import MagicMock, patch


class TestHealthEndpoint:
    """Tests for the health and ready endpoints."""

    def test_health_returns_200(self, client):
        """Verify health endpoint returns 200 OK."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_ready_all_ok(self, client, mock_cassandra_session):
        """Verify ready endpoint returns 200 when all services are up."""
        mock_cassandra_session.execute.return_value = [MagicMock(release_version="4.0")]
        with patch("confluent_kafka.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value.list_topics.return_value = MagicMock()
            response = client.get("/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["checks"]["cassandra"] == "ok"
        assert data["checks"]["kafka"] == "ok"

    def test_ready_cassandra_down(self, client, mock_cassandra_session):
        """Verify ready endpoint returns 503 when Cassandra is down."""
        mock_cassandra_session.execute.side_effect = Exception("connection refused")
        with patch("confluent_kafka.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value.list_topics.return_value = MagicMock()
            response = client.get("/ready")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unavailable"
        assert "error" in data["checks"]["cassandra"]

    def test_ready_kafka_down(self, client, mock_cassandra_session):
        """Verify ready returns 200 with degraded kafka when Kafka is down."""
        mock_cassandra_session.execute.return_value = [MagicMock(release_version="4.0")]
        with patch("confluent_kafka.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value.list_topics.side_effect = Exception("broker unreachable")
            response = client.get("/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["checks"]["kafka"].startswith("degraded")

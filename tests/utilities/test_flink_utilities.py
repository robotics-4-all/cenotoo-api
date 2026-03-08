import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from utilities.flink_utilities import deploy_flink_script, generate_flink_script


class TestGenerateFlinkScript:
    """Tests for generate_flink_script."""

    def test_tumbling_window(self):
        """Verify generate_flink_script creates correct script for tumbling window."""
        script = generate_flink_script(
            project_name="proj",
            topic_name="org.proj.coll",
            attribute="temperature",
            every_n=5,
            units="minute",
            metric="avg",
            interval_type="tumbling",
        )

        assert "TUMBLE" in script
        assert "temperature" in script
        assert "org.proj.coll" in script
        assert "KafkaSource" in script
        assert "KafkaSink" in script
        assert "avg" in script.lower() or "AVG" in script

    def test_sliding_window(self):
        """Verify generate_flink_script creates correct script for sliding window."""
        script = generate_flink_script(
            project_name="proj",
            topic_name="org.proj.coll",
            attribute="humidity",
            every_n=10,
            units="second",
            metric="sum",
            interval_type="sliding",
            sliding_factor=5,
        )

        assert "HOP" in script
        assert "humidity" in script
        assert "SECOND" in script

    def test_unsupported_interval_raises(self):
        """Verify generate_flink_script raises ValueError for unsupported interval type."""
        with pytest.raises(ValueError, match="Unsupported interval type"):
            generate_flink_script(
                project_name="proj",
                topic_name="org.proj.coll",
                attribute="temp",
                every_n=5,
                units="minute",
                metric="avg",
                interval_type="session",
            )

    def test_sink_topic_format(self):
        """Verify generate_flink_script formats sink topic name correctly."""
        script = generate_flink_script(
            project_name="proj",
            topic_name="org.proj.coll",
            attribute="temp",
            every_n=5,
            units="minute",
            metric="avg",
            interval_type="tumbling",
        )

        assert "org.proj.coll.5minute.avg.temp" in script

    def test_script_contains_pyflink_imports(self):
        """Verify generate_flink_script includes necessary PyFlink imports."""
        script = generate_flink_script(
            project_name="proj",
            topic_name="org.proj.coll",
            attribute="temp",
            every_n=1,
            units="hour",
            metric="max",
            interval_type="tumbling",
        )

        assert "from pyflink.table import TableEnvironment" in script
        assert "in_streaming_mode" in script


class TestDeployFlinkScript:
    """Tests for deploy_flink_script."""

    def test_happy_path(self):
        """Verify deploy_flink_script successfully deploys script to Flink container."""
        mock_container = MagicMock()
        mock_container.put_archive.return_value = True
        mock_container.exec_run.return_value = MagicMock(exit_code=0, output=b"Job submitted")

        mock_client = MagicMock()
        mock_client.containers.get.return_value = mock_container

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("print('hello')")
            script_path = f.name

        try:
            with patch("utilities.flink_utilities.docker") as mock_docker:
                mock_docker.from_env.return_value = mock_client

                deploy_flink_script(script_path)

                mock_client.containers.get.assert_called_once_with("test-flink-jobmanager19-1")
                mock_container.put_archive.assert_called_once()
                mock_container.exec_run.assert_called_once()
                assert os.path.basename(script_path) in mock_container.exec_run.call_args[0][0]
        finally:
            os.unlink(script_path)

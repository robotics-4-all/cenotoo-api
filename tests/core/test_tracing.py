"""Tests for core.tracing — setup_tracing with and without OTLP endpoint."""

import importlib
import sys
from unittest.mock import MagicMock, patch


class TestSetupTracingNoEndpoint:
    """Tests for setup_tracing with no endpoint."""

    def test_returns_early_when_endpoint_empty(self):
        """Verify that tracing setup returns early when the OTLP endpoint is empty."""
        with patch("core.tracing.settings") as mock_settings:
            mock_settings.otlp_endpoint = ""
            from core.tracing import setup_tracing

            app = MagicMock()
            setup_tracing(app)
            app.assert_not_called()


class TestSetupTracingWithEndpoint:
    """Tests for setup_tracing with endpoint."""

    def test_configures_tracing_when_endpoint_set(self):
        """Verify that tracing is configured correctly when the OTLP endpoint is set."""
        mock_trace = MagicMock()
        mock_resource = MagicMock()
        mock_tracer_provider = MagicMock()
        mock_batch_processor = MagicMock()
        mock_fastapi_instrumentor = MagicMock()
        mock_otlp_exporter = MagicMock()

        mock_resource_cls = MagicMock()
        mock_resource_cls.create.return_value = mock_resource

        mock_tracer_provider_cls = MagicMock(return_value=mock_tracer_provider)
        mock_batch_processor_cls = MagicMock(return_value=mock_batch_processor)
        mock_otlp_exporter_cls = MagicMock(return_value=mock_otlp_exporter)

        otel_modules = {
            "opentelemetry": MagicMock(trace=mock_trace),
            "opentelemetry.trace": mock_trace,
            "opentelemetry.instrumentation": MagicMock(),
            "opentelemetry.instrumentation.fastapi": MagicMock(
                FastAPIInstrumentor=mock_fastapi_instrumentor
            ),
            "opentelemetry.sdk": MagicMock(),
            "opentelemetry.sdk.resources": MagicMock(Resource=mock_resource_cls),
            "opentelemetry.sdk.trace": MagicMock(TracerProvider=mock_tracer_provider_cls),
            "opentelemetry.sdk.trace.export": MagicMock(
                BatchSpanProcessor=mock_batch_processor_cls
            ),
            "opentelemetry.exporter": MagicMock(),
            "opentelemetry.exporter.otlp": MagicMock(),
            "opentelemetry.exporter.otlp.proto": MagicMock(),
            "opentelemetry.exporter.otlp.proto.grpc": MagicMock(),
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": MagicMock(
                OTLPSpanExporter=mock_otlp_exporter_cls
            ),
        }

        saved_modules = {}
        for mod_name in otel_modules:
            if mod_name in sys.modules:
                saved_modules[mod_name] = sys.modules[mod_name]

        try:
            sys.modules.update(otel_modules)

            if "core.tracing" in sys.modules:
                del sys.modules["core.tracing"]

            with patch("config.settings") as mock_settings:
                mock_settings.otlp_endpoint = "http://localhost:4317"
                mock_settings.otlp_service_name = "test-service"
                mock_settings.environment = "testing"

                from core.tracing import setup_tracing

                app = MagicMock()
                setup_tracing(app)

            mock_resource_cls.create.assert_called_once()
            create_kwargs = mock_resource_cls.create.call_args
            attrs = create_kwargs[1]["attributes"]
            assert attrs["service.name"] == "test-service"
            assert attrs["deployment.environment"] == "testing"

            mock_tracer_provider_cls.assert_called_once_with(resource=mock_resource)
            mock_otlp_exporter_cls.assert_called_once_with(
                endpoint="http://localhost:4317", insecure=True
            )
            mock_tracer_provider.add_span_processor.assert_called_once_with(mock_batch_processor)
            mock_trace.set_tracer_provider.assert_called_once_with(mock_tracer_provider)
            mock_fastapi_instrumentor.instrument_app.assert_called_once_with(app)
        finally:
            for mod_name in otel_modules:
                if mod_name in saved_modules:
                    sys.modules[mod_name] = saved_modules[mod_name]
                else:
                    sys.modules.pop(mod_name, None)

            if "core.tracing" in sys.modules:
                del sys.modules["core.tracing"]
            importlib.import_module("core.tracing")

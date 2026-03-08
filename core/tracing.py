from config import settings


def setup_tracing(app):
    """Configure OpenTelemetry tracing for the FastAPI application."""
    if not settings.otlp_endpoint:
        return

    from opentelemetry import trace
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(
        attributes={
            "service.name": settings.otlp_service_name,
            "service.version": "1.0.1",
            "deployment.environment": settings.environment,
        }
    )

    provider = TracerProvider(resource=resource)

    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )

    exporter = OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)

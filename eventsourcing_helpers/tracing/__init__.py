from confluent_kafka_helpers.tracing import OpenTelemetryBackend
from confluent_kafka_helpers.tracing import attributes as attrs
from confluent_kafka_helpers.tracing.datadog import get_datadog_service_name

tracer = OpenTelemetryBackend("eventsourcing_helpers")

__all__ = ["tracer", "attrs", "get_datadog_service_name"]

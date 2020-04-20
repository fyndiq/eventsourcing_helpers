from confluent_kafka import avro


def get_key_schema(meta):
    if not meta:
        return
    schema = getattr(meta, 'key_schema_file', None)
    return avro.load(schema) if schema else None


def get_value_schema(meta):
    if not meta:
        return
    schema = getattr(meta, 'value_schema_file', None)
    return avro.load(schema) if schema else None

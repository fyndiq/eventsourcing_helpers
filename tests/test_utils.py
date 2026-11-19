import pytest

from eventsourcing_helpers.utils import (
    get_all_nested_keys,
    get_callable_representation,
    redact_config,
)


class Test:
    def method(self):
        pass

    @classmethod
    def klass(cls):
        pass

    @staticmethod
    def static():
        pass


def myfunc():
    pass


@pytest.mark.parametrize(
    "result, expected_result",
    [
        (get_callable_representation(Test), "Test"),
        (get_callable_representation(Test()), ""),
        (get_callable_representation(Test().method), "Test.method"),
        (get_callable_representation(Test.method), "Test.method"),
        (get_callable_representation(Test().klass), "Test.klass"),
        (get_callable_representation(Test.klass), "Test.klass"),
        (get_callable_representation(Test().static), "Test.static"),
        (get_callable_representation(Test.static), "Test.static"),
        (get_callable_representation(myfunc), "myfunc"),
        (get_callable_representation(None), ""),
        (get_callable_representation(1), ""),
    ],
)
def test_get_callable_representation(result, expected_result):
    assert expected_result == result


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "_version": 0,
            },
            [
                "_version",
            ],
        ),
        (
            {"_version": 0, "id": None, "py/object": "tests.test_models.NestedAggregate"},
            [
                "_version",
                "id",
                "py/object",
                "tests.test_models.NestedAggregate",
            ],
        ),
        (
            {
                "_version": 0,
                "id": None,
                "nested_entity": {
                    "__dict__": {"nested_entity_id": "a"},
                    "py/object": "eventsourcing_helpers.models.EntityDict",
                },
                "py/object": "tests.test_models.NestedAggregate",
            },
            [
                "_version",
                "id",
                "nested_entity",
                "py/object",
                "tests.test_models.NestedAggregate",
                "py/object",
                "__dict__",
                "eventsourcing_helpers.models.EntityDict",
                "nested_entity_id",
            ],
        ),
    ],
)
def test_get_all_nested_keys(data, expected_result):
    result = get_all_nested_keys(data, [])
    for expected_key in expected_result:
        assert expected_key in result


def test_redact_config_masks_credentials():
    config = {
        "bootstrap.servers": "kafka.example.com:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "avnadmin",
        "sasl.password": "s3cr3t-pw",
        "ssl.key.password": "s3cr3t-key-pw",
        "schema.registry.url": "https://avnadmin:s3cr3t-pw@registry.example.com:443",
        "consumer": {"api_token": "s3cr3t-token", "client.secret": "s3cr3t-secret"},
        "urls": ["https://user:s3cr3t-pw@a.example.com", "https://b.example.com"],
        "empty.password": "",
        "timeout": 10,
    }

    redacted = redact_config(config)

    assert "s3cr3t" not in repr(redacted)
    assert redacted == {
        "bootstrap.servers": "kafka.example.com:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "avnadmin",
        "sasl.password": "***",
        "ssl.key.password": "***",
        "schema.registry.url": "https://avnadmin:***@registry.example.com:443",
        "consumer": {"api_token": "***", "client.secret": "***"},
        "urls": ["https://user:***@a.example.com", "https://b.example.com"],
        "empty.password": "",
        "timeout": 10,
    }


def test_redact_config_does_not_modify_original():
    config = {"sasl.password": "s3cr3t-pw", "nested": {"url": "https://u:s3cr3t-pw@host"}}

    redact_config(config)

    assert config == {"sasl.password": "s3cr3t-pw", "nested": {"url": "https://u:s3cr3t-pw@host"}}


@pytest.mark.parametrize(
    "value",
    [
        "https://registry.example.com:443",
        "https://user@registry.example.com",
        "kafka-1:9092,kafka-2:9092",
        "/etc/ssl/ca.pem",
    ],
)
def test_redact_config_keeps_values_without_credentials(value):
    assert redact_config({"some.setting": value}) == {"some.setting": value}

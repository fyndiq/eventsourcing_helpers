import pytest

from eventsourcing_helpers.utils import get_all_nested_keys, get_callable_representation


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


@pytest.mark.parametrize('result, expected_result', [
    (get_callable_representation(Test), 'Test'),
    (get_callable_representation(Test()), ''),
    (get_callable_representation(Test().method), 'Test.method'),
    (get_callable_representation(Test.method), 'Test.method'),
    (get_callable_representation(Test().klass), 'Test.klass'),
    (get_callable_representation(Test.klass), 'Test.klass'),
    (get_callable_representation(Test().static), 'Test.static'),
    (get_callable_representation(Test.static), 'Test.static'),
    (get_callable_representation(myfunc), 'myfunc'),
    (get_callable_representation(None), ''),
    (get_callable_representation(1), ''),
])
def test_get_callable_representation(result, expected_result):
    assert expected_result == result


@pytest.mark.parametrize('data, expected_result', [
    (
        {
            '_version': 0,
        },
        [
            '_version',
        ]
    ),
    (
        {
            '_version': 0,
            'id': None,
            'py/object': 'tests.test_models.NestedAggregate'
        },
        [
            '_version',
            'id',
            'py/object',
            'tests.test_models.NestedAggregate',
        ]
    ),
    (
        {
            '_version': 0,
            'id': None,
            'nested_entity': {
                '__dict__': {
                    'nested_entity_id': 'a'
                },
                'py/object': 'eventsourcing_helpers.models.EntityDict'
            },
            'py/object': 'tests.test_models.NestedAggregate'
        },
        [
            '_version',
            'id',
            'nested_entity',
            'py/object',
            'tests.test_models.NestedAggregate',
            'py/object',
            '__dict__',
            'eventsourcing_helpers.models.EntityDict',
            'nested_entity_id'
        ]
    ),
])
def test_get_all_nested_keys(data, expected_result):
    result = get_all_nested_keys(data, [])
    for expected_key in expected_result:
        assert expected_key in result
